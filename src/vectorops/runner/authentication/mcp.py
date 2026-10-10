"""MCP credential bindings, grant selection, and local credential lifecycle."""

from collections.abc import Callable
import hashlib
import json
import time
from typing import Optional

from . import contracts, models, oauth
from . import mcp_client_credentials, mcp_oauth, mcp_settings
from .store import CredentialStore


class ProfileTokenSource:
    def __init__(
        self, resolver: contracts.CredentialResolver, profile: models.Profile
    ) -> None:
        self.resolver = resolver
        self.profile = profile.model_copy(deep=True)

    async def acquire(
        self,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> models.Credential:
        return await self.resolver.acquire(
            self.profile, execution_id, rejected=rejected, interactive=interactive
        )


class ClientTokenSource:
    def __init__(
        self, owner: "MCPCredentials", settings: mcp_settings.ClientCredentials
    ) -> None:
        self.owner = owner
        self.settings = settings.model_copy(deep=True)

    async def acquire(
        self,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> models.Credential:
        return await self.owner.client_token(self.settings, rejected=rejected)


class MCPCredentials:
    def __init__(
        self,
        store: CredentialStore,
        resolver: contracts.CredentialResolver,
        register: Callable[[str, oauth.OAuthAdapter], None],
    ) -> None:
        self.store, self.resolver, self.register = store, resolver, register

    @staticmethod
    def binding(settings: mcp_settings.Settings) -> models.Profile:
        identity = json.dumps(
            settings.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        return models.Profile(
            provider="mcp-binding-"
            + hashlib.sha256(identity.encode()).hexdigest()[:48],
            profile=settings.profile,
        )

    async def adapter(self, settings: mcp_settings.Settings) -> mcp_oauth.MCPAdapter:
        adapter = await mcp_oauth.MCPAdapter.discover(settings)
        await self.store.remember(self.binding(settings), adapter.profile)
        self.register(adapter.profile.provider, adapter)
        return adapter

    async def token_source(
        self, settings: mcp_settings.Settings | mcp_settings.ClientCredentials
    ) -> contracts.TokenSource:
        if isinstance(settings, mcp_settings.ClientCredentials):
            return ClientTokenSource(self, settings)
        adapter = await self.adapter(settings)
        return ProfileTokenSource(self.resolver, adapter.profile)

    async def secret(self, profile: str) -> str:
        reference = models.Profile(provider="mcp", profile=profile)
        async with self.store.locked(reference):
            value = await self.store.load(reference)
        if value is None or value.kind != "api_key":
            raise models.AuthenticationError("MCP API key is not configured")
        if value.expires_at is not None and value.expires_at <= time.time():
            raise models.AuthenticationError("MCP API key has expired")
        return value.secret

    async def client_token(
        self,
        settings: mcp_settings.ClientCredentials,
        *,
        rejected: Optional[str] = None,
    ) -> models.Credential:
        reference = mcp_client_credentials.profile(settings)
        async with self.store.locked(reference):
            value = await self.store.load(reference)
            if value is not None and value.reusable(time.time(), rejected):
                return value
            secret = await self.secret(settings.client_secret)
            return await self.store.publish(
                reference, lambda: mcp_client_credentials.request(settings, secret)
            )

    async def forget(
        self,
        settings: (
            mcp_settings.NoAuth
            | mcp_settings.HeaderAuth
            | mcp_settings.Settings
            | mcp_settings.ClientCredentials
        ),
    ) -> None:
        """Forget bound local tokens; discover only to migrate pre-binding OAuth stores."""
        if isinstance(settings, mcp_settings.NoAuth):
            return
        if isinstance(settings, mcp_settings.HeaderAuth):
            await self.store.delete(
                models.Profile(provider="mcp", profile=settings.credential)
            )
            return
        if isinstance(settings, mcp_settings.ClientCredentials):
            await self.store.delete(mcp_client_credentials.profile(settings))
            return
        binding = self.binding(settings)
        async with self.store.locked(binding):
            references = await self.store.bindings(binding)
        if not references:
            await self.adapter(settings)
        async with self.store.locked(binding):
            for reference in await self.store.bindings(binding):
                await self.store.delete(reference)
