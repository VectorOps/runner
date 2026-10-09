"""Credential resolution and shared, host-assisted OAuth attempts owned by a runner."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
import secrets
import time
from typing import Optional
from uuid import uuid4

import connect
from connect.credentials import helpers
import pydantic

from vectorops.runner import global_config
from vectorops.runner.authentication import models as m, oauth
from vectorops.runner.authentication import mcp_oauth
from vectorops.runner.authentication.store import CredentialStore

EventSink = Callable[[dict[str, pydantic.JsonValue]], None]


@dataclass
class Pending:
    requirement: m.Requirement
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    executions: dict[str, int] = field(default_factory=dict)
    waiters: int = 0
    cancelled: bool = False
    owner: Optional[str] = None
    claim: Optional[m.Claim] = None
    verifier: str = field(default="", repr=False)
    challenge: str = ""
    completing: bool = False


class AuthenticationService:
    def __init__(
        self,
        configuration: Optional[global_config.GlobalConfiguration] = None,
        *,
        event_sink: Optional[EventSink] = None,
        adapters: Optional[dict[str, oauth.OAuthAdapter]] = None,
    ) -> None:
        self.configuration = configuration or global_config.GlobalConfiguration()
        self.store = CredentialStore(self.configuration)
        self.adapters: dict[str, oauth.OAuthAdapter] = (
            {"chatgpt": oauth.ChatGPT()} if adapters is None else adapters
        )
        self.event_sink = event_sink
        self._pending: dict[str, Pending] = {}
        self._users: dict[str, int] = {}

    @property
    def waiting(self) -> bool:
        return bool(self._pending)

    def waiting_for(self, execution_id: str) -> bool:
        return any(execution_id in item.executions for item in self._pending.values())

    def _emit(self, kind: str, item: Pending) -> None:
        if self.event_sink is not None:
            try:
                self.event_sink(
                    {
                        "kind": kind,
                        "id": item.requirement.id,
                        "provider": item.requirement.provider,
                        "profile": item.requirement.profile,
                    }
                )
            except Exception:
                pass

    def _expire(self, item: Pending) -> None:
        if (
            item.claim is not None
            and item.claim.expires_at <= time.time()
            and not item.completing
        ):
            self._release(item)

    def _release(self, item: Pending) -> None:
        item.owner, item.claim, item.verifier, item.challenge = None, None, "", ""
        self._emit("auth.required", item)

    def pending(self) -> list[m.Requirement]:
        result = []
        for item in self._pending.values():
            self._expire(item)
            result.append(
                item.requirement.model_copy(
                    update={
                        "status": "claimed" if item.claim else "waiting",
                        "execution_ids": sorted(item.executions),
                    }
                )
            )
        return result

    def _find(self, identifier: str) -> Pending:
        for item in self._pending.values():
            if item.requirement.id == identifier:
                self._expire(item)
                return item
        raise m.AuthenticationConflict("authentication request is no longer pending")

    def claim(self, identifier: str, owner: str) -> m.Claim:
        item = self._find(identifier)
        if item.claim is not None:
            if item.owner != owner:
                raise m.AuthenticationConflict(
                    "authentication is claimed by another host"
                )
            return item.claim
        item.verifier, item.challenge = helpers.generate_pkce_pair()
        item.owner = owner
        item.claim = m.Claim(
            id=identifier,
            attempt=uuid4().hex,
            state=helpers.create_oauth_state(),
            redirect_uri=self.adapters[item.requirement.provider].redirect_uri,
            expires_at=time.time() + 600,
        )
        return item.claim

    def _attempt(self, reference: m.Attempt, owner: str) -> Pending:
        item = self._find(reference.id)
        if (
            item.claim is None
            or item.claim.attempt != reference.attempt
            or item.owner != owner
        ):
            raise m.AuthenticationConflict(
                "authentication attempt is stale or belongs to another host"
            )
        return item

    def ready(self, reference: m.Attempt, owner: str) -> str:
        item = self._attempt(reference, owner)
        assert item.claim is not None
        return self.adapters[item.requirement.provider].authorization_url(
            item.challenge, item.claim.state
        )

    def disconnect(self, owner: str) -> None:
        for item in self._pending.values():
            if item.owner == owner and not item.completing:
                self._release(item)

    def release(self, reference: m.Attempt, owner: str) -> None:
        item = self._attempt(reference, owner)
        if item.completing:
            raise m.AuthenticationConflict("authentication exchange is in progress")
        self._release(item)

    def cancel(self, identifier: str) -> None:
        item = self._find(identifier)
        if item.completing:
            raise m.AuthenticationConflict("authentication exchange is in progress")
        item.cancelled = True
        item.changed.set()
        self._emit("auth.cancelled", item)

    async def complete(self, callback: m.Callback, owner: str) -> None:
        item = self._attempt(callback, owner)
        assert item.claim is not None
        if not secrets.compare_digest(callback.state, item.claim.state):
            raise m.AuthenticationConflict("OAuth state mismatch")
        if callback.error:
            self.cancel(callback.id)
            return
        if not callback.code:
            raise m.AuthenticationConflict("OAuth callback is missing its code")
        if item.completing:
            raise m.AuthenticationConflict(
                "authentication exchange is already in progress"
            )
        item.completing = True

        async def exchange() -> None:
            async with self.store.locked(item.requirement):
                value = await self.adapters[item.requirement.provider].exchange(
                    item.requirement, callback.code or "", item.verifier
                )
                await self.store.save(value)

        task = asyncio.create_task(exchange())
        try:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        finally:
            item.completing = False
            self._release(item)
            item.changed.set()
        self._emit("auth.completed", item)

    async def provision(self, value: m.Credential) -> None:
        if value.kind == "oauth2" and (
            value.provider not in self.adapters or not value.refresh_token
        ):
            raise m.AuthenticationError(
                "OAuth provisioning requires a supported provider and refresh token"
            )
        await self.store.provision(value)
        item = self._pending.get(value.key)
        if item is not None:
            item.changed.set()

    async def _wait(self, profile: m.Profile, execution_id: Optional[str]) -> None:
        item = self._pending.get(profile.key)
        if item is None:
            item = Pending(
                m.Requirement(
                    provider=profile.provider, profile=profile.profile, id=uuid4().hex
                )
            )
            self._pending[profile.key] = item
            self._emit("auth.required", item)
        item.waiters += 1
        if execution_id is not None:
            item.executions[execution_id] = item.executions.get(execution_id, 0) + 1
        try:
            if item.cancelled:
                raise m.AuthenticationCancelled("provider authentication was cancelled")
            try:
                async with asyncio.timeout(1):
                    await item.changed.wait()
            except TimeoutError:
                pass
            if item.cancelled:
                raise m.AuthenticationCancelled("provider authentication was cancelled")
            item.changed.clear()
        finally:
            item.waiters -= 1
            if execution_id is not None:
                item.executions[execution_id] -= 1
                if not item.executions[execution_id]:
                    del item.executions[execution_id]

    async def acquire(
        self,
        profile: m.Profile,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> m.Credential:
        """Serialize refresh across processes; wait outside the lock for user consent."""
        invalid: Optional[str] = None
        self._users[profile.key] = self._users.get(profile.key, 0) + 1
        try:
            while True:
                async with self.store.locked(profile):
                    value = await self.store.load(profile)
                    if value is not None and value.secret != invalid:
                        if value.kind == "api_key":
                            if rejected == value.secret:
                                raise m.AuthenticationError("provider rejected API key")
                            return value
                        adapter = self.adapters.get(profile.provider)
                        if adapter is None:
                            raise m.AuthenticationError("unsupported OAuth provider")
                        if (
                            value.expires_at is None
                            or value.expires_at <= time.time() + 60
                            or value.secret == rejected
                        ):

                            async def refresh() -> m.Credential:
                                assert value is not None
                                updated = await adapter.refresh(value)
                                await self.store.save(updated)
                                return updated

                            task = asyncio.create_task(refresh())
                            try:
                                try:
                                    return await asyncio.shield(task)
                                except asyncio.CancelledError:
                                    await task
                                    raise
                            except m.LoginRequired:
                                invalid = value.secret
                        else:
                            return value
                    elif value is None and profile.provider not in self.adapters:
                        raise m.AuthenticationError(
                            "provider API key is not configured"
                        )
                if not interactive:
                    raise m.LoginRequired(
                        "OAuth authorization required before retrying MCP"
                    )
                await self._wait(profile, execution_id)
        finally:
            self._users[profile.key] -= 1
            if not self._users[profile.key]:
                del self._users[profile.key]
                self._pending.pop(profile.key, None)

    async def mcp_secret(self, profile: str) -> str:
        """Resolve a provisioned MCP API key without interactive provider login."""
        reference = m.Profile(provider="mcp", profile=profile)
        async with self.store.locked(reference):
            value = await self.store.load(reference)
        if value is None or value.kind != "api_key":
            raise m.AuthenticationError("MCP API key is not configured")
        if value.expires_at is not None and value.expires_at <= time.time():
            raise m.AuthenticationError("MCP API key has expired")
        return value.secret

    async def mcp_adapter(self, settings: mcp_oauth.Settings) -> mcp_oauth.MCPAdapter:
        adapter = await mcp_oauth.MCPAdapter.discover(settings)
        self.adapters[adapter.profile.provider] = adapter
        return adapter

    async def transport(
        self,
        provider: Optional[str],
        model: str,
        profile: str = "default",
        execution_id: Optional[str] = None,
    ) -> "ProviderAuth":
        name = provider or model.partition("/")[0]
        if not provider and "/" not in model:
            raise m.AuthenticationError(
                "provider must be explicit or included in the model name"
            )
        selected = m.Profile(provider=name, profile=profile)
        value = await self.acquire(selected, execution_id)
        return ProviderAuth(self, selected, execution_id, value)


class ProviderAuth:
    def __init__(
        self,
        service: AuthenticationService,
        profile: m.Profile,
        execution_id: Optional[str],
        value: m.Credential,
    ) -> None:
        self.service, self.profile, self.execution_id, self.value = (
            service,
            profile,
            execution_id,
            value,
        )

    async def resolve(
        self, context: Optional[connect.AuthContext] = None
    ) -> connect.ResolvedAuth:
        value = self.value
        if value.kind == "oauth2":
            strategy: connect.TransportAuth = connect.ChatGPTAccessTokenAuth(
                value.secret, account_id=value.account_id
            )
        elif value.provider == "anthropic":
            strategy = connect.HeaderAPIKeyAuth(
                value.secret, header_name="x-api-key", prefix=""
            )
        elif value.provider == "gemini":
            strategy = connect.HeaderAPIKeyAuth(
                value.secret, header_name="x-goog-api-key", prefix=""
            )
        else:
            strategy = connect.BearerTokenAuth(value.secret)
        return await strategy.resolve(context)

    async def refresh(self, context: Optional[connect.AuthContext] = None) -> bool:
        if self.value.kind == "api_key":
            return False
        self.value = await self.service.acquire(
            self.profile, self.execution_id, rejected=self.value.secret
        )
        return True
