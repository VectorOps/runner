"""Pinned-issuer MCP OAuth for pre-registered public clients, without redirects."""

import hashlib
import json
from typing import Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import aiohttp
import pydantic

from . import models as m, mcp_tokens
from .mcp_settings import Settings as Settings, endpoint as endpoint
from .mcp_tokens import payload as payload


async def document(
    session: aiohttp.ClientSession, url: str
) -> dict[str, pydantic.JsonValue]:
    async with session.get(url, allow_redirects=False) as response:
        if response.status != 200:
            raise m.AuthenticationError("MCP OAuth metadata request was rejected")
        return await payload(response)


class MCPAdapter:
    def __init__(
        self, settings: Settings, authorization_endpoint: str, token_endpoint: str
    ) -> None:
        self.settings = settings.model_copy(deep=True)
        self.authorization_endpoint = authorization_endpoint
        self.token_endpoint = token_endpoint
        self.redirect_uri = settings.redirect_uri
        identity = json.dumps(
            {
                "settings": settings.model_dump(mode="json"),
                "authorization_endpoint": authorization_endpoint,
                "token_endpoint": token_endpoint,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self.profile = m.Profile(
            provider="mcp-oauth-" + hashlib.sha256(identity.encode()).hexdigest()[:48],
            profile=settings.profile,
        )

    @classmethod
    async def discover(cls, settings: Settings) -> "MCPAdapter":
        """Trust only the configured issuer and same-origin advertised endpoints."""
        try:
            resource = urlsplit(settings.resource)
            issuer = urlsplit(settings.issuer)
            resource_metadata = urlunsplit(
                (
                    resource.scheme,
                    resource.netloc,
                    "/.well-known/oauth-protected-resource" + resource.path.rstrip("/"),
                    "",
                    "",
                )
            )
            issuer_metadata = urlunsplit(
                (
                    issuer.scheme,
                    issuer.netloc,
                    "/.well-known/oauth-authorization-server" + issuer.path.rstrip("/"),
                    "",
                    "",
                )
            )
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30), trust_env=False
            ) as session:
                protected = await document(session, resource_metadata)
                if protected.get("resource") != settings.resource:
                    raise m.AuthenticationError("MCP OAuth resource metadata mismatch")
                servers = protected.get("authorization_servers")
                if not isinstance(servers, list) or settings.issuer not in servers:
                    raise m.AuthenticationError(
                        "MCP OAuth issuer is not authorized by the resource"
                    )
                metadata = await document(session, issuer_metadata)
            if metadata.get("issuer") != settings.issuer:
                raise m.AuthenticationError("MCP OAuth issuer metadata mismatch")
            methods = metadata.get("code_challenge_methods_supported")
            if not isinstance(methods, list) or "S256" not in methods:
                raise m.AuthenticationError("MCP OAuth requires PKCE S256")
            endpoints: list[str] = []
            for key in ("authorization_endpoint", "token_endpoint"):
                value = metadata.get(key)
                if not isinstance(value, str):
                    raise m.AuthenticationError("MCP OAuth endpoint is missing")
                endpoint(value)
                parsed = urlsplit(value)
                if (parsed.scheme, parsed.netloc) != (issuer.scheme, issuer.netloc):
                    raise m.AuthenticationError(
                        "MCP OAuth endpoint must share the configured issuer origin"
                    )
                endpoints.append(value)
            return cls(settings, *endpoints)
        except m.AuthenticationError:
            raise
        except Exception:
            raise m.AuthenticationError("MCP OAuth metadata discovery failed") from None

    def authorization_url(self, challenge: str, state: str) -> str:
        return (
            self.authorization_endpoint
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": self.settings.client_id,
                    "redirect_uri": self.redirect_uri,
                    "state": state,
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                    "resource": self.settings.resource,
                    "scope": " ".join(self.settings.scopes),
                }
            )
        )

    async def exchange(
        self, profile: m.Profile, code: str, verifier: str
    ) -> m.Credential:
        if profile.key != self.profile.key:
            raise m.AuthenticationError("MCP OAuth credential identity mismatch")
        return await self._token(
            {
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": verifier,
                "redirect_uri": self.redirect_uri,
            }
        )

    async def refresh(self, credential: m.Credential) -> m.Credential:
        if credential.key != self.profile.key:
            raise m.AuthenticationError("MCP OAuth credential identity mismatch")
        if not credential.refresh_token:
            raise m.LoginRequired("MCP OAuth authorization required")
        return await self._token(
            {"grant_type": "refresh_token", "refresh_token": credential.refresh_token},
            previous=credential,
        )

    async def _token(
        self, data: dict[str, str], previous: Optional[m.Credential] = None
    ) -> m.Credential:
        """Do not retry exchanges with unknown outcomes or expose endpoint payloads."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30), trust_env=False
            ) as session:
                async with session.post(
                    self.token_endpoint,
                    data={
                        **data,
                        "client_id": self.settings.client_id,
                        "resource": self.settings.resource,
                    },
                    allow_redirects=False,
                ) as response:
                    if response.status != 200:
                        if response.status == 400:
                            failure = await payload(response)
                            if failure.get("error") == "invalid_grant":
                                raise m.LoginRequired(
                                    "MCP OAuth authorization required"
                                )
                        raise m.AuthenticationError(
                            "MCP OAuth token request was rejected"
                        )
                    value = await payload(response)
            refresh = value.get(
                "refresh_token", previous.refresh_token if previous else None
            )
            if refresh is not None and (not isinstance(refresh, str) or not refresh):
                raise ValueError()
            return mcp_tokens.credential(
                value,
                self.profile,
                self.settings.scopes,
                refresh_token=refresh,
            )
        except m.AuthenticationError:
            raise
        except Exception:
            raise m.AuthenticationError("MCP OAuth token request failed") from None
