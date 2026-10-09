"""Pinned-issuer MCP OAuth for pre-registered public clients, without redirects."""

import hashlib
import json
import math
import time
from typing import Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import aiohttp
import pydantic

from . import models as m


def endpoint(value: str) -> str:
    parsed = urlsplit(value)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "OAuth endpoints must be absolute URLs without credentials, query, or fragment"
        )
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost", "::1")
    ):
        raise ValueError("OAuth endpoints require HTTPS except on loopback")
    if parsed.port == 0:
        raise ValueError("OAuth endpoint port must be nonzero")
    return value


class Settings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )
    issuer: str
    resource: str
    client_id: str = pydantic.Field(min_length=1, max_length=1024)
    redirect_uri: str
    scopes: list[str] = pydantic.Field(default_factory=list, max_length=64)
    profile: str = pydantic.Field(default="default", pattern=r"^[A-Za-z0-9_-]{1,64}$")

    @pydantic.field_validator("issuer", "resource", "redirect_uri")
    @classmethod
    def valid_url(cls, value: str) -> str:
        return endpoint(value)

    @pydantic.field_validator("redirect_uri")
    @classmethod
    def loopback(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in ("127.0.0.1", "localhost", "::1")
            or not parsed.port
            or not parsed.path
        ):
            raise ValueError(
                "OAuth callback requires a loopback HTTP URL with explicit port and path"
            )
        return value

    @pydantic.field_validator("scopes")
    @classmethod
    def scope_tokens(cls, values: list[str]) -> list[str]:
        for value in values:
            if not value or any(
                ord(char) < 33 or ord(char) > 126 or char in ('"', "\\")
                for char in value
            ):
                raise ValueError("OAuth scopes must be valid scope tokens")
        return sorted(set(values))


async def document(
    session: aiohttp.ClientSession, url: str
) -> dict[str, pydantic.JsonValue]:
    async with session.get(url, allow_redirects=False) as response:
        if response.status != 200:
            raise m.AuthenticationError("MCP OAuth metadata request was rejected")
        return await payload(response)


async def payload(response: aiohttp.ClientResponse) -> dict[str, pydantic.JsonValue]:
    data = bytearray()
    async for chunk in response.content.iter_chunked(16384):
        data.extend(chunk)
        if len(data) > 65536:
            raise m.AuthenticationError("MCP OAuth response exceeds the byte limit")
    try:
        result = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
            bytes(data)
        )
    except Exception:
        raise m.AuthenticationError("Invalid MCP OAuth response") from None
    return result


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
            token, lifetime = value.get("access_token"), value.get("expires_in")
            refresh = value.get(
                "refresh_token", previous.refresh_token if previous else None
            )
            if (
                not isinstance(token, str)
                or not token
                or any(char.isspace() for char in token)
            ):
                raise ValueError()
            if (
                not isinstance(lifetime, (int, float))
                or isinstance(lifetime, bool)
                or not math.isfinite(lifetime)
                or lifetime <= 60
            ):
                raise ValueError()
            if str(value.get("token_type", "")).lower() != "bearer":
                raise ValueError()
            if refresh is not None and (not isinstance(refresh, str) or not refresh):
                raise ValueError()
            scope = value.get("scope")
            if scope is not None and (
                not isinstance(scope, str)
                or not set(self.settings.scopes).issubset(scope.split())
            ):
                raise m.AuthenticationError("MCP OAuth granted insufficient scopes")
            return m.Credential(
                provider=self.profile.provider,
                profile=self.profile.profile,
                kind="oauth2",
                secret=token,
                refresh_token=refresh,
                expires_at=time.time() + lifetime,
            )
        except m.AuthenticationError:
            raise
        except Exception:
            raise m.AuthenticationError("MCP OAuth token request failed") from None
