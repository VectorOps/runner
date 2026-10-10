"""Bounded MCP OAuth responses and grant-independent bearer token policy."""

import math
import time
from typing import Optional

import aiohttp
import pydantic

from . import models


async def payload(response: aiohttp.ClientResponse) -> dict[str, pydantic.JsonValue]:
    data = bytearray()
    async for chunk in response.content.iter_chunked(16384):
        data.extend(chunk)
        if len(data) > 65536:
            raise models.AuthenticationError(
                "MCP OAuth response exceeds the byte limit"
            )
    try:
        return pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
            bytes(data)
        )
    except Exception:
        raise models.AuthenticationError("Invalid MCP OAuth response") from None


def credential(
    value: dict[str, pydantic.JsonValue],
    profile: models.Profile,
    scopes: list[str],
    *,
    refresh_token: Optional[str] = None,
    scope_error: str = "MCP OAuth granted insufficient scopes",
) -> models.Credential:
    """Validate common bearer fields; grants own refresh-token interpretation."""
    token, lifetime = value.get("access_token"), value.get("expires_in")
    if (
        not isinstance(token, str)
        or not token
        or any(ord(char) < 33 or ord(char) > 126 for char in token)
    ):
        raise ValueError("invalid bearer token")
    if (
        not isinstance(lifetime, (int, float))
        or isinstance(lifetime, bool)
        or not math.isfinite(lifetime)
        or lifetime <= 60
    ):
        raise ValueError("invalid token lifetime")
    if str(value.get("token_type", "")).lower() != "bearer":
        raise ValueError("invalid token type")
    scope = value.get("scope")
    if scope is not None and (
        not isinstance(scope, str) or not set(scopes).issubset(scope.split())
    ):
        raise models.AuthenticationError(scope_error)
    return models.Credential(
        provider=profile.provider,
        profile=profile.profile,
        kind="oauth2",
        secret=token,
        refresh_token=refresh_token,
        expires_at=time.time() + lifetime,
    )
