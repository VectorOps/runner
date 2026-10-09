"""Service-account token acquisition using encrypted credential references."""

import hashlib
import base64
import json
import math
import time
from urllib.parse import quote_plus

import aiohttp

from . import mcp_oauth, mcp_settings, models


def profile(settings: mcp_settings.ClientCredentials) -> models.Profile:
    identity = json.dumps(
        settings.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    return models.Profile(
        provider="mcp-client-" + hashlib.sha256(identity.encode()).hexdigest()[:48],
        profile=settings.profile,
    )


async def request(
    settings: mcp_settings.ClientCredentials, secret: str
) -> models.Credential:
    try:
        data = {
            "grant_type": "client_credentials",
            "resource": settings.resource,
            "scope": " ".join(settings.scopes),
        }
        headers: dict[str, str] = {}
        if settings.token_auth_method == "client_secret_basic":
            encoded = base64.b64encode(
                f"{quote_plus(settings.client_id, safe='')}:{quote_plus(secret, safe='')}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {encoded}"
        else:
            data.update(client_id=settings.client_id, client_secret=secret)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30), trust_env=False
        ) as session:
            async with session.post(
                settings.token_endpoint,
                data=data,
                headers=headers,
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise models.AuthenticationError(
                        "MCP client credentials were rejected"
                    )
                value = await mcp_oauth.payload(response)
        token, lifetime = value.get("access_token"), value.get("expires_in")
        if (
            not isinstance(token, str)
            or not token
            or any(ord(char) < 33 or ord(char) > 126 for char in token)
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
        scope = value.get("scope")
        if scope is not None and (
            not isinstance(scope, str)
            or not set(settings.scopes).issubset(scope.split())
        ):
            raise models.AuthenticationError(
                "MCP client credentials granted insufficient scopes"
            )
        ref = profile(settings)
        return models.Credential(
            provider=ref.provider,
            profile=ref.profile,
            kind="oauth2",
            secret=token,
            expires_at=time.time() + lifetime,
        )
    except models.AuthenticationError:
        raise
    except Exception:
        raise models.AuthenticationError(
            "MCP client credentials request failed"
        ) from None
