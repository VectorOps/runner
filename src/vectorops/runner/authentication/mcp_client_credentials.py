"""Service-account token acquisition using encrypted credential references."""

import hashlib
import base64
import json
from urllib.parse import quote_plus

import aiohttp

from . import mcp_tokens, mcp_settings, models


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
                value = await mcp_tokens.payload(response)
        return mcp_tokens.credential(
            value,
            profile(settings),
            settings.scopes,
            scope_error="MCP client credentials granted insufficient scopes",
        )
    except models.AuthenticationError:
        raise
    except Exception:
        raise models.AuthenticationError(
            "MCP client credentials request failed"
        ) from None
