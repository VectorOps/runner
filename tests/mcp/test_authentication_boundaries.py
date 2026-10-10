import hashlib
import json
from typing import Optional
from unittest.mock import AsyncMock

from aiohttp import web
import httpx
import pydantic
import pytest

from tests.mcp.test_oauth import server
from vectorops.runner.authentication import (
    mcp_client_credentials,
    mcp_oauth,
    mcp_settings,
)
from vectorops.runner.authentication import models as am
from vectorops.runner.authentication.mcp_http import MCPHTTPAuth
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import models
from vectorops.runner.mcp.pool import SharedPool


@pytest.mark.parametrize("legacy", [False, True])
async def test_logout_after_restart_is_offline_and_forgets_all_bound_profiles(
    legacy: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth = AuthenticationService()
    async with server() as remote:
        settings = remote.settings()
        config = (
            models.HTTPServer(url=settings.resource, oauth=settings)
            if legacy
            else models.HTTPServer(
                url=settings.resource,
                auth=mcp_settings.AuthorizationCode(**settings.model_dump()),
            )
        )
        selected = config.authentication_settings()
        assert isinstance(selected, mcp_settings.Settings)
        first = await auth.mcp_adapter(selected)
        second = mcp_oauth.MCPAdapter(
            selected, first.authorization_endpoint, remote.base + "/rotated-token"
        )
        with monkeypatch.context() as patch:
            patch.setattr(
                mcp_oauth.MCPAdapter, "discover", AsyncMock(return_value=second)
            )
            await auth.mcp_adapter(selected)
        for adapter in (first, second):
            await auth.provision(
                am.Credential(
                    provider=adapter.profile.provider,
                    profile=adapter.profile.profile,
                    kind="oauth2",
                    secret="access",
                    refresh_token="refresh",
                )
            )
    forbidden = AsyncMock(side_effect=AssertionError("logout performed discovery"))
    monkeypatch.setattr(mcp_oauth.MCPAdapter, "discover", forbidden)
    restarted = AuthenticationService()
    pool = SharedPool(restarted)
    try:
        await pool.logout(config)
        await pool.logout(config)
        for adapter in (first, second):
            assert await restarted.store.load(adapter.profile) is None
        forbidden.assert_not_awaited()
    finally:
        await pool.close()


async def test_logout_migrates_pre_binding_credentials() -> None:
    async with server() as remote:
        auth = AuthenticationService()
        adapter = await mcp_oauth.MCPAdapter.discover(remote.settings())
        await auth.store.provision(
            am.Credential(
                provider=adapter.profile.provider, kind="oauth2", secret="legacy"
            )
        )
        await auth.mcp.forget(remote.settings())
        assert await auth.store.load(adapter.profile) is None


@pytest.mark.parametrize("legacy", [False, True])
def test_credential_identity_and_settings_compatibility(legacy: bool) -> None:
    settings = mcp_oauth.Settings(
        issuer="https://issuer.test",
        resource="https://resource.test/mcp",
        client_id="client",
        redirect_uri="http://localhost:1234/callback",
        scopes=["write", "read", "read"],
    )
    assert mcp_oauth.Settings is mcp_settings.Settings
    selected = (
        settings if legacy else mcp_settings.AuthorizationCode(**settings.model_dump())
    )
    adapter = mcp_oauth.MCPAdapter(
        selected, "https://issuer.test/authorize", "https://issuer.test/token"
    )
    identity = json.dumps(
        {
            "settings": selected.model_dump(mode="json"),
            "authorization_endpoint": adapter.authorization_endpoint,
            "token_endpoint": adapter.token_endpoint,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    assert (
        adapter.profile.provider
        == "mcp-oauth-" + hashlib.sha256(identity.encode()).hexdigest()[:48]
    )
    assert selected.scopes == ["read", "write"]
    client = mcp_settings.ClientCredentials(
        token_endpoint="https://issuer.test/token",
        resource=settings.resource,
        client_id="client",
        client_secret="secret",
        scopes=["write", "read", "read"],
    )
    client_identity = json.dumps(
        client.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    assert (
        mcp_client_credentials.profile(client).provider
        == "mcp-client-" + hashlib.sha256(client_identity.encode()).hexdigest()[:48]
    )
    assert client.scopes == selected.scopes


class Tokens:
    def __init__(self) -> None:
        self.calls: list[tuple[Optional[str], Optional[str], bool]] = []

    async def acquire(
        self,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> am.Credential:
        self.calls.append((execution_id, rejected, interactive))
        return am.Credential(
            provider="test", kind="oauth2", secret=f"token-{len(self.calls)}"
        )


async def test_http_auth_uses_only_token_source_and_bound_resource() -> None:
    source = Tokens()
    auth = MCPHTTPAuth(source, "https://resource.test/mcp", "run")
    sent: list[str] = []

    async def reject(request: httpx.Request) -> httpx.Response:
        sent.append(request.headers["Authorization"])
        return httpx.Response(401, headers={"WWW-Authenticate": "Bearer"})

    await auth.prepare()
    async with httpx.AsyncClient(
        auth=auth, transport=httpx.MockTransport(reject)
    ) as client:
        assert (await client.get("https://resource.test/mcp")).status_code == 401
        with pytest.raises(am.AuthenticationError, match="destination"):
            await client.get("https://other.test/mcp")
    assert sent == ["Bearer token-1", "Bearer token-2"]
    assert source.calls == [("run", None, True), ("run", "token-1", False)]
    assert auth.rejected == "token-2"


@pytest.mark.parametrize(
    "field,value",
    [
        ("access_token", "bad\x00token"),
        ("access_token", "bad\x7ftoken"),
        ("access_token", "bad\u00e9token"),
        ("access_token", "bad token"),
        ("expires_in", True),
        ("expires_in", 60),
        ("token_type", "Basic"),
        ("scope", "write"),
    ],
)
async def test_grants_share_bearer_validation(
    field: str, value: pydantic.JsonValue
) -> None:
    async def token(request: web.Request) -> web.Response:
        result: dict[str, pydantic.JsonValue] = {
            "access_token": "valid",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "read",
        }
        result[field] = value
        return web.json_response(result)

    app = web.Application()
    app.router.add_post("/token", token)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    settings = mcp_settings.Settings(
        issuer=base,
        resource=base + "/mcp",
        client_id="client",
        redirect_uri="http://localhost:1234/callback",
        scopes=["read"],
    )
    adapter = mcp_oauth.MCPAdapter(settings, base + "/authorize", base + "/token")
    client = mcp_settings.ClientCredentials(
        token_endpoint=base + "/token",
        resource=settings.resource,
        client_id="client",
        client_secret="secret",
        scopes=["read"],
    )
    try:
        with pytest.raises(am.AuthenticationError):
            await adapter.exchange(adapter.profile, "code", "verifier")
        with pytest.raises(am.AuthenticationError):
            await mcp_client_credentials.request(client, "secret")
    finally:
        await runner.cleanup()


def test_credential_reuse_policy() -> None:
    value = am.Credential(
        provider="test", kind="oauth2", secret="token", expires_at=161
    )
    assert value.reusable(100)
    assert not value.reusable(101)
    assert not value.reusable(100, "token")
    assert not value.model_copy(update={"expires_at": None}).reusable(100)
    assert not value.model_copy(update={"kind": "api_key"}).reusable(100)
