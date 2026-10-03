import base64
import json
import time

from aiohttp import web
from connect.credentials import chatgpt
import pytest

from vectorops.runner.authentication import models as m, oauth


def token() -> str:
    payload = (
        base64.urlsafe_b64encode(
            json.dumps(
                {"https://api.openai.com/auth": {"chatgpt_account_id": "account"}}
            ).encode()
        )
        .decode()
        .rstrip("=")
    )
    return f"header.{payload}.signature"


@pytest.mark.parametrize(
    "failure", ["none", "invalid_grant", "invalid_client", "transient", "malformed"]
)
async def test_token_exchange_classification_and_redaction(
    unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    attempts = 0

    async def respond(request: web.Request) -> web.Response:
        nonlocal attempts
        attempts += 1
        form = await request.post()
        assert form["client_id"] == chatgpt.CLIENT_ID
        assert form["grant_type"] == "authorization_code"
        assert form["code_verifier"] == "private-verifier"
        if failure == "transient" and attempts == 1:
            return web.json_response(
                {"error_description": "private-error-detail"}, status=503
            )
        if failure in ("invalid_grant", "invalid_client"):
            return web.json_response(
                {"error": failure, "error_description": "private-error-detail"},
                status=400,
            )
        if failure == "malformed":
            return web.json_response({"access_token": "private-error-detail"})
        return web.json_response(
            {
                "access_token": token(),
                "refresh_token": "private-refresh",
                "expires_in": 3600,
            }
        )

    app = web.Application()
    app.router.add_post("/token", respond)
    server = web.AppRunner(app, access_log=None)
    await server.setup()
    await web.TCPSite(server, "127.0.0.1", unused_tcp_port).start()
    monkeypatch.setattr(
        chatgpt, "TOKEN_URL", f"http://127.0.0.1:{unused_tcp_port}/token"
    )
    try:
        if failure in ("none", "transient"):
            value = await oauth.ChatGPT().exchange(
                m.Profile(provider="chatgpt"), "private-code", "private-verifier"
            )
            assert value.account_id == "account"
            assert value.refresh_token == "private-refresh"
            assert value.expires_at > time.time() + 3500
            assert attempts == (2 if failure == "transient" else 1)
        else:
            with pytest.raises(m.AuthenticationError) as error:
                await oauth.ChatGPT().exchange(
                    m.Profile(provider="chatgpt"), "private-code", "private-verifier"
                )
            assert isinstance(error.value, m.LoginRequired) == (
                failure == "invalid_grant"
            )
            assert "private" not in str(error.value)
    finally:
        await server.cleanup()


async def test_refresh_preserves_refresh_token_when_not_rotated(
    unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def respond(request: web.Request) -> web.Response:
        form = await request.post()
        assert form["grant_type"] == "refresh_token"
        assert form["refresh_token"] == "keep-refresh"
        return web.json_response({"access_token": token(), "expires_in": 3600})

    app = web.Application()
    app.router.add_post("/token", respond)
    server = web.AppRunner(app, access_log=None)
    await server.setup()
    await web.TCPSite(server, "127.0.0.1", unused_tcp_port).start()
    monkeypatch.setattr(
        chatgpt, "TOKEN_URL", f"http://127.0.0.1:{unused_tcp_port}/token"
    )
    try:
        value = await oauth.ChatGPT().refresh(
            m.Credential(
                provider="chatgpt",
                kind="oauth2",
                secret="expired",
                refresh_token="keep-refresh",
            )
        )
        assert value.refresh_token == "keep-refresh"
    finally:
        await server.cleanup()
