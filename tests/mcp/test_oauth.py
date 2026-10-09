import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlsplit

import aiohttp
from aiohttp import web
import httpx
import pydantic
import pytest

from vectorops.runner.authentication import mcp_oauth, models as am
from vectorops.runner.authentication.mcp_http import MCPHTTPAuth
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, oauth_host
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.mcp import models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner.workflow import WorkflowRunner


class OAuthServer:
    def __init__(self) -> None:
        self.base = ""
        self.failure = ""
        self.grants: list[dict[str, str]] = []
        self.headers: list[Optional[str]] = []
        self.rejections = 0
        self.challenge = ""
        self.token_started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    def settings(self) -> mcp_oauth.Settings:
        return mcp_oauth.Settings(
            issuer=self.base,
            resource=self.base + "/mcp",
            client_id="registered-client",
            redirect_uri="http://127.0.0.1:1456/callback",
            scopes=["read"],
        )

    async def handle(self, request: web.Request) -> web.Response:
        if request.path.startswith("/.well-known/oauth-protected-resource"):
            return web.json_response(
                {
                    "resource": self.base
                    + ("/wrong" if self.failure == "resource" else "/mcp"),
                    "authorization_servers": [self.base],
                }
            )
        if request.path == "/.well-known/oauth-authorization-server":
            if self.failure == "redirect":
                raise web.HTTPFound(self.base + "/private")
            return web.json_response(
                {
                    "issuer": self.base
                    + ("/wrong" if self.failure == "issuer" else ""),
                    "authorization_endpoint": self.base + "/authorize",
                    "token_endpoint": (
                        "https://evil.test/token"
                        if self.failure == "endpoint"
                        else self.base + "/token"
                    ),
                    "code_challenge_methods_supported": (
                        [] if self.failure == "pkce" else ["S256"]
                    ),
                }
            )
        if request.path == "/token":
            data = await request.post()
            self.grants.append({key: str(value) for key, value in data.items()})
            if data.get("grant_type") == "authorization_code" and self.challenge:
                verifier = str(data["code_verifier"])
                assert (
                    base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
                    .decode()
                    .rstrip("=")
                    == self.challenge
                )
            self.token_started.set()
            await self.release.wait()
            if self.failure == "invalid_grant":
                return web.json_response(
                    {"error": "invalid_grant", "error_description": "secret-detail"},
                    status=400,
                )
            if self.failure == "token_redirect":
                raise web.HTTPFound(self.base + "/private")
            return web.json_response(
                {
                    "access_token": f"private-access-{len(self.grants)}",
                    "refresh_token": f"private-refresh-{len(self.grants)}",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "write" if self.failure == "scope" else "read",
                }
            )
        if request.path == "/mcp":
            self.headers.append(request.headers.get("Authorization"))
            if self.rejections:
                self.rejections -= 1
                return web.Response(
                    status=401,
                    headers={
                        "WWW-Authenticate": 'Bearer error="invalid_token", resource_metadata="https://evil.test/metadata"'
                    },
                )
            if request.method != "POST":
                return web.Response(status=405)
            value = await request.json()
            if "id" not in value:
                return web.Response(status=202)
            if value["method"] == "initialize":
                result = {
                    "protocolVersion": value["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fixture", "version": "1"},
                }
            elif value["method"] == "tools/list":
                result = {
                    "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]
                }
            else:
                result = {"content": [{"type": "text", "text": "done"}]}
            return web.json_response(
                {"jsonrpc": "2.0", "id": value["id"], "result": result}
            )
        raise AssertionError("untrusted destination was contacted")


@asynccontextmanager
async def server() -> AsyncIterator[OAuthServer]:
    fixture = OAuthServer()
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", fixture.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    fixture.base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    try:
        yield fixture
    finally:
        fixture.release.set()
        await runner.cleanup()


async def pending(auth: AuthenticationService) -> am.Requirement:
    async with asyncio.timeout(5):
        while not auth.pending():
            await asyncio.sleep(0.01)
    return auth.pending()[0]


async def authorize(auth: AuthenticationService) -> am.Claim:
    requirement = await pending(auth)
    claim = auth.claim(requirement.id, "host")
    url = auth.ready(am.Attempt(id=claim.id, attempt=claim.attempt), "host")
    query = parse_qs(urlsplit(url).query)
    assert query["code_challenge_method"] == ["S256"]
    assert query["state"] == [claim.state]
    await auth.complete(
        am.Callback(
            id=claim.id, attempt=claim.attempt, state=claim.state, code="private-code"
        ),
        "host",
    )
    return claim


@pytest.mark.parametrize(
    "failure", ["resource", "issuer", "endpoint", "pkce", "redirect"]
)
async def test_metadata_fails_closed(failure: str) -> None:
    async with server() as remote:
        remote.failure = failure
        with pytest.raises(am.AuthenticationError):
            await mcp_oauth.MCPAdapter.discover(remote.settings())
        assert remote.grants == []


async def test_pkce_login_shared_refresh_and_resource_binding() -> None:
    async with server() as remote:
        auth = AuthenticationService()
        adapter = await auth.mcp_adapter(remote.settings())
        tasks = [
            asyncio.create_task(auth.acquire(adapter.profile, f"run-{i}"))
            for i in range(2)
        ]
        await authorize(auth)
        values = await asyncio.gather(*tasks)
        assert values[0].secret == values[1].secret == "private-access-1"
        grant = remote.grants[0]
        assert grant["resource"] == remote.settings().resource
        assert grant["client_id"] == "registered-client"
        assert len(grant["code_verifier"]) >= 43
        refreshed = await asyncio.gather(
            *(
                auth.acquire(adapter.profile, rejected=values[0].secret)
                for _ in range(4)
            )
        )
        assert len(remote.grants) == 2
        assert all(value.secret == "private-access-2" for value in refreshed)
        changed = mcp_oauth.MCPAdapter(
            remote.settings().model_copy(update={"resource": remote.base + "/other"}),
            adapter.authorization_endpoint,
            adapter.token_endpoint,
        )
        assert changed.profile.key != adapter.profile.key
        restored = AuthenticationService()
        same = await restored.mcp_adapter(remote.settings())
        assert (await restored.acquire(same.profile)).secret == "private-access-2"


async def test_mcp_uses_existing_host_callback_flow(
    workspace: Path, unused_tcp_port: int
) -> None:
    async with server() as remote:
        settings = remote.settings().model_copy(
            update={"redirect_uri": f"http://127.0.0.1:{unused_tcp_port}/callback"}
        )
        async with RunnerService(workspace) as runner:
            adapter = await runner.authentication.mcp_adapter(settings)
            token = (await runner.configuration.load()).control_token
            transport = RunnerServer(runner, token)
            port = await transport.listen()
            task = asyncio.create_task(
                runner.authentication.acquire(adapter.profile, "execution")
            )
            try:
                requirement = await pending(runner.authentication)
                async with await client.RunnerClient.tcp(port) as host:

                    async def launch(url: str) -> None:
                        query = parse_qs(urlsplit(url).query)
                        remote.challenge = query["code_challenge"][0]
                        assert query["resource"] == [settings.resource]
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                settings.redirect_uri,
                                params={"state": "wrong", "code": "private-code"},
                            ) as response:
                                assert response.status == 400
                            async with session.get(
                                settings.redirect_uri,
                                params={
                                    "state": query["state"][0],
                                    "code": "private-code",
                                },
                            ) as response:
                                assert response.status == 200

                    await oauth_host.authenticate(host, requirement.id, launch=launch)
                    assert (await task).secret == "private-access-1"
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await transport.aclose()


async def test_refresh_rotation_is_published_before_cancellation() -> None:
    async with server() as remote:
        auth = AuthenticationService()
        adapter = await auth.mcp_adapter(remote.settings())
        login = asyncio.create_task(auth.acquire(adapter.profile))
        await authorize(auth)
        value = await login
        remote.release.clear()
        remote.token_started.clear()
        task = asyncio.create_task(auth.acquire(adapter.profile, rejected=value.secret))
        await asyncio.wait_for(remote.token_started.wait(), 3)
        task.cancel()
        remote.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        persisted = await auth.store.load(adapter.profile)
        assert persisted is not None and persisted.refresh_token == "private-refresh-2"


async def test_workflow_oauth_suspension_restart_and_no_secret_storage(
    workspace: Path,
) -> None:
    async with server() as remote:
        settings = remote.settings()
        (workspace / "config.toml").write_text(
            "[components.tools.approval]\napprove_all = true\n"
            f'[components.mcp.servers.test]\nkind = "http"\nurl = {json.dumps(settings.resource)}\n'
            "[components.mcp.servers.test.oauth]\n"
            + "\n".join(
                f"{key} = {json.dumps(value)}"
                for key, value in settings.model_dump().items()
            )
        )
        runner = WorkflowRunner(workspace)
        task = asyncio.create_task(
            runner.run("python:tests.fixtures.mcp_workflows:echo")
        )
        await pending(runner.authentication)
        identifier = runner.last_execution_id
        assert identifier is not None and runner.suspend_inputs(identifier)
        runner.stop()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not runner.authentication.pending()
        assert (await runner.execution_state(identifier)).status == "running"
        restarted = WorkflowRunner(workspace)
        resumed = asyncio.create_task(restarted.resume(identifier))
        try:
            await authorize(restarted.authentication)
            await asyncio.wait_for(resumed, 5)
        finally:
            if not resumed.done():
                resumed.cancel()
                await asyncio.gather(resumed, return_exceptions=True)
        for path in (workspace / "data").rglob("*"):
            if path.is_file():
                data = path.read_bytes()
                for secret in (b"private-access", b"private-refresh", b"private-code"):
                    assert secret not in data


async def test_configuration_contract_retains_pre_oauth_digest(workspace: Path) -> None:
    from tests.mcp.test_replay import configure

    configure(workspace)
    runner = WorkflowRunner(workspace)
    result = await runner.run("python:tests.fixtures.mcp_workflows:echo")
    assert runner.last_execution_id is not None
    output = workspace / "history.jsonl"
    await runner.export_execution(output, runner.last_execution_id)
    assert (
        "e27b17f60c6866b249b37b466f640e1123e34cfe37c0fc41eb7103c36cbd58e3"
        in output.read_text()
    )
    assert await WorkflowRunner(workspace).resume(runner.last_execution_id) == result


async def test_cancellation_keeps_other_execution_login_pending() -> None:
    async with server() as remote:
        auth = AuthenticationService()
        adapter = await auth.mcp_adapter(remote.settings())
        first = asyncio.create_task(auth.acquire(adapter.profile, "first"))
        second = asyncio.create_task(auth.acquire(adapter.profile, "second"))
        try:
            async with asyncio.timeout(5):
                while not (auth.waiting_for("first") and auth.waiting_for("second")):
                    await asyncio.sleep(0.01)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            assert auth.waiting_for("second") and not auth.waiting_for("first")
            auth.cancel((await pending(auth)).id)
            with pytest.raises(am.AuthenticationCancelled):
                await second
            assert not auth.pending()
        finally:
            first.cancel()
            second.cancel()
            await asyncio.gather(first, second, return_exceptions=True)


async def test_401_refresh_is_bounded_and_does_not_follow_challenge_urls() -> None:
    async with server() as remote:
        auth = AuthenticationService()
        adapter = await auth.mcp_adapter(remote.settings())
        task = asyncio.create_task(auth.acquire(adapter.profile))
        await authorize(auth)
        await task
        transport = MCPHTTPAuth(auth, adapter, "run")
        await transport.prepare()
        async with httpx.AsyncClient(auth=transport) as client:
            remote.rejections = 1
            response = await client.post(
                remote.settings().resource,
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            )
            assert response.status_code == 202
            assert remote.headers == [
                "Bearer private-access-1",
                "Bearer private-access-2",
            ]
            remote.rejections = 3
            response = await client.post(remote.settings().resource, json={})
            assert response.status_code == 401
            assert len(remote.headers) == 4
            with pytest.raises(am.AuthenticationError, match="destination"):
                await client.get(remote.base + "/private")


@pytest.mark.parametrize("failure", ["scope", "token_redirect", "invalid_grant"])
async def test_token_failures_are_sanitized(failure: str) -> None:
    async with server() as remote:
        adapter = await mcp_oauth.MCPAdapter.discover(remote.settings())
        remote.failure = failure
        with pytest.raises(am.AuthenticationError) as error:
            await adapter.exchange(adapter.profile, "private-code", "private-verifier")
        assert "secret-detail" not in str(error.value)
        assert "private-code" not in str(error.value)


async def test_runtime_login_outlives_startup_timeout_and_replay_is_offline(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with server() as remote:
        settings = remote.settings()
        (workspace / "config.toml").write_text(
            "[components.tools.approval]\napprove_all = true\n"
            f'[components.mcp.servers.test]\nkind = "http"\nurl = {json.dumps(settings.resource)}\n'
            "[components.mcp.servers.test.limits]\nstartup_seconds = 0.1\n"
            "[components.mcp.servers.test.oauth]\n"
            + "\n".join(
                f"{key} = {json.dumps(value)}"
                for key, value in settings.model_dump().items()
            )
        )
        runner = WorkflowRunner(workspace)
        task = asyncio.create_task(
            runner.run("python:tests.fixtures.mcp_workflows:echo")
        )
        await pending(runner.authentication)
        await asyncio.sleep(0.2)
        assert not task.done()
        assert runner.last_execution_id is not None
        assert runner.authentication.waiting_for(runner.last_execution_id)
        await authorize(runner.authentication)
        first = await asyncio.wait_for(task, 5)
        identifier = runner.last_execution_id
        assert identifier is not None
        output = workspace / "history.jsonl"
        await runner.export_execution(output, identifier)
        text = output.read_text()
        assert all(
            secret not in text
            for secret in (
                "private-access",
                "private-refresh",
                "private-code",
                "code_verifier",
            )
        )

        def forbidden(*args: object, **kwargs: object) -> None:
            raise AssertionError("replay contacted auth or MCP")

        monkeypatch.setattr(AuthenticationService, "mcp_adapter", forbidden)
        monkeypatch.setattr(MCPRuntime, "discover", forbidden)
        assert await WorkflowRunner(workspace).resume(identifier) == first


def test_no_oauth_serialization_preserves_existing_tool_names() -> None:
    config = models.HTTPServer(url="https://example.test/mcp")
    assert "oauth" not in config.model_dump(mode="json")
    assert (
        models.tool_name("test", config, "echo") == "mcp_test_echo_d821d48d7f23b2936b8c"
    )
    with pytest.raises(pydantic.ValidationError):
        models.HTTPServer(
            url="https://example.test/mcp",
            credential="key",
            oauth=mcp_oauth.Settings(
                issuer="https://issuer.test",
                resource="https://example.test/mcp",
                client_id="client",
                redirect_uri="http://127.0.0.1:1234/callback",
            ),
        )
