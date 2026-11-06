import asyncio
import json
from pathlib import Path
from typing import Optional

from aiohttp import web
from mcp import ClientSession, types
from mcp.shared.exceptions import McpError
import pytest

from tests.mcp.test_http import endpoint
from tests.mcp.test_runtime import server
from vectorops.runner import diagnostics
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import models
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError


class ExpiringServer:
    def __init__(self) -> None:
        self.initializations = 0
        self.expired: set[str] = set()
        self.expire_all = False
        self.barrier: Optional[asyncio.Barrier] = None
        self.rejection_started = asyncio.Event()
        self.requests: list[tuple[str, str]] = []
        self.deleted: set[str] = set()

    async def handle(self, request: web.Request) -> web.Response:
        session = request.headers.get("Mcp-Session-Id", "")
        if request.method == "DELETE":
            self.deleted.add(session)
            return web.Response(status=200)
        if request.method != "POST":
            return web.Response(status=405)
        payload = await request.json()
        method = payload["method"]
        self.requests.append((session, method))
        if "id" not in payload:
            return web.Response(status=202)
        if method == "initialize":
            self.initializations += 1
            session = f"session-{self.initializations}"
            result = {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {"tools": {}, "prompts": {}, "resources": {}},
                "serverInfo": {"name": "test", "version": "1"},
            }
        elif session in self.expired or self.expire_all:
            self.rejection_started.set()
            if self.barrier is not None:
                await asyncio.wait_for(self.barrier.wait(), 2)
            return web.Response(status=404, text="private-response-body")
        elif method == "tools/list":
            result = {"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]}
        elif method == "prompts/list":
            result = {"prompts": [{"name": "prompt"}]}
        elif method == "resources/list":
            result = {"resources": [{"name": "resource", "uri": "test://resource"}]}
        else:
            result = {"content": []}
        return web.json_response(
            {"jsonrpc": "2.0", "id": payload["id"], "result": result},
            headers={"Mcp-Session-Id": session},
        )


@pytest.mark.parametrize("content", [False, True])
@pytest.mark.parametrize("shared", [False, True])
async def test_expired_session_discovery_reconnects_once(
    content: bool, shared: bool
) -> None:
    remote = ExpiringServer()
    pool = SharedPool(AuthenticationService()) if shared else None
    runtime = MCPRuntime(pool=pool)
    async with endpoint(remote.handle) as url:
        config = models.HTTPServer(url=url)
        discover = runtime.discover_content if content else runtime.discover
        try:
            initial = await discover("test", config)
            old = await runtime._connection(config)
            assert not old.task.done()
            remote.expired.add("session-1")
            assert await discover("test", config) == initial
            assert remote.initializations == 2
            assert await runtime._connection(config) is not old
            assert old.task.done()
            assert "session-1" in remote.deleted
            assert await discover("test", config) == initial
            assert remote.initializations == 2
        finally:
            await runtime.close()
            if pool is not None:
                await pool.close()


async def test_concurrent_borrowers_share_one_replacement() -> None:
    remote = ExpiringServer()
    pool = SharedPool(AuthenticationService())
    first, second = MCPRuntime(pool=pool), MCPRuntime(pool=pool)
    async with endpoint(remote.handle) as url:
        config = models.HTTPServer(url=url)
        try:
            initial = await first.discover("test", config)
            remote.expired.add("session-1")
            remote.barrier = asyncio.Barrier(2)
            first_request = asyncio.create_task(first.discover("test", config))
            await asyncio.wait_for(remote.rejection_started.wait(), 2)
            results = await asyncio.wait_for(
                asyncio.gather(first_request, second.discover("test", config)),
                5,
            )
            assert results == [initial, initial]
            assert remote.initializations == 2
            assert await first._connection(config) is await second._connection(config)
            assert pool.status(config).connected
            assert pool.status(config).leases == 2
        finally:
            await asyncio.gather(first.close(), second.close())
            await pool.close()


async def test_failed_call_is_not_replayed_and_invalidates_status() -> None:
    remote = ExpiringServer()
    pool = SharedPool(AuthenticationService())
    runtime = MCPRuntime(pool=pool)
    async with endpoint(remote.handle) as url:
        config = models.HTTPServer(url=url)
        try:
            initial = await runtime.discover("test", config)
            remote.expired.add("session-1")
            with pytest.raises(MCPError, match="remote outcome may be unknown"):
                await runtime.call(config, "echo", {})
            assert not pool.status(config).connected
            assert remote.initializations == 1
            assert sum(method == "tools/call" for _, method in remote.requests) == 1
            assert await runtime.discover("test", config) == initial
            assert remote.initializations == 2
            assert pool.status(config).connected
            assert sum(method == "tools/call" for _, method in remote.requests) == 1
        finally:
            await runtime.close()
            await pool.close()


async def test_cancellation_during_reconnection_cleans_up() -> None:
    remote = ExpiringServer()
    reconnecting = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: web.Request) -> web.Response:
        response = await remote.handle(request)
        if request.method == "POST":
            payload = await request.json()
            if payload["method"] == "initialize" and remote.initializations == 2:
                reconnecting.set()
                await release.wait()
        return response

    runtime = MCPRuntime()
    async with endpoint(handler) as url:
        config = models.HTTPServer(url=url, limits=models.Limits(shutdown_seconds=0.1))
        try:
            await runtime.discover("test", config)
            old = await runtime._connection(config)
            remote.expired.add("session-1")
            task = asyncio.create_task(runtime.discover("test", config))
            await asyncio.wait_for(reconnecting.wait(), 2)
            replacement = next(iter(runtime._connections.values()))
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
            assert old.task.done()
            assert replacement.task.done()
            assert not runtime._connections
            assert remote.initializations == 2
        finally:
            release.set()
            await runtime.close()


async def test_discovery_retry_is_bounded_and_diagnostics_are_sanitized(
    tmp_path: Path,
) -> None:
    remote = ExpiringServer()
    remote.expire_all = True
    pool = SharedPool(AuthenticationService())
    runtime = MCPRuntime(pool=pool)
    async with diagnostics.DiagnosticLog(tmp_path) as logs:
        with logs.activate(), diagnostics.bind(execution_id="execution"):
            async with endpoint(remote.handle) as url:
                config = models.HTTPServer(
                    url=url, headers={"X-Private": "private-header"}
                )
                try:
                    with pytest.raises(MCPError, match="MCP discovery failed"):
                        await asyncio.wait_for(runtime.discover("test", config), 5)
                    assert remote.initializations == 2
                    assert not pool.status(config).connected
                finally:
                    await runtime.close()
                    await pool.close()
        await logs.flush()
    text = "".join(path.read_text() for path in (tmp_path / "logs").glob("*.jsonl"))
    assert "private-" not in text
    assert url not in text
    entries = [json.loads(line) for line in text.splitlines()]
    assert len(entries) == 2
    for index, entry in enumerate(entries):
        assert entry["event"] == "mcp.discovery_failed"
        assert entry["execution_id"] == "execution"
        assert entry["fields"]["error_type"] == "McpError"
        assert entry["fields"]["frames"]
        assert entry["fields"]["session_error"] == "session_terminated"
        assert entry["fields"]["retrying"] == (index == 0)


@pytest.mark.parametrize("code", [32600, -32602, -32603])
async def test_other_sdk_errors_do_not_reconnect(
    code: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = MCPRuntime()
    config = server()
    attempts = 0

    async def fail(*args: object, **kwargs: object) -> types.ListToolsResult:
        nonlocal attempts
        attempts += 1
        raise McpError(types.ErrorData(code=code, message="private-error"))

    try:
        await runtime.discover("test", config)
        old = await runtime._connection(config)
        monkeypatch.setattr(ClientSession, "list_tools", fail)
        with pytest.raises(MCPError, match="MCP discovery failed"):
            await runtime.discover("test", config)
        assert attempts == 1
        assert await runtime._connection(config) is old
        assert old.usable
    finally:
        await runtime.close()
