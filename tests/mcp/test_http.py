import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
import json

from aiohttp import web
import pytest

from vectorops.runner.authentication import models as auth_models
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError


@asynccontextmanager
async def endpoint(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_route("*", "/mcp", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    address = runner.addresses[0]
    try:
        yield f"http://127.0.0.1:{address[1]}/mcp"
    finally:
        await runner.cleanup()


async def test_stateful_http_credentials_sse_roots_and_delete() -> None:
    methods: list[str] = []
    roots_answered = asyncio.Event()
    deleted = asyncio.Event()
    auth = AuthenticationService()
    await auth.provision(
        auth_models.Credential(
            provider="mcp", profile="test", kind="api_key", secret="private-token"
        )
    )

    async def handler(request: web.Request) -> web.StreamResponse:
        assert request.headers["Authorization"] == "Bearer private-token"
        if request.method == "DELETE":
            assert request.headers["Mcp-Session-Id"] == "session"
            deleted.set()
            return web.Response(status=200)
        if request.method == "GET":
            return web.Response(status=405)
        payload = await request.json()
        method = payload.get("method", "response")
        methods.append(method)
        if method != "initialize":
            assert request.headers["Mcp-Session-Id"] == "session"
        if method == "response":
            assert payload["id"] == "roots"
            assert payload["result"]["roots"][0]["uri"] == "file:///tmp/project"
            roots_answered.set()
            return web.Response(status=202)
        if "id" not in payload:
            return web.Response(status=202)
        if method == "initialize":
            result = {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "test", "version": "1"},
            }
        elif method == "tools/list":
            result = {"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]}
        else:
            stream = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await stream.prepare(request)
            await stream.write(
                b'data: {"jsonrpc":"2.0","id":"roots","method":"roots/list"}\n\n'
            )
            await asyncio.wait_for(roots_answered.wait(), 2)
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"content": [{"type": "text", "text": "done"}]},
            }
            await stream.write(f"data: {json.dumps(response)}\n\n".encode())
            await stream.write_eof()
            return stream
        return web.json_response(
            {"jsonrpc": "2.0", "id": payload["id"], "result": result},
            headers={"Mcp-Session-Id": "session"},
        )

    async with endpoint(handler) as url:
        runtime = MCPRuntime(auth)
        config = models.HTTPServer(
            url=url, credential="test", roots=[models.Root(uri="file:///tmp/project")]
        )
        try:
            assert (await runtime.discover("test", config)).tools
            result = await runtime.call(config, "echo", {})
            assert not result.isError
            assert roots_answered.is_set()
        finally:
            await runtime.close()
    assert deleted.is_set()
    assert methods.count("initialize") == 1


async def test_http_failure_does_not_expose_response_body() -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(status=401, text="secret-token")

    async with endpoint(handler) as url:
        runtime = MCPRuntime()
        try:
            with pytest.raises(MCPError) as error:
                await runtime.discover("test", models.HTTPServer(url=url))
            assert "secret-token" not in str(error.value)
        finally:
            await runtime.close()
