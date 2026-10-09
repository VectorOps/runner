import asyncio
from typing import Literal

from aiohttp import web
import pytest

from tests.mcp.test_http import endpoint
from vectorops.runner.mcp import content, models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError


@pytest.mark.parametrize(
    "failure", ["none", "duplicate", "cursor", "pages", "timeout", "error"]
)
async def test_http_content_pagination_retrieval_and_failures(failure: str) -> None:
    methods: list[str] = []

    async def handler(request: web.Request) -> web.Response:
        if request.method == "GET":
            return web.Response(status=405)
        if request.method == "DELETE":
            return web.Response(status=200)
        payload = await request.json()
        if "id" not in payload:
            return web.Response(status=202)
        method = payload["method"]
        methods.append(method)
        if method == "initialize":
            result = {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {"prompts": {}, "resources": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
        elif method == "prompts/list":
            cursor = payload.get("params", {}).get("cursor")
            name = "first" if not cursor or failure == "duplicate" else "second"
            result = {"prompts": [{"name": name}]}
            if not cursor or failure in ("cursor", "pages"):
                result["nextCursor"] = "next"
            if failure == "cursor":
                result["prompts"] = []
        elif method == "resources/list":
            result = {"resources": [{"name": "text", "uri": "custom:///text"}]}
        elif method == "prompts/get":
            if failure == "timeout":
                await asyncio.sleep(0.3)
            if failure == "error":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "error": {"code": -32603, "message": "secret-detail"},
                    }
                )
            result = {
                "messages": [
                    {"role": "user", "content": {"type": "text", "text": "prompt"}}
                ]
            }
        else:
            assert method == "resources/read"
            result = {
                "contents": [
                    {
                        "uri": payload["params"]["uri"],
                        "mimeType": "text/plain",
                        "text": "resource",
                    }
                ]
            }
        return web.json_response(
            {"jsonrpc": "2.0", "id": payload["id"], "result": result},
            headers={"Mcp-Session-Id": "session"},
        )

    async with endpoint(handler) as url:
        runtime = MCPRuntime()
        config = models.HTTPServer(
            url=url,
            limits=models.Limits(
                request_seconds=0.15 if failure == "timeout" else 2,
                max_pages=1 if failure == "pages" else 4,
            ),
        )
        try:
            if failure in ("duplicate", "cursor", "pages"):
                with pytest.raises(MCPError):
                    await runtime.discover_content("test", config)
                return
            catalog = await runtime.discover_content("test", config)
            assert [item.name for item in catalog.prompts] == ["first", "second"]
            if failure in ("timeout", "error"):
                with pytest.raises(MCPError) as error:
                    await runtime.retrieve_content(config, "prompt", "first", {})
                assert "secret-detail" not in str(error.value)
                return
            prompt = await runtime.retrieve_content(config, "prompt", "first", {})
            assert isinstance(prompt, content.PromptResult)
            resource = await runtime.retrieve_content(
                config, "resource", "custom:///text", {}
            )
            assert isinstance(resource, content.ResourceResult)
            assert resource.contents[0].mime_type == "text/plain"
            assert "tools/list" not in methods
        finally:
            await runtime.close()


@pytest.mark.parametrize("kind", ["prompt", "resource"])
async def test_content_cancellation_propagates(
    kind: Literal["prompt", "resource"],
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: web.Request) -> web.Response:
        if request.method != "POST":
            return web.Response(status=405)
        payload = await request.json()
        if "id" not in payload:
            return web.Response(status=202)
        if payload["method"] == "initialize":
            result = {
                "protocolVersion": payload["params"]["protocolVersion"],
                "capabilities": {"prompts": {}, "resources": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
            return web.json_response(
                {"jsonrpc": "2.0", "id": payload["id"], "result": result}
            )
        started.set()
        await release.wait()
        return web.Response(status=500)

    async with endpoint(handler) as url:
        runtime = MCPRuntime()
        task = asyncio.create_task(
            runtime.retrieve_content(
                models.HTTPServer(url=url), kind, "custom:///item", {}
            )
        )
        try:
            await asyncio.wait_for(started.wait(), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runtime.close()
