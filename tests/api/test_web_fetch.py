import asyncio
import base64
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
import gzip
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import connect
import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import tools
from vectorops.runner.api.builtin_tools import web_fetch
from vectorops.runner.api.builtin_tools.web_content import convert_html
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.fixture
def context() -> ActivityExecutionContext:
    return ActivityExecutionContext(
        execution_id=None, workflow_id="web", step_id="fetch"
    )


def payload(result: tools.ToolResponse) -> dict[str, pydantic.JsonValue]:
    block = result.content[0]
    assert isinstance(block, connect.TextBlock)
    return pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(block.text)


@asynccontextmanager
async def serve(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[TestServer]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    server = TestServer(app)
    async with server:
        yield server


async def test_conversion_precedes_output_limit(
    context: ActivityExecutionContext,
) -> None:
    html = (
        "<head><script>" + "x" * 1100000 + "</script></head>"
        "<nav>Navigation</nav><main><h1>Article</h1><p>Useful content.</p></main>"
    )

    async def handle(request: web.Request) -> web.Response:
        return web.Response(text=html, content_type="text/html")

    async with serve(handle) as server:
        result = payload(
            await tools.web_fetch_tool().execute(
                context, {"url": str(server.make_url("/")), "max_bytes": 40}
            )
        )
    assert result["content"] == "# Article\n\nUseful content."
    assert result["bytes_read"] == len(html.encode())
    assert result["output_bytes"] == result["total_bytes"] == 26
    assert result["content_format"] == "markdown"
    assert result["truncated"] is False
    assert result["eof"] is True


def test_markdown_structure_and_selection() -> None:
    html = (
        '<head><base href="https://example.com/docs/"><title>Site</title><style>noise</style></head>'
        "<nav>Navigation</nav><main><h1>Title</h1><h5>Details</h5>"
        '<p>See <a href="guide">guide</a> and <code>inline_code</code>.</p>'
        '<pre><code class="language-python">if ready:\n    run()\n    finish()\n</code></pre>'
        "<ol><li>First</li><li>Second</li></ol>"
        "<table><tr><th>Name</th><th>Value</th></tr><tr><td>timeout</td><td>30</td></tr></table>"
        '<img src="diagram.png" alt="A connects to B">'
        '<p hidden>Hidden</p><p aria-hidden="true">Duplicate</p>'
        "<script>secret</script><aside>Sidebar</aside></main><footer>Footer</footer>"
    )
    result = convert_html(html, "https://example.com/page", "markdown", True)
    for expected in (
        "# Title",
        "##### Details",
        "[guide](https://example.com/docs/guide)",
        "`inline_code`",
        "```python\nif ready:\n    run()\n    finish()\n```",
        "1. First\n2. Second",
        "| Name | Value |",
        "| timeout | 30 |",
        "![A connects to B](https://example.com/docs/diagram.png)",
    ):
        assert expected in result
    for omitted in (
        "Navigation",
        "Site",
        "noise",
        "Hidden",
        "Duplicate",
        "secret",
        "Sidebar",
        "Footer",
    ):
        assert omitted not in result


@pytest.mark.parametrize("container", ["article", 'div role="main"', "section"])
def test_selection_fallback(container: str) -> None:
    tag = container.split()[0]
    assert (
        convert_html(
            f"<{container}><p>Body &amp; text</p></{tag}>",
            "https://example.com",
            "markdown",
            True,
        )
        == "Body & text"
    )


async def test_output_modes(context: ActivityExecutionContext) -> None:
    html = "<nav>Navigation</nav><main><h1>Title</h1><p>Text</p></main>"

    async def handle(request: web.Request) -> web.Response:
        return web.Response(text=html, content_type="text/html")

    async with serve(handle) as server:
        url = str(server.make_url("/"))
        tool = tools.web_fetch_tool()
        raw = payload(await tool.execute(context, {"url": url, "format": "raw"}))
        text = payload(await tool.execute(context, {"url": url, "format": "text"}))
        full = payload(
            await tool.execute(context, {"url": url, "main_content_only": False})
        )
    assert raw["content"] == html
    assert text["content"] == "Title\nText"
    assert "Navigation" in str(full["content"])
    assert "# Title" in str(full["content"])


@pytest.mark.parametrize("limit", [4, 5, 10, 64])
async def test_utf8_ranges_reconstruct_document(
    context: ActivityExecutionContext, limit: int
) -> None:
    text = "# Heading\n\nCafé 中文 🙂 end."

    async def handle(request: web.Request) -> web.Response:
        return web.Response(text=text, content_type="text/markdown")

    offset = 0
    pieces: list[str] = []
    expected_hash = hashlib.sha256(text.encode()).hexdigest()
    async with serve(handle) as server:
        for _ in range(len(text.encode()) + 1):
            result = payload(
                await tools.web_fetch_tool().execute(
                    context,
                    {
                        "url": str(server.make_url("/")),
                        "max_bytes": limit,
                        "offset": offset,
                    },
                )
            )
            piece = result["content"]
            assert isinstance(piece, str)
            assert result["content_sha256"] == expected_hash
            assert result["total_bytes"] == len(text.encode())
            assert result["offset"] == offset
            assert result["output_bytes"] == len(piece.encode()) <= limit
            pieces.append(piece)
            next_offset = result["next_offset"]
            assert isinstance(next_offset, int)
            assert next_offset == offset + len(piece.encode())
            if result["eof"]:
                assert result["truncated"] is False
                break
            assert next_offset > offset
            offset = next_offset
        else:
            pytest.fail("range continuation did not terminate")
        empty = payload(
            await tools.web_fetch_tool().execute(
                context,
                {"url": str(server.make_url("/")), "offset": len(text.encode()) + 100},
            )
        )
    assert "".join(pieces) == text
    assert empty["content"] == ""
    assert empty["next_offset"] == len(text.encode())
    assert empty["eof"] is True


async def test_ranges_apply_to_markdown(context: ActivityExecutionContext) -> None:
    async def handle(request: web.Request) -> web.Response:
        return web.Response(text="<h1>Title</h1><p>Body</p>", content_type="text/html")

    async with serve(handle) as server:
        result = payload(
            await tools.web_fetch_tool().execute(
                context, {"url": str(server.make_url("/")), "offset": 9, "max_bytes": 4}
            )
        )
    assert result["content"] == "Body"
    assert result["total_bytes"] == 13
    assert result["eof"] is True


@pytest.mark.parametrize(
    "offset,limit,message", [(1, 4, "boundary"), (0, 1, "too small")]
)
async def test_invalid_utf8_ranges(
    context: ActivityExecutionContext, offset: int, limit: int, message: str
) -> None:
    async def handle(request: web.Request) -> web.Response:
        return web.Response(text="🙂")

    async with serve(handle) as server:
        with pytest.raises(tools.ToolExecutionError, match=message):
            await tools.web_fetch_tool().execute(
                context,
                {
                    "url": str(server.make_url("/")),
                    "offset": offset,
                    "max_bytes": limit,
                },
            )


@pytest.mark.parametrize("mime", ["application/octet-stream", "image/png"])
async def test_binary_and_image_ranges(
    context: ActivityExecutionContext, mime: str
) -> None:
    data = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
    )

    async def handle(request: web.Request) -> web.Response:
        return web.Response(body=data, content_type=mime)

    async with serve(handle) as server:
        url = str(server.make_url("/"))
        tool = tools.web_fetch_tool()
        full = await tool.execute(context, {"url": url})
        head = await tool.execute(context, {"url": url, "max_bytes": 4})
        tail = await tool.execute(context, {"url": url, "offset": 4})
    assert len(head.content) == len(tail.content) == 1
    assert (
        base64.b64decode(str(payload(head)["content"]))
        + base64.b64decode(str(payload(tail)["content"]))
        == data
    )
    assert payload(head)["truncated"] is True
    assert payload(tail)["eof"] is True
    if mime == "image/png":
        assert isinstance(full.content[1], connect.ImageBlock)
        assert base64.b64decode(full.content[1].data) == data
    else:
        assert base64.b64decode(str(payload(full)["content"])) == data


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "HEAD"])
async def test_redirect_method_and_body(
    context: ActivityExecutionContext, status: int, method: str
) -> None:
    received: list[tuple[str, bytes, str]] = []

    async def handle(request: web.Request) -> web.Response:
        if request.path == "/start":
            return web.Response(status=status, headers={"Location": "end"})
        received.append(
            (
                request.method,
                await request.read(),
                request.headers.get("Authorization", ""),
            )
        )
        return web.Response(text="destination")

    async with serve(handle) as server:
        arguments: dict[str, pydantic.JsonValue] = {
            "url": str(server.make_url("/start")),
            "method": method,
            "headers": {"Authorization": "test-token"},
        }
        if method != "HEAD":
            arguments["body"] = {"kind": "text", "text": "body"}
        result = payload(await tools.web_fetch_tool().execute(context, arguments))
        assert result["url"] == str(server.make_url("/end"))
    rewritten = (status == 303 and method != "HEAD") or (
        status in (301, 302) and method == "POST"
    )
    assert received == [
        (
            "GET" if rewritten else method,
            b"" if rewritten or method == "HEAD" else b"body",
            "test-token",
        )
    ]
    assert result["status"] == 200
    assert len(str(result["redirects"])) > 2


async def test_redirect_opt_out_and_missing_location(
    context: ActivityExecutionContext,
) -> None:
    paths: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        paths.append(request.path)
        return web.Response(
            status=308,
            text="Redirecting",
            headers={"Location": "/end"} if request.path == "/start" else {},
        )

    async with serve(handle) as server:
        tool = tools.web_fetch_tool()
        stopped = payload(
            await tool.execute(
                context,
                {"url": str(server.make_url("/start")), "follow_redirects": False},
            )
        )
        missing = payload(
            await tool.execute(context, {"url": str(server.make_url("/missing"))})
        )
    assert paths == ["/start", "/missing"]
    assert stopped["status"] == missing["status"] == 308
    assert stopped["location"] == "/end"
    assert stopped["redirects"] == []


@pytest.mark.parametrize("loop", [False, True])
async def test_redirect_limit(context: ActivityExecutionContext, loop: bool) -> None:
    calls = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        number = int(request.path[1:])
        if not loop and number == 10:
            return web.Response(text="done")
        return web.Response(
            status=302, headers={"Location": "/0" if loop else f"/{number + 1}"}
        )

    async with serve(handle) as server:
        if loop:
            with pytest.raises(tools.ToolExecutionError, match="Too many redirects"):
                await tools.web_fetch_tool().execute(
                    context, {"url": str(server.make_url("/0"))}
                )
        else:
            result = payload(
                await tools.web_fetch_tool().execute(
                    context, {"url": str(server.make_url("/0"))}
                )
            )
            assert result["content"] == "done"
    assert calls == 11


async def test_cross_origin_header_filter(context: ActivityExecutionContext) -> None:
    received: dict[str, str] = {}

    async def destination(request: web.Request) -> web.Response:
        received.update(request.headers)
        return web.Response(text="done")

    async with serve(destination) as target:

        async def redirect(request: web.Request) -> web.Response:
            return web.Response(
                status=308,
                headers={
                    "Location": str(target.make_url("/")),
                    "Set-Cookie": "new-secret=value",
                },
            )

        async with serve(redirect) as source:
            await tools.web_fetch_tool().execute(
                context,
                {
                    "url": str(source.make_url("/")),
                    "headers": {
                        "Authorization": "test-secret",
                        "Cookie": "test-secret=value",
                        "X-Api-Key": "test-secret",
                        "Host": "original.example",
                        "Accept": "text/plain",
                        "Accept-Language": "en",
                        "User-Agent": "fetch-test",
                    },
                },
            )
            assert received["Host"] == target.make_url("/").authority
    for name in ("Authorization", "Cookie", "X-Api-Key"):
        assert name not in received
    assert received["Accept"] == "text/plain"
    assert received["Accept-Language"] == "en"
    assert received["User-Agent"] == "fetch-test"


@pytest.mark.parametrize("status", [303, 307, 308])
async def test_cross_origin_body_policy(
    context: ActivityExecutionContext, status: int
) -> None:
    received: list[tuple[str, bytes]] = []

    async def destination(request: web.Request) -> web.Response:
        received.append((request.method, await request.read()))
        return web.Response(text="done")

    async with serve(destination) as target:

        async def redirect(request: web.Request) -> web.Response:
            return web.Response(
                status=status, headers={"Location": str(target.make_url("/"))}
            )

        async with serve(redirect) as source:
            arguments: dict[str, pydantic.JsonValue] = {
                "url": str(source.make_url("/")),
                "method": "POST",
                "body": {"kind": "text", "text": "private body"},
            }
            if status == 303:
                await tools.web_fetch_tool().execute(context, arguments)
                assert received == [("GET", b"")]
            else:
                with pytest.raises(tools.ToolExecutionError, match="Cross-origin"):
                    await tools.web_fetch_tool().execute(context, arguments)
                assert received == []


@pytest.mark.parametrize(
    "location",
    ["file:///tmp/file", "http://user:secret@example.com", "http://example.com:bad"],
)
async def test_invalid_redirect_destination(
    context: ActivityExecutionContext, location: str
) -> None:
    async def handle(request: web.Request) -> web.Response:
        return web.Response(status=302, headers={"Location": location})

    async with serve(handle) as server:
        with pytest.raises(tools.ToolExecutionError, match="Invalid redirect URL"):
            await tools.web_fetch_tool().execute(
                context, {"url": str(server.make_url("/"))}
            )


async def test_https_downgrade_rejected(context: ActivityExecutionContext) -> None:
    response = MagicMock(spec=aiohttp.ClientResponse)
    response.status = 302
    response.url = "https://example.com/"
    response.headers = {"Location": "http://example.com/"}
    manager = AsyncMock()
    manager.__aenter__.return_value = response
    with patch.object(
        aiohttp.ClientSession, "request", return_value=manager
    ) as request:
        with pytest.raises(tools.ToolExecutionError, match="HTTPS to HTTP"):
            await tools.web_fetch_tool().execute(
                context, {"url": "https://example.com/"}
            )
    request.assert_called_once()


async def test_timeout_covers_entire_redirect_chain(
    context: ActivityExecutionContext,
) -> None:
    async def handle(request: web.Request) -> web.Response:
        await asyncio.sleep(0.04)
        return web.Response(status=302, headers={"Location": "/next"})

    async with serve(handle) as server:
        with pytest.raises(TimeoutError):
            await tools.web_fetch_tool().execute(
                context, {"url": str(server.make_url("/")), "timeout_s": 0.07}
            )


@pytest.mark.parametrize("size", [1024, 1025])
@pytest.mark.parametrize("compressed", [False, True])
async def test_download_cap(
    context: ActivityExecutionContext,
    monkeypatch: pytest.MonkeyPatch,
    size: int,
    compressed: bool,
) -> None:
    monkeypatch.setattr(web_fetch, "MAX_DOWNLOAD_BYTES", 1024)

    async def handle(request: web.Request) -> web.Response:
        data = b"x" * size
        return web.Response(
            body=gzip.compress(data) if compressed else data,
            headers={"Content-Encoding": "gzip"} if compressed else {},
            content_type="text/plain",
        )

    async with serve(handle) as server:
        arguments: dict[str, pydantic.JsonValue] = {
            "url": str(server.make_url("/")),
            "max_bytes": 10,
        }
        if size > 1024:
            with pytest.raises(
                tools.ToolExecutionError, match="decompressed download limit"
            ):
                await tools.web_fetch_tool().execute(context, arguments)
        else:
            result = payload(await tools.web_fetch_tool().execute(context, arguments))
            assert result["content"] == "x" * 10
            assert result["bytes_read"] == 1024
            assert result["truncated"] is True


@pytest.mark.parametrize(
    "arguments",
    [
        {"offset": -1},
        {"format": "pdf"},
        {"follow_redirects": "yes"},
        {"url": "https://example.com:bad"},
    ],
)
def test_argument_validation(arguments: dict[str, pydantic.JsonValue]) -> None:
    with pytest.raises(tools.ToolExecutionError):
        tools.web_fetch_tool().validate({"url": "https://example.com", **arguments})


async def test_changed_document_hash(context: ActivityExecutionContext) -> None:
    count = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal count
        count += 1
        return web.Response(text=f"document {count}")

    async with serve(handle) as server:
        arguments: dict[str, pydantic.JsonValue] = {
            "url": str(server.make_url("/")),
            "max_bytes": 4,
        }
        first = payload(await tools.web_fetch_tool().execute(context, arguments))
        second = payload(
            await tools.web_fetch_tool().execute(
                context, {**arguments, "offset": first["next_offset"]}
            )
        )
    assert first["content_sha256"] != second["content_sha256"]


def test_malformed_links_and_decorative_anchors() -> None:
    html = (
        '<head><base href="http://[broken"></head><main><h2>'
        '<a href="#title">\u200b</a>Title</h2>'
        '<p><a href="http://[broken">Broken</a> '
        '<a href="javascript:alert(1)">Unsafe</a> '
        '<a href="/guide">Guide</a></p></main>'
    )
    result = convert_html(html, "https://example.com", "markdown", True)
    assert "## Title" in result
    assert "Broken" in result
    assert "Unsafe" in result
    assert "[Guide](https://example.com/guide)" in result
    assert "\u200b" not in result
    assert "javascript:" not in result


def test_nested_empty_anchors() -> None:
    html = '<main><a href="#a"><a href="#b">\u200b</a></a><p>Body</p></main>'
    assert convert_html(html, "https://example.com", "markdown", True) == "Body"


async def test_non_utf8_response_and_raw_ranges(
    context: ActivityExecutionContext,
) -> None:
    html = "<p>Café</p>"

    async def handle(request: web.Request) -> web.Response:
        return web.Response(
            body=html.encode("iso-8859-1"),
            content_type="text/html",
            charset="iso-8859-1",
        )

    async with serve(handle) as server:
        url = str(server.make_url("/"))
        converted = payload(await tools.web_fetch_tool().execute(context, {"url": url}))
        raw = payload(
            await tools.web_fetch_tool().execute(
                context, {"url": url, "format": "raw", "offset": 3, "max_bytes": 5}
            )
        )
    assert converted["content"] == raw["content"] == "Café"
    assert converted["total_bytes"] == 5
    assert raw["total_bytes"] == len(html.encode())
    assert raw["bytes_read"] == len(html.encode("iso-8859-1"))


async def test_json_and_http_errors(context: ActivityExecutionContext) -> None:
    content = '{"text":"<h1>not HTML</h1>"}'

    async def handle(request: web.Request) -> web.Response:
        return web.Response(
            status=404, text=content, content_type="application/problem+json"
        )

    async with serve(handle) as server:
        result = await tools.web_fetch_tool().execute(
            context, {"url": str(server.make_url("/"))}
        )
    assert result.is_error
    assert payload(result)["content"] == content


async def test_replay_preserves_legacy_result(history: MemoryHistory) -> None:
    class LegacyArguments(pydantic.BaseModel):
        url: str

    async def legacy_execute(
        context: ActivityExecutionContext, arguments: LegacyArguments
    ) -> tools.ToolResponse:
        return tools.ToolResponse(
            content=[
                connect.TextBlock(
                    text=json.dumps(
                        {
                            "url": arguments.url,
                            "status": 308,
                            "bytes_read": 15,
                            "truncated": False,
                            "location": "/page",
                            "encoding": "utf-8",
                            "content": "Redirecting...\n",
                        }
                    )
                )
            ]
        )

    binding: tools.ToolBinding = tools.Tool(
        "web_fetch", "Legacy fetch", LegacyArguments, legacy_execute
    )

    async def workflow() -> pydantic.JsonValue:
        result = await tools.invoke_tool(
            binding,
            connect.ToolCallBlock(
                id="fetch",
                name="web_fetch",
                arguments={"url": "https://example.com/page/"},
            ),
        )
        return result.model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    before = list(history.appended)
    binding = tools.web_fetch_tool()
    with patch(
        "aiohttp.ClientSession", side_effect=AssertionError("network during replay")
    ):
        second = await replay.run(history, workflow, first.id)
    assert second.outcome == first.outcome
    assert history.appended == before
