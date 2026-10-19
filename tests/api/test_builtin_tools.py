import asyncio
import base64
from collections.abc import AsyncIterator
import json
from pathlib import Path
import sys
from unittest.mock import patch

from aiohttp import web
from aiohttp.test_utils import TestServer
import connect
import pydantic
import pytest

from tests.fixtures import replay, sources
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import composition, tools
from vectorops.runner.api.llm import plan
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.processes.manager import ProcessManager
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.workflow import WorkflowRunner


def payload(response: tools.ToolResponse) -> dict[str, pydantic.JsonValue]:
    block = response.content[0]
    assert isinstance(block, connect.TextBlock)
    return pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(block.text)


@pytest.fixture
async def context(tmp_path: Path) -> AsyncIterator[ActivityExecutionContext]:
    target = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        yield ActivityExecutionContext(
            execution_id="execution",
            workflow_id="workflow",
            step_id="step",
            environment=target,
        )
    finally:
        await target.processes.close()


@pytest.mark.parametrize("data", [b"hello\n", b"\xff\x00\x80", b"a\x00b", b""])
async def test_read_bytes(
    context: ActivityExecutionContext, tmp_path: Path, data: bytes
) -> None:
    (tmp_path / "sample").write_bytes(data)
    result = payload(await tools.read_file_tool().execute(context, {"path": "sample"}))
    content = result["content"]
    assert isinstance(content, str)
    restored = (
        base64.b64decode(content)
        if result["encoding"] == "base64"
        else content.encode()
    )
    assert restored == data
    assert result["eof"] is True


async def test_image_ranges_and_lines(
    context: ActivityExecutionContext, tmp_path: Path
) -> None:
    data = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
    )
    (tmp_path / "image.png").write_bytes(data)
    binding = tools.read_file_tool()
    result = await binding.execute(context, {"path": "image.png"})
    image = result.content[1]
    assert isinstance(image, connect.ImageBlock)
    assert base64.b64decode(image.data) == data
    partial = await binding.execute(context, {"path": "image.png", "limit": 4})
    assert len(partial.content) == 1
    assert payload(partial)["truncated"] is True
    assert payload(partial)["next_offset"] == 4
    tail = payload(await binding.execute(context, {"path": "image.png", "offset": 4}))
    assert tail["encoding"] == "base64"
    (tmp_path / "text").write_text("first\nsecond\nthird\n")
    lines = payload(
        await binding.execute(
            context, {"path": "text", "mode": "lines", "start_line": 2, "max_lines": 1}
        )
    )
    assert lines["text"] == "second\n"
    with pytest.raises(tools.ToolExecutionError):
        binding.validate({"path": "text", "mode": "lines", "offset": 2})
    with pytest.raises(Exception, match="UTF-8"):
        await binding.execute(context, {"path": "image.png", "mode": "text"})


async def test_missing_environment_and_path_errors(
    context: ActivityExecutionContext,
) -> None:
    empty = ActivityExecutionContext(execution_id=None, workflow_id="w", step_id="s")
    with pytest.raises(tools.ToolExecutionError, match="target environment"):
        await tools.read_file_tool().execute(empty, {"path": "missing"})
    with pytest.raises(Exception):
        await tools.read_file_tool().execute(context, {"path": "../outside"})


async def test_process_tools(context: ActivityExecutionContext) -> None:
    result = payload(
        await tools.exec_tool().execute(
            context,
            {
                "command": {
                    "kind": "argv",
                    "argv": [
                        sys.executable,
                        "-u",
                        str(sources.path("process.py")),
                        "stdin",
                    ],
                },
                "keep_stdin_open": True,
                "yield_timeout_ms": 100,
            },
        )
    )
    process_id = result["process_id"]
    assert isinstance(process_id, str)
    listing = await tools.list_processes_tool().execute(context, {})
    assert process_id in str(listing.content)
    await tools.write_stdin_tool().execute(
        context, {"process_id": process_id, "data": "hello", "eof": True}
    )
    output = ""
    cursor = result["cursor"]
    async with asyncio.timeout(5):
        while True:
            result = payload(
                await tools.poll_process_tool().execute(
                    context,
                    {
                        "process_id": process_id,
                        "cursor": cursor,
                        "wait_timeout_ms": 100,
                    },
                )
            )
            output += str(result["chunks"])
            cursor = result["cursor"]
            if result["output_complete"]:
                break
    assert "hello" in output
    failed = await tools.exec_tool().execute(
        context,
        {
            "command": {
                "kind": "argv",
                "argv": [sys.executable, str(sources.path("process.py")), "exit", "3"],
            },
            "yield_timeout_ms": 1000,
        },
    )
    assert failed.is_error
    running = payload(
        await tools.exec_tool().execute(
            context,
            {
                "command": {
                    "kind": "argv",
                    "argv": [
                        sys.executable,
                        str(sources.path("process.py")),
                        "sleep",
                        "30",
                    ],
                },
                "yield_timeout_ms": 0,
            },
        )
    )
    killed = payload(
        await tools.kill_process_tool().execute(
            context, {"process_id": running["process_id"]}
        )
    )
    assert killed["termination_confirmed"] is True


@pytest.mark.parametrize(
    "offset,limit,expected,eof",
    [(0, 2, "ab", False), (2, 2, "cd", True), (4, 2, "", True), (20, 2, "", True)],
)
async def test_file_byte_ranges(
    context: ActivityExecutionContext,
    tmp_path: Path,
    offset: int,
    limit: int,
    expected: str,
    eof: bool,
) -> None:
    (tmp_path / "file").write_text("abcd")
    result = payload(
        await tools.read_file_tool().execute(
            context, {"path": "file", "offset": offset, "limit": limit}
        )
    )
    assert result["content"] == expected
    assert result["eof"] is eof


async def test_local_listing(context: ActivityExecutionContext, tmp_path: Path) -> None:
    (tmp_path / "file").write_bytes(b"data")
    (tmp_path / "directory").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "file")
    listing = tools.list_files_tool()
    partial = payload(await listing.execute(context, {"limit": 2}))
    assert partial["truncated"] is True
    result = payload(await listing.execute(context, {"limit": 3}))
    assert result["truncated"] is False
    assert {(entry["path"], entry["kind"]) for entry in result["entries"]} == {
        ("file", "file"),
        ("directory", "directory"),
        ("link", "symlink"),
    }


@pytest.mark.parametrize("mode", ["auto", "lines"])
async def test_file_read_limit(context: ActivityExecutionContext, mode: str) -> None:
    assert context.environment is not None
    with pytest.raises(EnvironmentError) as error:
        await tools.read_file_tool().execute(
            context,
            {
                "path": "file",
                "mode": mode,
                "limit": context.environment.config.max_read_bytes + 1,
            },
        )
    assert error.value.code == ErrorCode.RESOURCE_LIMIT


async def test_plan_reconstruction_and_atomic_validation(
    history: MemoryHistory,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    snapshots: list[list[plan.Todo]] = []

    async def workflow() -> pydantic.JsonValue:
        binding = tools.update_plan_tool()
        updates: list[dict[str, pydantic.JsonValue]] = [
            {"todos": [{"id": "a", "title": "First", "status": "in_progress"}]},
            {"todos": [{"id": "b", "title": "Second", "status": "in_progress"}]},
            {
                "todos": [
                    {"id": "a", "status": "completed"},
                    {"id": "b", "title": "Second", "status": "pending"},
                ]
            },
        ]
        for index, arguments in enumerate(updates):
            result = await tools.invoke_tool(
                binding,
                connect.ToolCallBlock(
                    id=str(index), name="update_plan", arguments=arguments
                ),
            )
            assert result.is_error is (index == 1)
        snapshots.append(binding.todos)
        return [todo.model_dump(mode="json") for todo in binding.todos]

    first = await replay.run(
        history, workflow, tool_policy_version=1, workspace=tmp_path
    )
    assert first.outcome.kind == "completed"
    before = list(history.appended)
    second = await replay.run(
        history, workflow, first.id, tool_policy_version=1, workspace=tmp_path
    )
    assert second.outcome == first.outcome
    assert snapshots[0] == snapshots[1]
    assert [todo.id for todo in snapshots[1]] == ["a", "b"]
    assert history.appended == before


async def test_plan_replacement_and_isolation(history: MemoryHistory) -> None:
    async def workflow() -> pydantic.JsonValue:
        binding = tools.update_plan_tool()
        ctx = composition.workflow_context()
        await binding.execute_workflow(
            ctx, {"todos": [{"id": "a", "title": "A", "status": "pending"}]}
        )
        invalid: list[dict[str, pydantic.JsonValue]] = [
            {"todos": [{"id": "new", "status": "pending"}]},
            {"merge": False, "todos": [{"id": "a", "status": "pending"}]},
            {
                "todos": [
                    {"id": "a", "status": "pending"},
                    {"id": "a", "status": "completed"},
                ]
            },
        ]
        for arguments in invalid:
            with pytest.raises(tools.ToolExecutionError):
                await binding.execute_workflow(ctx, arguments)
        await binding.execute_workflow(
            ctx,
            {
                "merge": False,
                "todos": [{"id": "b", "title": "B", "status": "completed"}],
            },
        )
        assert [todo.id for todo in binding.todos] == ["b"]
        assert tools.update_plan_tool().todos == []
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"


@pytest.fixture
async def server() -> AsyncIterator[TestServer]:
    async def handle(request: web.Request) -> web.Response:
        if request.path == "/html":
            return web.Response(
                text="<h1>Title</h1><script>secret</script><p>Hello &amp; bye</p>",
                content_type="text/html",
            )
        if request.path == "/large":
            return web.Response(body=b"x" * 100, content_type="text/plain")
        if request.path == "/binary":
            return web.Response(
                body=b"\xff\x00", content_type="application/octet-stream"
            )
        if request.path == "/redirect":
            return web.Response(status=302, headers={"Location": "/html"})
        if request.path == "/error":
            return web.Response(status=404, text="missing")
        if request.path == "/slow":
            await asyncio.sleep(0.1)
            return web.Response(text="late")
        return web.Response(body=await request.read(), content_type="text/plain")

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handle)
    instance = TestServer(app)
    async with instance:
        yield instance


async def test_web_fetch(server: TestServer, context: ActivityExecutionContext) -> None:
    binding = tools.web_fetch_tool()
    html = payload(
        await binding.execute(context, {"url": str(server.make_url("/html"))})
    )
    assert html["content"] == "# Title\n\nHello & bye"
    large = payload(
        await binding.execute(
            context, {"url": str(server.make_url("/large")), "max_bytes": 10}
        )
    )
    assert large["content"] == "x" * 10
    assert large["truncated"] is True
    binary = payload(
        await binding.execute(context, {"url": str(server.make_url("/binary"))})
    )
    assert binary["content"] == base64.b64encode(b"\xff\x00").decode()
    redirect = payload(
        await binding.execute(context, {"url": str(server.make_url("/redirect"))})
    )
    assert redirect["status"] == 200
    assert redirect["url"] == str(server.make_url("/html"))
    assert redirect["content"] == html["content"]
    error = await binding.execute(context, {"url": str(server.make_url("/error"))})
    assert error.is_error
    posted = payload(
        await binding.execute(
            context,
            {
                "url": str(server.make_url("/echo")),
                "method": "POST",
                "body": {"kind": "text", "text": "hello"},
            },
        )
    )
    assert posted["content"] == "hello"


@pytest.mark.parametrize(
    "arguments",
    [
        {"url": "file:///tmp/file"},
        {"url": "http://user:secret@example.com"},
        {"url": "http://example.com", "body": {"kind": "text", "text": "bad"}},
        {
            "url": "http://example.com",
            "method": "POST",
            "body": {"kind": "binary", "data_base64": "!"},
        },
        {"url": "http://example.com", "max_bytes": 1048577},
    ],
)
def test_web_validation(arguments: dict[str, pydantic.JsonValue]) -> None:
    with pytest.raises(tools.ToolExecutionError):
        tools.web_fetch_tool().validate(arguments)


async def test_web_replays_without_network(
    server: TestServer, history: MemoryHistory
) -> None:
    async def workflow() -> pydantic.JsonValue:
        result = await tools.invoke_tool(
            tools.web_fetch_tool(),
            connect.ToolCallBlock(
                id="fetch",
                name="web_fetch",
                arguments={"url": str(server.make_url("/html"))},
            ),
        )
        return result.model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    with patch(
        "aiohttp.ClientSession", side_effect=AssertionError("network during replay")
    ):
        second = await replay.run(history, workflow, first.id)
    assert second.outcome == first.outcome


async def test_web_timeout_and_cancellation(
    server: TestServer, context: ActivityExecutionContext
) -> None:
    binding = tools.web_fetch_tool()
    with pytest.raises(TimeoutError):
        await binding.execute(
            context, {"url": str(server.make_url("/slow")), "timeout_s": 0.01}
        )
    task = asyncio.create_task(
        binding.execute(context, {"url": str(server.make_url("/slow"))})
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_binary_process_output(context: ActivityExecutionContext) -> None:
    response = await tools.exec_tool().execute(
        context,
        {
            "command": {
                "kind": "argv",
                "argv": [
                    sys.executable,
                    str(sources.path("process.py")),
                    "binary_output",
                ],
            },
            "yield_timeout_ms": 1000,
        },
    )
    assert not response.is_error
    chunks = payload(response)["chunks"]
    assert isinstance(chunks, list) and chunks
    chunk = chunks[0]
    assert isinstance(chunk, dict)
    assert chunk["encoding"] == "base64"
    assert chunk["data"] == "/wA="


async def test_environment_tools_replay_without_io(
    workspace: Path, history: MemoryHistory
) -> None:
    (workspace / "sample.bin").write_bytes(b"\xff\x00")
    (workspace / "config.toml").write_text(
        f"[environment]\nkind = 'local'\nbase_project_path = {json.dumps(str(workspace))}\n"
        "[components.tools.approval]\napprove_all = true\n"
    )
    (workspace / "workflows/tool_replay.py").write_text(sources.read("tool_replay.py"))
    runner = WorkflowRunner(workspace, history)
    first = await runner.run("tool_replay:main", workflow_id="root")
    assert isinstance(first, list)
    assert all(
        not connect.ToolResultMessage.model_validate(result).is_error
        for result in first
    )
    (workspace / "sample.bin").unlink()
    with (
        patch(
            "vectorops.runner.api.builtin_tools.read_file.paths.open_regular",
            side_effect=AssertionError("file I/O during replay"),
        ),
        patch.object(
            ProcessManager, "exec", side_effect=AssertionError("exec during replay")
        ),
    ):
        second = await runner.run(
            "tool_replay:main", continue_from_step_id=history.heads["root"]
        )
    assert second == first
