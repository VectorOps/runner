import asyncio
from pathlib import Path
from typing import Literal

import connect
from mcp import types
import pydantic
import pytest

from tests.mcp.test_replay import configure
from tests.mcp.test_runtime import server
from vectorops.runner.api import mcp
from vectorops.runner.mcp import content, content_binding, models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_stdio_content_capabilities_and_conversion() -> None:
    runtime = MCPRuntime()
    config = server()
    try:
        catalog = await runtime.discover_content("test", config)
        assert [item.name for item in catalog.prompts] == ["greet"]
        assert [item.uri for item in catalog.resources] == ["file:///picture.png"]
        selected = mcp.Content(catalog)
        assert len(selected.tools()) == 2
        catalog.prompts.clear()
        assert selected.catalog.prompts
        prompt = await runtime.retrieve_content(
            config, "prompt", "greet", {"who": "world"}
        )
        assert isinstance(prompt, content.PromptResult)
        assert [item.role for item in prompt.messages] == ["user", "assistant"]
        resource = await runtime.retrieve_content(
            config, "resource", "file:///picture.png", {}
        )
        assert isinstance(resource, content.ResourceResult)
        assert isinstance(resource.contents[0].content[0], connect.ImageBlock)
    finally:
        await runtime.close()


async def test_missing_capability_is_not_called() -> None:
    runtime = MCPRuntime()
    try:
        config = server("tools_only")
        catalog = await runtime.discover_content("test", config)
        assert not catalog.prompts and not catalog.resources
        with pytest.raises(MCPError, match="capability"):
            await runtime.retrieve_content(config, "prompt", "greet", {})
    finally:
        await runtime.close()


async def test_content_pagination_and_size_are_bounded() -> None:
    runtime = MCPRuntime()
    try:
        with pytest.raises(MCPError, match="cursor"):
            await runtime.discover_content("test", server("content_cursor"))
        with pytest.raises(MCPError):
            await runtime.retrieve_content(
                server("large_content"), "prompt", "greet", {"who": "world"}
            )
    finally:
        await runtime.close()


def test_content_fallbacks_and_roles_preserve_data_without_fetching_links() -> None:
    prompt = content.prompt_result(
        types.GetPromptResult(
            messages=[
                types.PromptMessage(
                    role="assistant",
                    content=types.ImageContent(
                        type="image", mimeType="image/png", data="YQ=="
                    ),
                ),
                types.PromptMessage(
                    role="user",
                    content=types.ResourceLink(
                        type="resource_link",
                        name="remote",
                        uri=pydantic.AnyUrl("https://example.test/private"),
                    ),
                ),
            ]
        )
    )
    assert prompt.messages[0].role == "assistant"
    assert isinstance(prompt.messages[0].content[0], connect.ImageBlock)
    link = prompt.messages[1].content[0]
    assert (
        isinstance(link, connect.TextBlock)
        and "https://example.test/private" in link.text
    )
    binary = content.resource_result(
        types.ReadResourceResult(
            contents=[
                types.BlobResourceContents(
                    uri=pydantic.AnyUrl("file:///binary"),
                    mimeType="application/octet-stream",
                    blob="YQ==",
                ),
                types.TextResourceContents(
                    uri=pydantic.AnyUrl("file:///text"), text="hello"
                ),
            ]
        )
    )
    assert binary.contents[0].mime_type == "application/octet-stream"
    assert isinstance(binary.contents[0].content[0], connect.TextBlock)
    assert "YQ==" in binary.contents[0].content[0].text
    with pytest.raises(MCPError, match="base64"):
        content.resource_result(
            types.ReadResourceResult(
                contents=[
                    types.BlobResourceContents(
                        uri=pydantic.AnyUrl("file:///bad"), blob="?"
                    )
                ]
            )
        )


@pytest.mark.parametrize("kind", ["prompt", "resource"])
async def test_binding_rejects_explicit_other_source_and_unselected_identity(
    kind: Literal["prompt", "resource"],
) -> None:
    catalog = content.ContentCatalog(
        server_name="allowed",
        server=server(),
        prompts=[
            content.Prompt(
                name="greet",
                arguments=[content.PromptArgument(name="who", required=True)],
            )
        ],
        resources=[content.Resource(uri="file:///picture.png", name="picture")],
    )
    binding = content_binding.ContentTool(catalog, kind)
    identity = "greet" if kind == "prompt" else "file:///picture.png"
    with pytest.raises(MCPError, match="source"):
        await binding.execute(
            ActivityExecutionContext(None, "workflow", "step"),
            {"source": "forbidden", "identity": identity},
        )
    with pytest.raises(MCPError, match="selection"):
        binding.validate({"source": "allowed", "identity": "other"})
    if kind == "prompt":
        with pytest.raises(MCPError, match="arguments"):
            binding.validate({"source": "allowed", "identity": identity})
    empty = mcp.Content(content.ContentCatalog(server_name="allowed", server=server()))
    assert empty.tools() == []


async def test_sqlite_content_replays_offline(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure(workspace)
    runner = WorkflowRunner(workspace)
    result = await runner.run("python:tests.fixtures.mcp_workflows:content")
    assert isinstance(result, dict)
    prompt = content.PromptResult.model_validate(result["prompt"])
    assert [item.role for item in prompt.messages] == ["user", "assistant"]
    identifier = runner.last_execution_id
    assert identifier is not None

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("replay contacted MCP")

    monkeypatch.setattr(MCPRuntime, "discover_content", forbidden)
    monkeypatch.setattr(MCPRuntime, "retrieve_content", forbidden)
    (workspace / "config.toml").write_text("")
    assert await WorkflowRunner(workspace).resume(identifier) == result


@pytest.mark.parametrize("approved", [True, False])
@pytest.mark.parametrize("kind", ["prompt", "resource"])
async def test_content_tools_use_approval_and_replay(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    approved: bool,
    kind: str,
) -> None:
    configure(workspace, approve=False)
    calls: list[str] = []
    original = MCPRuntime.retrieve_content

    async def tracked(
        self: MCPRuntime,
        server: models.ServerConfig,
        kind: Literal["prompt", "resource"],
        identity: str,
        arguments: dict[str, str],
    ) -> content.PromptResult | content.ResourceResult:
        calls.append(identity)
        return await original(self, server, kind, identity, arguments)

    monkeypatch.setattr(MCPRuntime, "retrieve_content", tracked)
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run(
            "python:tests.fixtures.mcp_workflows:content_tool", input={"kind": kind}
        )
    )
    try:
        async with asyncio.timeout(5):
            while True:
                identifier = runner.last_execution_id
                if identifier is not None:
                    page = await runner.pending_inputs(
                        mailbox.RequestQuery(execution_id=identifier)
                    )
                    if page.requests:
                        break
                await asyncio.sleep(0.01)
        assert calls == []
        request = page.requests[0]
        await runner.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=request.request_id,
                response={"approved": approved},
                idempotency_key="answer",
            )
        )
        result = await task
        assert connect.ToolResultMessage.model_validate(result).is_error is not approved
        assert len(calls) == int(approved)
        assert await runner.resume(identifier) == result
        assert len(calls) == int(approved)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_disallowed_source_fails_before_approval(workspace: Path) -> None:
    configure(workspace, approve=False)
    runner = WorkflowRunner(workspace)
    result = await runner.run(
        "python:tests.fixtures.mcp_workflows:content_tool",
        input={"source": "forbidden"},
    )
    assert connect.ToolResultMessage.model_validate(result).is_error
    assert runner.last_execution_id is not None
    assert not (
        await runner.pending_inputs(
            mailbox.RequestQuery(execution_id=runner.last_execution_id)
        )
    ).requests


@pytest.mark.parametrize("kind", ["prompt", "resource"])
async def test_descriptor_changes_fail_before_retrieval(
    kind: Literal["prompt", "resource"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = content.ContentCatalog(
        server_name="test",
        server=server(),
        prompts=[content.Prompt(name="greet", description="original")],
        resources=[content.Resource(uri="file:///picture.png", name="original")],
    )
    changed = catalog.model_copy(
        update={
            "prompts": [content.Prompt(name="greet", description="changed")],
            "resources": [content.Resource(uri="file:///picture.png", name="changed")],
        }
    )

    async def discovered(
        self: MCPRuntime, name: str, server: models.ServerConfig
    ) -> content.ContentCatalog:
        return changed

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("changed descriptor was fetched")

    monkeypatch.setattr(MCPRuntime, "discover_content", discovered)
    monkeypatch.setattr(MCPRuntime, "retrieve_content", forbidden)
    runtime = MCPRuntime()
    try:
        with pytest.raises(MCPError, match="changed since discovery"):
            await content_binding.retrieve(
                ActivityExecutionContext(None, "workflow", "step", mcp=runtime),
                catalog,
                kind,
                "greet" if kind == "prompt" else "file:///picture.png",
                {},
            )
    finally:
        await runtime.close()


def test_content_tool_identity_binds_server_configuration_and_selection() -> None:
    catalog = content.ContentCatalog(
        server_name="same", server=server(), prompts=[content.Prompt(name="first")]
    )
    original = content_binding.ContentTool(catalog, "prompt").definition.name
    other_server = catalog.model_copy(
        update={"server": models.HTTPServer(url="https://other.test/mcp")}
    )
    other_selection = catalog.model_copy(
        update={"prompts": [content.Prompt(name="second")]}
    )
    assert (
        content_binding.ContentTool(other_server, "prompt").definition.name != original
    )
    assert (
        content_binding.ContentTool(other_selection, "prompt").definition.name
        != original
    )
