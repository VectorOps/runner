"""MCP workflows for persistent discovery and tool replay tests."""

import connect
import pydantic

from vectorops.runner.api import composition, mcp, tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@composition.activity(name="mcp_runtime_identity")
async def runtime_identity(ctx: ActivityExecutionContext) -> int:
    assert ctx.mcp is not None
    return id(ctx.mcp)


async def child(ctx: WorkflowExecutionContext) -> int:
    return await runtime_identity()


async def parent(ctx: WorkflowExecutionContext) -> bool:
    identity = await runtime_identity()
    handle = await ctx.spawn_workflow("python:tests.fixtures.mcp_workflows:child")
    outcomes = await ctx.join_workflows([handle])
    assert outcomes[0].kind == "completed"
    return outcomes[0].result == identity


async def echo(
    ctx: WorkflowExecutionContext, value: pydantic.JsonValue = "hello"
) -> pydantic.JsonValue:
    bindings = await mcp.discover([mcp.Selection(server="test")])
    binding = bindings[0]
    result = await tools.invoke_tool(
        binding,
        connect.ToolCallBlock(
            id="echo",
            name=binding.definition.name,
            arguments={"value": value},
        ),
    )
    return result.model_dump(mode="json")


async def content(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    selected = await mcp.discover_content(
        "test", prompts=["greet"], resources=["file:///picture.png"]
    )
    prompt = await selected.get_prompt("greet", {"who": "world"})
    resource = await selected.read_resource("file:///picture.png")
    return {
        "prompt": prompt.model_dump(mode="json"),
        "resource": resource.model_dump(mode="json"),
    }


async def content_tool(
    ctx: WorkflowExecutionContext,
    kind: str = "prompt",
    source: str = "test",
) -> pydantic.JsonValue:
    selected = await mcp.discover_content(
        "test", prompts=["greet"], resources=["file:///picture.png"]
    )
    binding = selected.tools()[0 if kind == "prompt" else 1]
    result = await tools.invoke_tool(
        binding,
        connect.ToolCallBlock(
            id="content",
            name=binding.definition.name,
            arguments={
                "source": source,
                "identity": "greet" if kind == "prompt" else "file:///picture.png",
                "arguments": {"who": "world"} if kind == "prompt" else {},
            },
        ),
    )
    return result.model_dump(mode="json")
