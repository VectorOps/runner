import asyncio
import connect
import pydantic
from vectorops.runner.api import llm
from vectorops.runner.api.mailbox import request_input
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


class Arguments(pydantic.BaseModel):
    value: int


async def invoke(name: str, value: int) -> connect.ToolResultMessage:
    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        return llm.ToolResponse(content=[connect.TextBlock(text=str(args.value))])

    return await llm.invoke_tool(
        llm.Tool(name, name, Arguments, execute),
        connect.ToolCallBlock(id=str(value), name=name, arguments={"value": value}),
    )


async def child(ctx: object) -> pydantic.JsonValue:
    return (await invoke("test", 4)).model_dump(mode="json")


async def workflow(ctx: WorkflowExecutionContext) -> None:
    assert not (await invoke("test", 1)).is_error
    await request_input("checkpoint")
    assert not (await invoke("test", 2)).is_error
    await invoke("other", 3)
    handle = await ctx.spawn_workflow("flow:child")
    await ctx.join_workflows([handle])
    return None


async def parallel(ctx: object) -> None:
    results = await asyncio.gather(invoke("test", 1), invoke("test", 2))
    assert sum(result.is_error for result in results) == 1
    assert not (await invoke("test", 3)).is_error
    return None
