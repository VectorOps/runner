import asyncio
import pydantic

from vectorops.runner.api.mailbox import request_input
from vectorops.runner.runner.activity_context import ActivityExecutionContext


async def workflow(ctx: object) -> pydantic.JsonValue:
    return await request_input("user.message", {"prompt": "Your message?"})


async def parallel(ctx: object) -> list[pydantic.JsonValue]:
    return list(
        await asyncio.gather(
            request_input("user.message", "First"),
            request_input("user.message", "Second"),
            request_input("approval", "Approve?"),
        )
    )


async def tool(ctx: object) -> pydantic.JsonValue:
    import connect
    from vectorops.runner.api import llm

    class Arguments(pydantic.BaseModel):
        value: int

    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    result = await llm.invoke_tool(
        llm.Tool("test", "Test", Arguments, execute),
        connect.ToolCallBlock(id="call", name="test", arguments={"value": 1}),
    )
    return result.model_dump(mode="json")


async def tool_twice(ctx: object) -> pydantic.JsonValue:
    await tool(ctx)
    return await tool(ctx)
