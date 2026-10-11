import asyncio

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api import tools
from vectorops.runner.api.mailbox import request_input
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

started = asyncio.Event()
release = asyncio.Event()
finished = asyncio.Event()


@c.activity()
async def work(ctx: ActivityExecutionContext) -> str:
    started.set()
    await release.wait()
    finished.set()
    return "worked"


@c.workflow()
async def child(ctx: WorkflowExecutionContext) -> str:
    return str(await request_input("child", "continue"))


async def mixed(ctx: WorkflowExecutionContext) -> list[str]:
    return list(await asyncio.gather(child(), work()))


async def children(ctx: WorkflowExecutionContext) -> list[str]:
    return list(await asyncio.gather(child(), child()))


class Arguments(pydantic.BaseModel):
    pass


async def parallel_tools(ctx: WorkflowExecutionContext) -> None:
    async def execute(
        context: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        started.set()
        await release.wait()
        finished.set()
        return tools.ToolResponse(content=[connect.TextBlock(text="worked")])

    active = tools.Tool("work", "Work", Arguments, execute)
    waiting = tools.Tool(
        "approval", "Approve", Arguments, execute, approval=tools.ToolApproval()
    )
    await asyncio.gather(
        *(
            tools.invoke_tool(
                tool,
                connect.ToolCallBlock(
                    id=tool.definition.name, name=tool.definition.name, arguments={}
                ),
            )
            for tool in (active, waiting)
        )
    )
