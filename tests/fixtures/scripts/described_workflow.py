from vectorops.runner.api import composition as c
from vectorops.runner.runner.context import WorkflowExecutionContext


@c.workflow(description="Double the requested number")
async def child(ctx: WorkflowExecutionContext, value: int) -> int:
    return value * 2


async def main(ctx: WorkflowExecutionContext) -> int:
    return await child(3)
