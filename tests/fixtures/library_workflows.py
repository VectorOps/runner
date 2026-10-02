from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from tests.fixtures import probe


@c.activity()
async def multiply(ctx: ActivityExecutionContext, value: int) -> int:
    probe.current.calls.append(value)
    return value * 3


@c.workflow(name="library-multiply")
async def child(ctx: WorkflowExecutionContext, value: int = 2) -> int:
    return await multiply(value)
