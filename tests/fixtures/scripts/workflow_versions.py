import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@c.activity(kind="test.legacy")
async def legacy(ctx: ActivityExecutionContext) -> str:
    return "legacy"


@c.activity(kind="test.modern")
async def modern(ctx: ActivityExecutionContext) -> str:
    return "modern"


@c.workflow(kind="test.child", version=3)
async def child(ctx: WorkflowExecutionContext) -> dict[str, pydantic.JsonValue]:
    value = await legacy() if ctx.version < 4 else await modern()
    return {"version": ctx.version, "value": value}


@c.workflow(kind="test.workflow", version=1)
async def workflow(ctx: WorkflowExecutionContext) -> dict[str, pydantic.JsonValue]:
    value = await legacy() if ctx.version < 2 else await modern()
    return {"version": ctx.version, "value": value, "child": await child()}
