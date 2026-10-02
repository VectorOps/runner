from collections.abc import AsyncGenerator

from tests.fixtures import probe
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@c.activity()
async def effect(ctx: ActivityExecutionContext, value: int = 1) -> int:
    probe.current.contexts.append(ctx)
    if ctx.step_id not in probe.current.effects:
        probe.current.effects[ctx.step_id] = value
    probe.current.started.put_nowait(value)
    await probe.current.gate.wait()
    return probe.current.effects[ctx.step_id]


@c.streamable_activity()
async def stream(
    ctx: ActivityExecutionContext, value: int = 1
) -> AsyncGenerator[c.ActivityDelta | c.ActivityFinal[int], None]:
    probe.current.contexts.append(ctx)
    yield c.delta(value)
    probe.current.started.put_nowait(value)
    await probe.current.gate.wait()
    yield c.final(value)


async def workflow(ctx: WorkflowExecutionContext) -> int:
    return await effect()


async def streaming(ctx: WorkflowExecutionContext) -> int:
    return await stream()
