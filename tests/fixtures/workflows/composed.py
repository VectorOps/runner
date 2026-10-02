from collections.abc import AsyncGenerator
import typing

from tests.fixtures import probe
from tests.fixtures.library_workflows import child as library_child
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ActivityError


@c.activity()
async def calculate(
    ctx: ActivityExecutionContext, value: int, block: bool = False
) -> int:
    probe.current.calls.append(value)
    if block:
        probe.current.active += 1
        probe.current.peak_active = max(probe.current.peak_active, probe.current.active)
        probe.current.started.put_nowait(value)
        try:
            await probe.current.gate.wait()
        finally:
            probe.current.active -= 1
    if value < 0:
        raise ValueError("negative child")
    return value * 2


@c.streamable_activity()
async def generate(
    ctx: ActivityExecutionContext, value: int
) -> AsyncGenerator[c.ActivityDelta | c.ActivityFinal[int], None]:
    probe.current.calls.append(value)
    yield c.delta({"text": "partial"})
    yield c.delta({"text": "complete"})
    yield c.final(value)


@c.workflow(name="double")
async def child(
    ctx: WorkflowExecutionContext, value: int = 2, *, block: bool = False
) -> int:
    return await calculate(value, block)


@c.workflow()
async def streaming_child(ctx: WorkflowExecutionContext, value: int = 7) -> int:
    return await generate(value)


@c.workflow()
async def workflow(ctx: WorkflowExecutionContext, value: int = 3) -> int:
    return await child(value)


async def parallel(ctx: WorkflowExecutionContext) -> list[int]:
    first = await child.start(2, block=True)
    second = await child.start(1, block=True)
    return typing.cast(list[int], await c.gather_workflows([first, second]))


async def library(ctx: WorkflowExecutionContext) -> int:
    return await library_child(4)


async def caught(ctx: WorkflowExecutionContext) -> str:
    try:
        return str(await child(-1))
    except ActivityError as exc:
        return exc.error.message


async def streamed(ctx: WorkflowExecutionContext) -> int:
    return await streaming_child()


async def inline(ctx: WorkflowExecutionContext) -> int:
    async def helper(context: WorkflowExecutionContext) -> int:
        return await calculate(4)

    return await helper(ctx)
