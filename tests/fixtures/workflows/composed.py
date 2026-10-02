from collections.abc import AsyncGenerator
import asyncio

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
            await probe.current.gates.get(value, probe.current.gate).wait()
        finally:
            probe.current.active -= 1
    if value < 0:
        raise ValueError("negative child")
    probe.current.finished.put_nowait(value)
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
    return list(await asyncio.gather(child(2, block=True), child(1, block=True)))


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


async def gathered(ctx: WorkflowExecutionContext, block: bool = True) -> list[int]:
    return list(await asyncio.gather(child(2, block=block), child(1, block=block)))


async def tasked(ctx: WorkflowExecutionContext) -> list[int]:
    first = asyncio.create_task(child(2, block=True))
    second = asyncio.create_task(child(1, block=True))
    await asyncio.wait([first, second])
    return [first.result(), second.result()]


async def grouped(ctx: WorkflowExecutionContext) -> list[int]:
    async with asyncio.TaskGroup() as group:
        first = group.create_task(child(2, block=True))
        second = group.create_task(child(1, block=True))
    return [first.result(), second.result()]


async def collected(ctx: WorkflowExecutionContext) -> list[str]:
    results = await asyncio.gather(child(-1), child(2), return_exceptions=True)
    return [str(result) for result in results]


async def streamed_gather(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(streaming_child(2), streaming_child(1)))


async def fail_fast(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(child(-1), child(2, block=True)))


async def many_children(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(*(child(value) for value in range(20))))


async def cancelled_wait(ctx: WorkflowExecutionContext) -> str:
    task = asyncio.create_task(child(2, block=True))
    await probe.current.started.get()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return "wait cancelled"


async def abandoned_call(ctx: WorkflowExecutionContext) -> str:
    asyncio.create_task(child(2, block=True))
    await probe.current.started.get()
    return "parent completed"


async def branches(ctx: WorkflowExecutionContext, mode: str = "children") -> list[int]:
    async def branch(value: int) -> int:
        if mode == "activities" or (mode == "mixed" and value == 1):
            result = await calculate(value, block=True)
            return await calculate(result + 10)
        result = await child(value, block=True)
        return await child(result + 10)

    return list(await asyncio.gather(branch(1), branch(2)))


@c.workflow()
async def fanout(ctx: WorkflowExecutionContext, depth: int = 2, value: int = 1) -> int:
    if depth == 0:
        return await calculate(value, block=True)
    results = await asyncio.gather(
        fanout(depth - 1, value * 2), fanout(depth - 1, value * 2 + 1)
    )
    return await calculate(sum(results))


async def parallel_activities(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(calculate(1, True), calculate(2, True)))


async def mixed_streaming(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(generate(1), calculate(2), streaming_child(3)))


async def first_completed(ctx: WorkflowExecutionContext) -> int:
    tasks = [asyncio.create_task(child(value, block=True)) for value in (1, 2)]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    result = next(iter(done)).result()
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    return await calculate(result)


async def group_failure(ctx: WorkflowExecutionContext) -> None:
    async with asyncio.TaskGroup() as group:
        group.create_task(child(-1))
        group.create_task(child(2, block=True))
