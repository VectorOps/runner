import asyncio

import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

gates: dict[int, asyncio.Event] = {}
started: asyncio.Queue[int] = asyncio.Queue()
observed: asyncio.Queue[int] = asyncio.Queue()
calls: list[int] = []
waiter: asyncio.Event = asyncio.Event()


@c.activity(kind="test.next.work")
async def work(ctx: ActivityExecutionContext, value: int) -> int:
    calls.append(value)
    started.put_nowait(value)
    await gates[value].wait()
    if value < 0:
        raise ValueError("worker failed")
    return value


@c.activity(kind="test.next.observed")
async def record(ctx: ActivityExecutionContext, value: int) -> None:
    observed.put_nowait(value)


@c.activity(kind="test.next.ready")
async def ready(ctx: ActivityExecutionContext) -> None:
    await started.get()


@c.activity(kind="test.next.release")
async def release(ctx: ActivityExecutionContext) -> None:
    gates[1].set()


@c.activity(kind="test.next.pause")
async def pause(ctx: ActivityExecutionContext) -> None:
    await waiter.wait()


@c.workflow()
async def worker(ctx: WorkflowExecutionContext, value: int) -> int:
    return await work(value)


@c.workflow()
async def pool(
    ctx: WorkflowExecutionContext, all_ready: bool = False, refill: bool = False
) -> pydantic.JsonValue:
    handles = [
        await ctx.spawn_workflow(
            "python:tests.fixtures.next_completed_workflows:worker",
            input={"value": value},
        )
        for value in (1, 2)
    ]
    if all_ready:
        await ctx.join_workflows(handles)
    results: list[pydantic.JsonValue] = []
    while handles:
        completion = await ctx.next_completed(handles)
        assert completion.outcome.kind == "completed"
        value = completion.outcome.result
        assert isinstance(value, int)
        results.append(value)
        handles.remove(completion.handle)
        await record(value)
        if refill and len(results) == 1:
            handles.append(
                await ctx.spawn_workflow(
                    "python:tests.fixtures.next_completed_workflows:worker",
                    input={"value": 3},
                )
            )
    return results


@c.workflow()
async def outcomes(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    failed = await ctx.spawn_workflow(
        "python:tests.fixtures.next_completed_workflows:worker", input={"value": -1}
    )
    await ctx.join_workflows([failed], failure_policy="collect")
    cancelled = await ctx.spawn_workflow(
        "python:tests.fixtures.next_completed_workflows:worker", input={"value": 1}
    )
    await ctx.cancel_workflows([cancelled])
    candidates = [cancelled, failed]
    first = await ctx.next_completed(candidates)
    repeated = await ctx.next_completed(candidates)
    assert first.handle == repeated.handle == failed
    candidates.remove(first.handle)
    second = await ctx.next_completed(candidates)
    return [first.outcome.kind, second.outcome.kind]


@c.workflow()
async def cancelled_waiter(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.next_completed_workflows:worker", input={"value": 1}
    )
    observer = asyncio.create_task(ctx.next_completed([handle]))
    await ready()
    observer.cancel()
    await asyncio.gather(observer, return_exceptions=True)
    await release()
    completion = await ctx.next_completed([handle])
    return completion.outcome.kind


@c.workflow()
async def recover_ready(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handles = [
        await ctx.spawn_workflow(
            "python:tests.fixtures.next_completed_workflows:worker",
            input={"value": value},
        )
        for value in (1, 2)
    ]
    await ctx.join_workflows(handles)
    await pause()
    return (await ctx.next_completed(handles)).outcome.model_dump(mode="json")


@c.workflow()
async def repeated_observers(ctx: WorkflowExecutionContext) -> bool:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.next_completed_workflows:worker", input={"value": 1}
    )
    first, second = await asyncio.gather(
        ctx.next_completed([handle]), ctx.next_completed([handle])
    )
    return first.handle == second.handle == handle
