import asyncio
from typing import Optional

import pydantic
from vectorops.runner.api import composition as c
from vectorops.runner.api.mailbox import request_input
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

started: asyncio.Queue[str] = asyncio.Queue()
release: asyncio.Event = asyncio.Event()
cleaned: list[str] = []
calls: list[str] = []
resource_cleanup: list[str] = []
cleanup_gate: asyncio.Event = asyncio.Event()
cleanup_started: asyncio.Event = asyncio.Event()
owner_task: Optional[asyncio.Task[str]] = None
should_cancel: bool = True


@c.activity(kind="test.cancel.block")
async def block(ctx: ActivityExecutionContext, name: str) -> str:
    calls.append(name)
    if ctx.resources is not None:

        async def cleanup() -> None:
            resource_cleanup.append(name)

        ctx.resources.register(name, cleanup)
    started.put_nowait(name)
    try:
        await release.wait()
        return name
    finally:
        cleaned.append(name)


@c.activity(kind="test.cancel.ready")
async def ready(ctx: ActivityExecutionContext, count: int = 1) -> None:
    for _ in range(count):
        await started.get()


@c.activity(kind="test.cancel.release")
async def unblock(ctx: ActivityExecutionContext) -> None:
    release.set()


@c.workflow()
async def child(ctx: WorkflowExecutionContext, name: str = "child") -> str:
    return await block(name)


@c.workflow()
async def nested(ctx: WorkflowExecutionContext) -> str:
    return await child("grandchild")


@c.workflow()
async def owned(ctx: WorkflowExecutionContext, nested_child: bool = False) -> str:
    async with asyncio.TaskGroup() as group:
        worker = group.create_task(nested() if nested_child else child())
        await ready()
        worker.cancel()
    return "parent continues"


@c.workflow()
async def explicit(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.cancellation_workflows:child"
    )
    await ready()
    first = await ctx.cancel_workflows([handle], reason="stale")
    second = await ctx.cancel_workflows([handle], reason="again")
    joined = await ctx.join_workflows([handle], failure_policy="collect")
    return [first[0].kind, second[0].kind, joined[0].kind]


@c.workflow()
async def observer(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.cancellation_workflows:child"
    )
    waiter = asyncio.create_task(ctx.join_workflows([handle]))
    await ready()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    await unblock()
    outcomes = await ctx.join_workflows([handle])
    return outcomes[0].kind


@c.workflow()
async def failure(ctx: WorkflowExecutionContext) -> None:
    raise ValueError("child failure")


@c.workflow()
async def group_failure(ctx: WorkflowExecutionContext) -> str:
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(child())
            await ready()
            group.create_task(failure())
    except ExceptionGroup:
        return "caught"
    return "unexpected"


@c.workflow()
async def completed(ctx: WorkflowExecutionContext) -> str:
    return "done"


@c.workflow()
async def cancel_completed(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.cancellation_workflows:completed"
    )
    await ctx.join_workflows([handle])
    result = await ctx.cancel_workflows([handle])
    return result[0].kind


@c.workflow()
async def mailbox_child(ctx: WorkflowExecutionContext) -> str:
    await request_input("child", "waiting")
    return "answered"


@c.workflow()
async def mailbox_parent(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.cancellation_workflows:mailbox_child"
    )
    await request_input("parent", "cancel child")
    result = await ctx.cancel_workflows([handle])
    await request_input("parent", "still available")
    return result[0].kind


@c.activity(kind="test.cancel.slow-cleanup")
async def slow_cleanup(ctx: ActivityExecutionContext) -> None:
    started.put_nowait("slow")
    try:
        await asyncio.Event().wait()
    finally:
        cleanup_started.set()
        await cleanup_gate.wait()
        cleaned.append("slow")


@c.workflow()
async def slow_child(ctx: WorkflowExecutionContext) -> str:
    await slow_cleanup()
    return "unexpected"


@c.workflow()
async def repeated(ctx: WorkflowExecutionContext) -> str:
    global owner_task
    owner_task = asyncio.create_task(slow_child())
    await ready()
    owner_task.cancel()
    await asyncio.gather(owner_task, return_exceptions=True)
    return "done"


@c.workflow()
async def cancelled_success(ctx: WorkflowExecutionContext) -> str:
    global owner_task
    owner_task = asyncio.create_task(completed())
    result = await asyncio.gather(owner_task, return_exceptions=True)
    return "cancelled" if isinstance(result[0], asyncio.CancelledError) else "completed"


@c.workflow()
async def sibling(ctx: WorkflowExecutionContext) -> str:
    async with asyncio.TaskGroup() as group:
        cancelled = group.create_task(child("cancelled"))
        remaining = group.create_task(child("remaining"))
        await ready(2)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        await unblock()
    return remaining.result()


@c.workflow()
async def suspended(ctx: WorkflowExecutionContext) -> str:
    return await mailbox_child()


@c.workflow()
async def replay_cancellation(ctx: WorkflowExecutionContext) -> str:
    worker = asyncio.create_task(child())
    await ready()
    if should_cancel:
        worker.cancel()
    result = await asyncio.gather(worker, return_exceptions=True)
    return "cancelled" if isinstance(result[0], asyncio.CancelledError) else "completed"


@c.workflow()
async def spawn_race(ctx: WorkflowExecutionContext) -> str:
    global owner_task
    owner_task = asyncio.create_task(child())
    await asyncio.gather(owner_task, return_exceptions=True)
    return "done"


@c.workflow()
async def suppress(ctx: WorkflowExecutionContext) -> str:
    try:
        return await block("suppressed")
    except asyncio.CancelledError:
        return "suppressed"


@c.workflow()
async def cancel_suppressed(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow(
        "python:tests.fixtures.cancellation_workflows:suppress"
    )
    await ready()
    result = await ctx.cancel_workflows([handle])
    return result[0].kind
