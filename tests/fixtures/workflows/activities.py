import asyncio
import typing

import pydantic

from tests.fixtures import probe
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ActivityError


@activity(name="increment", kind="test.increment")
async def increment(value: int, amount: int = 1) -> int:
    if probe.current.before_call is not None:
        probe.current.before_call()
    probe.current.calls.append(value)
    return value + amount


@activity()
async def fail() -> None:
    raise ValueError("activity failed")


@activity()
async def blocked(value: int = 0) -> int:
    probe.current.active += 1
    probe.current.peak_active = max(probe.current.active, probe.current.peak_active)
    probe.current.started.put_nowait(value)
    try:
        await probe.current.gate.wait()
        return value
    except asyncio.CancelledError:
        probe.current.cancelled += 1
        raise
    finally:
        probe.current.active -= 1


@activity()
async def invalid_result() -> pydantic.JsonValue:
    return typing.cast(pydantic.JsonValue, object())


@activity()
async def nested_activity() -> int:
    return await increment(1)


async def workflow(ctx: WorkflowExecutionContext, value: int = 0) -> int:
    first = await increment(value)
    return await increment(first)


async def many(ctx: WorkflowExecutionContext, count: int = 100) -> int:
    value = 0
    for _ in range(count):
        value = await increment(value)
    return value


async def empty(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    return {"id": ctx.workflow.id, "data": str(ctx.data_path)}


async def caught(ctx: WorkflowExecutionContext) -> str:
    try:
        await fail()
    except ActivityError as exc:
        return exc.error.message
    raise AssertionError("expected failure")


async def unhandled(ctx: WorkflowExecutionContext) -> None:
    await fail()


async def root_failure(ctx: WorkflowExecutionContext) -> None:
    raise LookupError("workflow failed")


async def wait(ctx: WorkflowExecutionContext) -> int:
    return await blocked()


async def invalid(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    return await invalid_result()


async def nested(ctx: WorkflowExecutionContext) -> int:
    return await nested_activity()


async def parallel(ctx: WorkflowExecutionContext) -> list[int]:
    return list(await asyncio.gather(blocked(1), blocked(2)))


def synchronous(ctx: WorkflowExecutionContext) -> int:
    return 1
