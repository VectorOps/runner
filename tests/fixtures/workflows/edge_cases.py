import asyncio
import typing

import pydantic

from tests.fixtures import probe
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.context import WorkflowExecutionContext


@activity()
async def identity(value: pydantic.JsonValue) -> pydantic.JsonValue:
    probe.current.calls.append(1)
    return value


async def invalid_activity_input(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    return await identity(typing.cast(pydantic.JsonValue, object()))


async def invalid_workflow_result(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    return typing.cast(pydantic.JsonValue, object())


async def empty_join(ctx: WorkflowExecutionContext) -> None:
    await ctx.join_workflows([])


async def duplicate_join(ctx: WorkflowExecutionContext) -> None:
    handle = await ctx.spawn_workflow("children", "child", input={"value": 1})
    await ctx.join_workflows([handle, handle])


async def repeated_join(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("children", "child", input={"value": 2})
    await ctx.join_workflows([handle])
    outcomes = await ctx.join_workflows([handle])
    if outcomes[0].kind != "completed":
        raise AssertionError("unexpected failure")
    return outcomes[0].result


async def no_join(ctx: WorkflowExecutionContext) -> str:
    await ctx.spawn_workflow("activities", "wait")
    await probe.current.started.get()
    return "parent finished"


async def failed_parent(ctx: WorkflowExecutionContext) -> None:
    await ctx.spawn_workflow("activities", "wait")
    await probe.current.started.get()
    raise RuntimeError("parent failed")


async def cancelled_workflow(ctx: WorkflowExecutionContext) -> None:
    raise asyncio.CancelledError()
