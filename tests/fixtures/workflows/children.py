import typing

import pydantic

from tests.fixtures import probe
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ActivityError


@activity()
async def work(ctx: ActivityExecutionContext, value: int, block: bool = False) -> int:
    probe.current.contexts.append(ctx)
    probe.current.calls.append(value)
    if block:
        probe.current.active += 1
        probe.current.peak_active = max(probe.current.active, probe.current.peak_active)
        probe.current.started.put_nowait(value)
        try:
            await probe.current.gate.wait()
        finally:
            probe.current.active -= 1
    if value < 0:
        raise ValueError(f"child {value} failed")
    return value * 2


async def child(ctx: WorkflowExecutionContext, value: int, block: bool = False) -> int:
    return await work(value, block)


async def parent(
    ctx: WorkflowExecutionContext,
    values: list[int],
    block: bool = False,
    policy: typing.Literal["propagate", "collect"] = "propagate",
) -> pydantic.JsonValue:
    handles = [
        await ctx.spawn_workflow(
            "children:child", input={"value": value, "block": block}
        )
        for value in values
    ]
    outcomes = await ctx.join_workflows(handles, failure_policy=policy)
    return [
        (
            outcome.result
            if outcome.kind == "completed"
            else {"error": outcome.error.message}
        )
        for outcome in outcomes
    ]


async def sequential(ctx: WorkflowExecutionContext) -> list[pydantic.JsonValue]:
    results: list[pydantic.JsonValue] = []
    for value in (1, 2):
        handle = await ctx.spawn_workflow("children:child", input={"value": value})
        outcomes = await ctx.join_workflows([handle])
        if outcomes[0].kind == "completed":
            results.append(outcomes[0].result)
    return results


async def nested(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("children:sequential")
    outcomes = await ctx.join_workflows([handle])
    if outcomes[0].kind != "completed":
        raise AssertionError("child failed")
    return outcomes[0].result


async def deep_leaf(ctx: WorkflowExecutionContext, value: int = 1) -> int:
    first = await work(value)
    return await work(first)


async def deep_child(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("children:deep_leaf")
    outcomes = await ctx.join_workflows([handle])
    if outcomes[0].kind != "completed":
        raise AssertionError("grandchild failed")
    return outcomes[0].result


async def deep_parent(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("children:deep_child")
    outcomes = await ctx.join_workflows([handle])
    if outcomes[0].kind != "completed":
        raise AssertionError("child failed")
    return outcomes[0].result


async def catch(ctx: WorkflowExecutionContext) -> str:
    handle = await ctx.spawn_workflow("children:child", input={"value": -1})
    try:
        await ctx.join_workflows([handle])
    except ActivityError as exc:
        return exc.error.message
    raise AssertionError("child should fail")


async def abandon(ctx: WorkflowExecutionContext) -> None:
    await ctx.spawn_workflow("activities:wait")
    raise RuntimeError("parent failed")
