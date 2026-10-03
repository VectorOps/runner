import asyncio

import pydantic

from vectorops.runner.api import configuration, llm, mailbox
from vectorops.runner.runner.context import WorkflowExecutionContext


async def main(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    return (await llm.resolve_settings()).model_dump(mode="json")


async def changing(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    old = await configuration.get("llm.model", "default")
    await mailbox.request_input("continue")
    new = await configuration.get("llm.model", "default")
    return [old, new]


async def children(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handles = await asyncio.gather(
        ctx.spawn_workflow("configuration", "main"),
        ctx.spawn_workflow("configuration", "main"),
    )
    outcomes = await ctx.join_workflows(list(handles))
    return [outcome.model_dump(mode="json") for outcome in outcomes]
