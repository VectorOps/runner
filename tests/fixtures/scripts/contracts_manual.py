import pydantic

from vectorops.runner import models
from vectorops.runner.runner.context import WorkflowExecutionContext


async def manual(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("contracts:turn", input={"text": "hello"})
    outcome = (await ctx.join_workflows([handle]))[0]
    assert isinstance(outcome, models.StepCompleted)
    return outcome.result
