import asyncio
from collections.abc import AsyncGenerator

import pydantic

from vectorops.runner import diagnostics, models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner import streaming


def report(context: ActivityExecutionContext) -> None:
    diagnostics.log(
        "activity.body",
        fields={
            "expected_execution": context.execution_id,
            "expected_workflow": context.workflow_id,
            "expected_step": context.step_id,
        },
    )


async def ordinary(context: ActivityExecutionContext) -> pydantic.JsonValue:
    await asyncio.sleep(0)
    report(context)
    return None


async def stream(
    context: ActivityExecutionContext,
) -> AsyncGenerator[streaming.StreamItem, None]:
    report(context)
    yield streaming.ActivityDelta("private-snapshot")
    await asyncio.sleep(0)
    report(context)
    yield streaming.ActivityFinal(None)


async def child(context: WorkflowExecutionContext) -> None:
    await asyncio.gather(
        context.run_activity(
            models.StepOperation(kind="test", name="ordinary"), ordinary
        ),
        context.run_streaming_activity(
            models.StepOperation(kind="test", name="stream"), stream
        ),
    )


async def main(context: WorkflowExecutionContext) -> None:
    children = await asyncio.gather(
        context.spawn_workflow("diagnostics:child"),
        context.spawn_workflow("diagnostics:child"),
    )
    await context.join_workflows(list(children))


async def wait(context: WorkflowExecutionContext) -> None:
    diagnostics.log("waiting")
    await asyncio.sleep(60)
