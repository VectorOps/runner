from pathlib import Path

import pytest

from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.project import RunnerConfig, RunnerProject


class StubSubmitter:
    def __init__(self, response: context.OperationResponse) -> None:
        self.response = response
        self.requests: list[context.OperationRequest] = []

    async def submit(
        self, request: context.OperationRequest
    ) -> context.OperationResponse:
        self.requests.append(request)
        return self.response


def make_context(
    tmp_path: Path, response: context.OperationResponse
) -> context.WorkflowExecutionContext:
    return context.WorkflowExecutionContext(
        RunnerProject(tmp_path, RunnerConfig()),
        models.Workflow(id="parent", definition_ref="file:workflow"),
        StubSubmitter(response),
    )


async def invoke() -> int:
    return 1


async def test_activity_response_and_error(tmp_path: Path) -> None:
    ctx = make_context(
        tmp_path, context.ActivityResponse(models.StepCompleted(result=7))
    )
    assert (
        await ctx.run_activity(
            models.StepOperation(kind="activity", name="work"), invoke
        )
        == 7
    )
    ctx = make_context(
        tmp_path,
        context.ActivityResponse(
            models.StepFailed(error=models.StepError(type="Error", message="failed"))
        ),
    )
    with pytest.raises(ActivityError, match="failed"):
        await ctx.run_activity(
            models.StepOperation(kind="activity", name="work"), invoke
        )


async def test_spawn_response(tmp_path: Path) -> None:
    handle = context.ChildWorkflowHandle("parent", "child", "spawn")
    ctx = make_context(tmp_path, context.SpawnResponse(handle))
    assert await ctx.spawn_workflow("file", "entry", input={"value": 1}) == handle


async def test_join_response_and_error(tmp_path: Path) -> None:
    handle = context.ChildWorkflowHandle("parent", "child", "spawn")
    outcomes: list[models.StepCompleted | models.StepFailed] = [
        models.StepCompleted(result=1)
    ]
    ctx = make_context(tmp_path, context.JoinResponse(outcomes))
    assert await ctx.join_workflows([handle]) == outcomes
    ctx = make_context(
        tmp_path,
        context.JoinResponse(
            [], error=models.StepError(type="Error", message="failed")
        ),
    )
    with pytest.raises(ActivityError, match="failed"):
        await ctx.join_workflows([handle])


async def test_foreign_handle_is_rejected(tmp_path: Path) -> None:
    ctx = make_context(tmp_path, context.JoinResponse([]))
    with pytest.raises(ValueError, match="belong"):
        await ctx.join_workflows(
            [context.ChildWorkflowHandle("other", "child", "spawn")]
        )


@pytest.mark.parametrize("operation", ["activity", "spawn", "join"])
async def test_wrong_response_variant(tmp_path: Path, operation: str) -> None:
    ctx = make_context(
        tmp_path,
        (
            context.JoinResponse([])
            if operation != "join"
            else context.ActivityResponse(models.StepCompleted())
        ),
    )
    with pytest.raises(RuntimeError, match="invalid"):
        if operation == "activity":
            await ctx.run_activity(
                models.StepOperation(kind="activity", name="work"), invoke
            )
        elif operation == "spawn":
            await ctx.spawn_workflow("file")
        else:
            await ctx.join_workflows(
                [context.ChildWorkflowHandle("parent", "child", "spawn")]
            )
