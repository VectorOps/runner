import asyncio
from pathlib import Path

import pytest

from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner.activity_context import ActivityExecutionContext
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


async def invoke(ctx: ActivityExecutionContext) -> int:
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
    assert await ctx.spawn_workflow("file:entry", input={"value": 1}) == handle


async def test_join_response_and_error(tmp_path: Path) -> None:
    handle = context.ChildWorkflowHandle("parent", "child", "spawn")
    outcomes: list[models.ChildOutcome] = [models.StepCompleted(result=1)]
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
            await ctx.spawn_workflow("file:workflow")
        else:
            await ctx.join_workflows(
                [context.ChildWorkflowHandle("parent", "child", "spawn")]
            )


@pytest.mark.parametrize(
    "outcome",
    [
        models.StepCompleted(result=7),
        models.StepFailed(error=models.StepError(type="Error", message="failed")),
        models.StepCancelled(reason="stale"),
    ],
)
async def test_await_handle_collects_outcome(
    tmp_path: Path, outcome: models.ChildOutcome
) -> None:
    identity = context.ChildWorkflowHandle("parent", "child", "spawn")
    submitter = StubSubmitter(context.SpawnResponse(identity))
    ctx = context.WorkflowExecutionContext(
        RunnerProject(tmp_path, RunnerConfig()),
        models.Workflow(id="parent", definition_ref="file:workflow"),
        submitter,
    )
    handle = await ctx.spawn_workflow("file:child")
    assert handle == identity
    assert hash(handle) == hash(identity)
    assert repr(handle) == repr(identity)
    submitter.response = context.JoinResponse([outcome])
    assert await handle == outcome
    assert await handle == outcome
    joins = submitter.requests[1:]
    assert len(joins) == 2
    for request in joins:
        assert request.kind == "join"
        assert request.operation.spawn_step_ids == ["spawn"]
        assert request.operation.failure_policy == "collect"


async def test_unbound_handle_cannot_be_awaited() -> None:
    with pytest.raises(RuntimeError, match="spawning context"):
        await context.ChildWorkflowHandle("parent", "child", "spawn")


async def test_handle_is_accepted_by_asyncio_helpers(tmp_path: Path) -> None:
    identity = context.ChildWorkflowHandle("parent", "child", "spawn")
    submitter = StubSubmitter(context.SpawnResponse(identity))
    ctx = context.WorkflowExecutionContext(
        RunnerProject(tmp_path, RunnerConfig()),
        models.Workflow(id="parent", definition_ref="file:workflow"),
        submitter,
    )
    handle = await ctx.spawn_workflow("file:child")
    outcome = models.StepCompleted(result=7)
    submitter.response = context.JoinResponse([outcome])
    assert await asyncio.gather(handle) == [outcome]
    assert await asyncio.ensure_future(handle) == outcome
    assert [await item for item in asyncio.as_completed([handle])] == [outcome]


@pytest.mark.parametrize(
    "handles",
    [
        [],
        [context.ChildWorkflowHandle("parent", "child", "spawn")] * 2,
        [context.ChildWorkflowHandle("other", "child", "spawn")],
    ],
)
async def test_next_completed_rejects_invalid_candidates(
    tmp_path: Path, handles: list[context.ChildWorkflowHandle]
) -> None:
    submitter = StubSubmitter(context.JoinResponse([]))
    ctx = context.WorkflowExecutionContext(
        RunnerProject(tmp_path, RunnerConfig()),
        models.Workflow(id="parent", definition_ref="file:workflow"),
        submitter,
    )
    with pytest.raises(ValueError):
        await ctx.next_completed(handles)
    assert submitter.requests == []


async def test_next_completed_response(tmp_path: Path) -> None:
    handle = context.ChildWorkflowHandle("parent", "child", "spawn")
    completion = context.ChildCompletion(handle, models.StepCancelled(reason="stale"))
    ctx = make_context(tmp_path, context.NextCompletedResponse(completion))
    assert await ctx.next_completed([handle]) is completion
    with pytest.raises(RuntimeError, match="invalid next_completed"):
        await make_context(tmp_path, context.JoinResponse([])).next_completed([handle])
