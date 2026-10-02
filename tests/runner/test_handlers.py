import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner import context, handlers
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.workflow import WorkflowRunner


class ActivityHandlerStub:
    async def execute(
        self, request: context.ActivityRequest
    ) -> context.ActivityResponse:
        return context.ActivityResponse(
            models.StepCompleted(result=await request.invoke())
        )


async def test_registry_dispatch_and_duplicate_registration() -> None:
    registry = handlers.HandlerRegistry()
    registry.register(context.ActivityRequest, ActivityHandlerStub())

    async def invoke() -> int:
        return 9

    response = await registry.dispatch(
        context.ActivityRequest(
            models.StepOperation(kind="activity", name="work"), invoke
        )
    )
    assert response.kind == "activity"
    assert response.outcome.kind == "completed"
    assert response.outcome.result == 9
    with pytest.raises(ValueError, match="already registered"):
        registry.register(context.ActivityRequest, ActivityHandlerStub())
    with pytest.raises(KeyError):
        await registry.dispatch(context.SpawnRequest("file", "workflow", {}))


@pytest.mark.parametrize(
    "operation,outcome",
    [
        (models.StepOperation(kind="activity", name="work"), models.StepCompleted()),
        (
            models.WorkflowSpawnOperation(child_workflow_id="child"),
            models.StepFailed(error=models.StepError(type="Error", message="failed")),
        ),
    ],
)
def test_require_successful_spawn(
    operation: models.WorkflowOperation, outcome: models.StepOutcome
) -> None:
    step = models.WorkflowStep(
        id="s", workflow_id="parent", operation=operation, outcome=outcome
    )
    with pytest.raises(ReplayMismatchError):
        handlers.require_spawn(step)


@pytest.mark.parametrize("mutation", ["definition", "input"])
async def test_replayed_spawn_validates_child_configuration(
    runner: WorkflowRunner, history: MemoryHistory, mutation: str
) -> None:
    await runner.run("children", "sequential", workflow_id="root")
    path = history.path(history.heads["root"])
    spawn = handlers.require_spawn(path[0])
    child = history.workflows[spawn.child_workflow_id]
    update = (
        {"definition_ref": "other:workflow"}
        if mutation == "definition"
        else {"input": {"value": 99}}
    )
    history.workflows[child.id] = child.model_copy(update=update)
    before = list(history.appended)
    with pytest.raises(ReplayMismatchError, match="configuration"):
        await runner.run("children", "sequential", continue_from_step_id=path[-1].id)
    assert history.appended == before


@pytest.mark.parametrize(
    "mutation",
    ["foreign_spawn", "activity_completion", "invalid_spawn", "missing_completion"],
)
async def test_join_validates_referenced_records(
    runner: WorkflowRunner, history: MemoryHistory, mutation: str
) -> None:
    await runner.run("children", "sequential", workflow_id="root")
    path = history.path(history.heads["root"])
    join = path[1]
    assert join.outcome.kind == "joined"
    completion_id = join.outcome.child_completions[0].completion_step_id
    if mutation == "foreign_spawn":
        history.steps[path[0].id] = path[0].model_copy(
            update={"workflow_id": "foreign"}
        )
    elif mutation == "activity_completion":
        completion = history.steps[completion_id]
        history.steps[completion_id] = completion.model_copy(
            update={"operation": models.StepOperation(kind="activity", name="wrong")}
        )
    elif mutation == "invalid_spawn":
        history.steps[path[0].id] = path[0].model_copy(
            update={"operation": models.StepOperation(kind="activity", name="wrong")}
        )
    else:
        del history.steps[completion_id]
    error = KeyError if mutation == "missing_completion" else ReplayMismatchError
    with pytest.raises(error):
        await runner.run("children", "sequential", continue_from_step_id=path[-1].id)
