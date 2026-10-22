import pydantic
import pytest

from vectorops.runner import models


def test_cancel_operation_and_terminal_roundtrip() -> None:
    intent = models.WorkflowStep(
        id="cancel",
        workflow_id="parent",
        operation=models.WorkflowCancelOperation(
            spawn_step_ids=["spawn"], reason="stale", owner_spawn_step_id="spawn"
        ),
        outcome=models.StepWaiting(),
        deferred_delivery=True,
    )
    completion = models.WorkflowStep(
        id="terminal",
        workflow_id="child",
        operation=models.WorkflowCompleteOperation(),
        outcome=models.StepCancelled(reason="stale"),
    )
    for step in (intent, completion):
        assert models.WorkflowStep.model_validate_json(step.model_dump_json()) == step


@pytest.mark.parametrize(
    "ids,owner",
    [([], None), (["spawn", "spawn"], None), ([""], None), (["spawn"], "other")],
)
def test_invalid_cancel_selection(ids: list[str], owner: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowCancelOperation.model_validate(
            {"spawn_step_ids": ids, "owner_spawn_step_id": owner}
        )


def test_cancelled_outcome_is_not_an_activity_result() -> None:
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep(
            id="step",
            workflow_id="workflow",
            operation=models.StepOperation(kind="activity", name="work"),
            outcome=models.StepCancelled(),
        )


@pytest.mark.parametrize("kind", ["activity", "llm.generate", "workflow.describe"])
def test_activity_classification_preserves_custom_kinds(kind: str) -> None:
    operation = models.StepOperation(kind=kind, name="work")
    step = models.WorkflowStep(
        id="step",
        workflow_id="workflow",
        operation=operation,
        outcome=models.StepPending(),
        deferred_delivery=True,
    )
    restored = models.WorkflowStep.model_validate_json(step.model_dump_json())
    assert models.is_activity_operation(restored.operation)
    assert restored.operation.kind == kind


@pytest.mark.parametrize(
    "operation",
    [
        models.WorkflowSpawnOperation(child_workflow_id="child"),
        models.WorkflowJoinOperation(spawn_step_ids=["spawn"]),
        models.WorkflowCancelOperation(spawn_step_ids=["spawn"]),
        models.WorkflowCompleteOperation(),
        models.WorkflowResumeOperation(step_id="previous"),
    ],
)
def test_reserved_operations_are_not_activities(
    operation: models.WorkflowOperation,
) -> None:
    assert not models.is_activity_operation(operation)
