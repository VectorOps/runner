import pydantic
import pytest

from vectorops.runner import models


def tool_step() -> models.WorkflowStep:
    return models.WorkflowStep(
        id="call",
        workflow_id="root",
        operation=models.StepOperation(kind="llm.tool", name="invoke_tool"),
        outcome=models.StepPending(),
    )


def test_legacy_authorization_is_absent_and_roundtrips() -> None:
    step = tool_step()
    assert step.authorized is None
    assert "authorized" not in step.model_dump()
    assert models.WorkflowStep.model_validate_json(step.model_dump_json()) == step
    authorized = step.authorize()
    assert (
        models.WorkflowStep.model_validate_json(authorized.model_dump_json())
        == authorized
    )


@pytest.mark.parametrize("value", ["true", 1, {}, []])
def test_authorization_rejects_non_boolean_values(value: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep.model_validate(
            {**tool_step().model_dump(), "authorized": value}
        )


def test_authorization_requires_pending_tool_and_separate_transition() -> None:
    step = tool_step()
    authorized = step.authorize()
    authorized.validate_authorization_update(step)
    with pytest.raises(ValueError, match="revoked"):
        step.validate_authorization_update(authorized)
    with pytest.raises(ValueError, match="before completion"):
        authorized.model_copy(
            update={"outcome": models.StepCompleted()}
        ).validate_authorization_update(step)
    with pytest.raises(ValueError, match="pending tool"):
        step.model_copy(update={"outcome": models.StepCompleted()}).authorize()
    with pytest.raises(pydantic.ValidationError, match="only valid for tool"):
        models.WorkflowStep.model_validate(
            {
                **authorized.model_dump(),
                "operation": {"kind": "activity", "name": "work"},
            }
        )
