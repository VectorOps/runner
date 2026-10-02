import json
import math
import typing

import pydantic
import pytest

from vectorops.runner import models as m


def step_data() -> dict[str, pydantic.JsonValue]:
    return {
        "id": "s",
        "workflow_id": "w",
        "operation": {"kind": "activity", "name": "add", "input": {"x": 1}},
        "outcome": {"kind": "completed", "result": 2},
    }


@pytest.mark.parametrize(
    "operation,outcome",
    [
        (
            {"kind": "activity", "name": "add"},
            {"kind": "completed", "result": [None, True, {"a": 1}]},
        ),
        (
            {"kind": "workflow.spawn", "child_workflow_id": "child"},
            {"kind": "completed"},
        ),
        (
            {
                "kind": "workflow.join",
                "spawn_step_ids": ["a", "b"],
                "failure_policy": "collect",
            },
            {
                "kind": "joined",
                "child_completions": [
                    {"spawn_step_id": "a", "completion_step_id": "a-end"},
                    {"spawn_step_id": "b", "completion_step_id": "b-end"},
                ],
            },
        ),
        (
            {"kind": "workflow.complete"},
            {"kind": "failed", "error": {"type": "Error", "message": "failed"}},
        ),
    ],
)
def test_step_json_roundtrip(
    operation: dict[str, object], outcome: dict[str, object]
) -> None:
    step = m.WorkflowStep.model_validate(
        {**step_data(), "operation": operation, "outcome": outcome}
    )
    assert m.WorkflowStep.model_validate_json(step.model_dump_json()) == step
    assert json.loads(step.model_dump_json())["id"] == "s"


def test_workflow_json_defaults_and_frozen_fields() -> None:
    workflow = m.Workflow(id="w", definition_ref="example:workflow")
    assert m.Workflow.model_validate_json(workflow.model_dump_json()) == workflow
    other = m.Workflow(id="other", definition_ref="example:workflow")
    workflow.input["value"] = 1
    assert other.input == {}
    with pytest.raises(pydantic.ValidationError):
        workflow.id = "changed"


@pytest.mark.parametrize(
    "patch",
    [
        {"id": ""},
        {"workflow_id": ""},
        {"previous_step_id": ""},
        {"previous_step_id": "s"},
        {"unknown": True},
        {"operation": {"kind": "workflow.spawn", "child_workflow_id": "w"}},
        {"operation": {"kind": "workflow.spawn", "name": "wrong"}},
        {"operation": {"kind": "workflow.join", "spawn_step_ids": []}},
        {"operation": {"kind": "workflow.join", "spawn_step_ids": [""]}},
        {"operation": {"kind": "workflow.join", "spawn_step_ids": ["a", "a"]}},
        {"operation": {"kind": "workflow.join", "spawn_step_ids": ["s"]}},
        {"outcome": {"kind": "unknown"}},
        {"outcome": {"kind": "failed"}},
        {
            "outcome": {
                "kind": "joined",
                "child_completions": [
                    {"spawn_step_id": "a", "completion_step_id": "end"}
                ],
            }
        },
    ],
)
def test_invalid_step_records(patch: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.WorkflowStep.model_validate({**step_data(), **patch})


@pytest.mark.parametrize(
    "references",
    [
        [("b", "b-end"), ("a", "a-end")],
        [("a", "end"), ("b", "end")],
        [("a", "a-end"), ("a", "b-end")],
        [("a", "s"), ("b", "b-end")],
        [("a", "a-end")],
    ],
)
def test_invalid_join_references(references: list[tuple[str, str]]) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.WorkflowStep.model_validate(
            {
                **step_data(),
                "operation": {"kind": "workflow.join", "spawn_step_ids": ["a", "b"]},
                "outcome": {
                    "kind": "joined",
                    "child_completions": [
                        {"spawn_step_id": a, "completion_step_id": b}
                        for a, b in references
                    ],
                },
            }
        )


@pytest.mark.parametrize("value", [object(), math.inf, math.nan, {"nested": object()}])
def test_json_payload_rejects_non_json_values(value: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.StepCompleted(result=typing.cast(pydantic.JsonValue, value))
    with pytest.raises(pydantic.ValidationError):
        m.Workflow(
            id="w",
            definition_ref="file:workflow",
            input={"value": typing.cast(pydantic.JsonValue, value)},
        )


@pytest.mark.parametrize(
    "kind", ["workflow.spawn", "workflow.join", "workflow.complete"]
)
def test_generic_operation_cannot_impersonate_workflow_operations(kind: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.StepOperation(kind=kind, name="invalid")
