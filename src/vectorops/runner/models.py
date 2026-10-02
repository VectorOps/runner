"""JSON-serializable workflow configuration and normalized step logs."""

from __future__ import annotations

import typing

import pydantic

_WORKFLOW_OPERATION_KINDS: typing.Final[tuple[str, ...]] = (
    "workflow.spawn",
    "workflow.join",
    "workflow.complete",
)


class _JsonModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid",
        frozen=True,
        allow_inf_nan=False,
    )


class Workflow(_JsonModel):
    """Configuration needed to initialize a workflow independently of its history."""

    id: str = pydantic.Field(min_length=1)
    definition_ref: str = pydantic.Field(min_length=1)
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


class StepOperation(_JsonModel):
    """An operation's type, implementation name, and JSON-compatible input."""

    kind: str = pydantic.Field(min_length=1)
    name: str = pydantic.Field(min_length=1)
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)

    @pydantic.field_validator("kind", mode="after")
    @classmethod
    def validate_kind(cls, kind: str) -> str:
        if kind in _WORKFLOW_OPERATION_KINDS:
            raise ValueError("workflow operations require their typed operation model")
        return kind


class WorkflowSpawnOperation(_JsonModel):
    """Establish a child invocation whose configuration is stored independently."""

    kind: typing.Literal["workflow.spawn"] = "workflow.spawn"
    child_workflow_id: str = pydantic.Field(min_length=1)


class WorkflowJoinOperation(_JsonModel):
    """Wait for children in invocation order, propagating or collecting failures."""

    kind: typing.Literal["workflow.join"] = "workflow.join"
    spawn_step_ids: list[str] = pydantic.Field(min_length=1)
    failure_policy: typing.Literal["propagate", "collect"] = "propagate"

    @pydantic.field_validator("spawn_step_ids", mode="after")
    @classmethod
    def validate_spawn_steps(cls, step_ids: list[str]) -> list[str]:
        if any(not step_id for step_id in step_ids):
            raise ValueError("spawn step IDs must not be empty")
        if len(set(step_ids)) != len(step_ids):
            raise ValueError("spawn step IDs must be unique")
        return step_ids


class WorkflowCompleteOperation(_JsonModel):
    """Record the workflow's final result or error, even without prior activities."""

    kind: typing.Literal["workflow.complete"] = "workflow.complete"


def _operation_tag(operation: object) -> str:
    """Dispatch typed workflow operations while preserving arbitrary activity kinds."""
    if isinstance(
        operation,
        (WorkflowSpawnOperation, WorkflowJoinOperation, WorkflowCompleteOperation),
    ):
        return operation.kind
    if isinstance(operation, dict):
        kind = operation.get("kind")
        if isinstance(kind, str) and kind in _WORKFLOW_OPERATION_KINDS:
            return kind
    return "operation"


WorkflowOperation: typing.TypeAlias = typing.Annotated[
    typing.Union[
        typing.Annotated[StepOperation, pydantic.Tag("operation")],
        typing.Annotated[WorkflowSpawnOperation, pydantic.Tag("workflow.spawn")],
        typing.Annotated[WorkflowJoinOperation, pydantic.Tag("workflow.join")],
        typing.Annotated[WorkflowCompleteOperation, pydantic.Tag("workflow.complete")],
    ],
    pydantic.Discriminator(_operation_tag),
]


class StepError(_JsonModel):
    """Serializable error information without a Python exception object."""

    type: str = pydantic.Field(min_length=1)
    message: str


class StepCompleted(_JsonModel):
    """The recorded result of a successful operation."""

    kind: typing.Literal["completed"] = "completed"
    result: pydantic.JsonValue = None


class StepFailed(_JsonModel):
    """The recorded error of a failed operation."""

    kind: typing.Literal["failed"] = "failed"
    error: StepError


class StepStreaming(_JsonModel):
    """Latest replacement state of an unfinished streaming activity."""

    kind: typing.Literal["streaming"] = "streaming"
    state: pydantic.JsonValue = None


class StepPending(_JsonModel):
    """Durable identity of an ordinary activity whose outcome is not yet known."""

    kind: typing.Literal["pending"] = "pending"


class ChildWorkflowCompletion(_JsonModel):
    """Bind a spawned child to the exact terminal step observed by a join."""

    spawn_step_id: str = pydantic.Field(min_length=1)
    completion_step_id: str = pydantic.Field(min_length=1)


class StepJoined(_JsonModel):
    """Recorded child completions in join order, without duplicating child results."""

    kind: typing.Literal["joined"] = "joined"
    child_completions: list[ChildWorkflowCompletion] = pydantic.Field(min_length=1)

    @pydantic.field_validator("child_completions", mode="after")
    @classmethod
    def validate_completions(
        cls, completions: list[ChildWorkflowCompletion]
    ) -> list[ChildWorkflowCompletion]:
        if len({item.spawn_step_id for item in completions}) != len(completions):
            raise ValueError("child spawn step IDs must be unique")
        if len({item.completion_step_id for item in completions}) != len(completions):
            raise ValueError("child completion step IDs must be unique")
        return completions


StepOutcome: typing.TypeAlias = typing.Annotated[
    typing.Union[StepCompleted, StepFailed, StepJoined, StepStreaming, StepPending],
    pydantic.Field(discriminator="kind"),
]


class WorkflowStep(_JsonModel):
    """An operation linked to its predecessor without embedding history."""

    id: str = pydantic.Field(min_length=1)
    workflow_id: str = pydantic.Field(min_length=1)
    previous_step_id: typing.Optional[str] = pydantic.Field(default=None, min_length=1)
    operation: WorkflowOperation
    outcome: StepOutcome

    @pydantic.model_validator(mode="after")
    def validate_relationships(self) -> typing.Self:
        """Validate local links and outcome shape without loading referenced histories."""
        if self.previous_step_id == self.id:
            raise ValueError("a step cannot be its own predecessor")
        if self.outcome.kind in ("streaming", "pending") and not isinstance(
            self.operation, StepOperation
        ):
            raise ValueError("unfinished state requires an activity operation")
        if isinstance(self.operation, WorkflowSpawnOperation):
            if self.operation.child_workflow_id == self.workflow_id:
                raise ValueError("a child invocation must have a distinct workflow ID")
        if isinstance(self.operation, WorkflowJoinOperation):
            if self.id in self.operation.spawn_step_ids:
                raise ValueError("a join cannot reference itself as a spawn")
            if isinstance(self.outcome, StepJoined):
                spawn_ids = [
                    item.spawn_step_id for item in self.outcome.child_completions
                ]
                if spawn_ids != self.operation.spawn_step_ids:
                    raise ValueError(
                        "joined completions must match spawn steps in order"
                    )
                if any(
                    item.completion_step_id == self.id
                    for item in self.outcome.child_completions
                ):
                    raise ValueError("a join cannot reference itself as a completion")
            elif not isinstance(self.outcome, StepFailed):
                raise ValueError("a join requires a joined or failed outcome")
        elif isinstance(self.outcome, StepJoined):
            raise ValueError("a joined outcome requires a workflow join operation")
        return self
