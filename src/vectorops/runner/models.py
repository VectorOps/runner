"""JSON-serializable workflow configuration and normalized step logs."""

from __future__ import annotations

import typing
from uuid import UUID

import pydantic

from vectorops.runner import usage as usage_models

_WORKFLOW_OPERATION_KINDS: typing.Final[tuple[str, ...]] = (
    "workflow.spawn",
    "workflow.join",
    "workflow.complete",
    "workflow.resume",
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
    kind: str = pydantic.Field(default="workflow", min_length=1)
    version: int = pydantic.Field(default=1, ge=1, strict=True)
    tool_policy_version: typing.Optional[typing.Literal[1]] = None
    inbox_version: typing.Optional[typing.Literal[1]] = None
    created_at: typing.Optional[str] = None
    name: typing.Optional[str] = pydantic.Field(
        default=None, min_length=1, max_length=120
    )
    usage: typing.Optional[usage_models.WorkflowUsage] = None

    def same_configuration(self, other: Workflow) -> bool:
        """Compare execution configuration without mutable presentation metadata."""
        return self.model_dump(exclude={"name", "usage"}) == other.model_dump(
            exclude={"name", "usage"}
        )


class StepOperation(_JsonModel):
    """An operation's type, implementation name, and JSON-compatible input."""

    kind: str = pydantic.Field(min_length=1)
    name: str = pydantic.Field(min_length=1)
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    version: int = pydantic.Field(default=1, ge=1, strict=True)

    @property
    def replay_ignores_input(self) -> bool:
        return self.kind.startswith("llm.")

    def matches_replay(self, recorded: StepOperation) -> bool:
        """Match activity positions, treating LLM metadata and versions as informational."""
        if (self.kind, self.name) != (recorded.kind, recorded.name):
            return False
        if self.replay_ignores_input:
            return True
        return self.input == recorded.input

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
    description: typing.Optional[str] = None


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


class WorkflowResumeOperation(_JsonModel):
    """Deliver an invocation's persisted outcome at this position in history."""

    kind: typing.Literal["workflow.resume"] = "workflow.resume"
    step_id: str = pydantic.Field(min_length=1)


def _operation_tag(operation: object) -> str:
    """Dispatch typed workflow operations while preserving arbitrary activity kinds."""
    if isinstance(
        operation,
        (
            WorkflowSpawnOperation,
            WorkflowJoinOperation,
            WorkflowCompleteOperation,
            WorkflowResumeOperation,
        ),
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
        typing.Annotated[WorkflowResumeOperation, pydantic.Tag("workflow.resume")],
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


class StepWaiting(_JsonModel):
    """Reserved history position of a join awaiting child completions."""

    kind: typing.Literal["waiting"] = "waiting"


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
    typing.Union[
        StepCompleted, StepFailed, StepJoined, StepStreaming, StepPending, StepWaiting
    ],
    pydantic.Field(discriminator="kind"),
]


class WorkflowStep(_JsonModel):
    """An operation linked to its predecessor without embedding history."""

    id: str = pydantic.Field(min_length=1)
    workflow_id: str = pydantic.Field(min_length=1)
    previous_step_id: typing.Optional[str] = pydantic.Field(default=None, min_length=1)
    operation: WorkflowOperation
    outcome: StepOutcome
    authorized: typing.Optional[bool] = pydantic.Field(
        default=None, strict=True, exclude_if=lambda value: value is None
    )
    authorization_rule_id: typing.Optional[UUID] = pydantic.Field(
        default=None, exclude_if=lambda value: value is None
    )
    created_at: typing.Optional[str] = None
    completed_at: typing.Optional[str] = None
    metrics: typing.Optional[dict[str, pydantic.JsonValue]] = None
    resume_step_id: typing.Optional[str] = pydantic.Field(
        default=None, min_length=1, exclude_if=lambda value: value is None
    )
    deferred_delivery: bool = False
    revision: int = pydantic.Field(default=1, ge=1, strict=True)
    deliveries: list[str] = pydantic.Field(
        default_factory=list, exclude_if=lambda value: not value
    )

    @pydantic.field_validator("deliveries", mode="after")
    @classmethod
    def validate_deliveries(cls, values: list[str]) -> list[str]:
        if any(not value for value in values) or len(set(values)) != len(values):
            raise ValueError("delivery step IDs must be nonempty and unique")
        return values

    def authorize(self) -> WorkflowStep:
        """Mark a pending tool invocation authorized without changing its outcome."""
        if self.operation.kind != "llm.tool" or self.outcome.kind != "pending":
            raise ValueError("authorization requires a pending tool call")
        return self.model_copy(
            update={"authorized": True, "authorization_rule_id": None}
        )

    def validate_authorization_update(self, previous: WorkflowStep) -> None:
        if self.authorization_rule_id != previous.authorization_rule_id:
            if (
                previous.authorized is True
                or previous.outcome.kind != "pending"
                or self.outcome.kind != "pending"
                or self.operation.kind != "llm.tool"
            ):
                raise ValueError(
                    "authorization provenance must be recorded before completion"
                )
        if self.authorized == previous.authorized:
            return
        if self.authorized is False and previous.authorized is not True:
            if self.operation.kind != "llm.tool" or self.outcome.kind != "pending":
                raise ValueError("authorization requires a pending tool call")
            return
        if previous.authorized is True or self.authorized is not True:
            raise ValueError("authorization cannot be revoked")
        previous.authorize()
        if self.outcome.kind != "pending":
            raise ValueError("authorization must be persisted before completion")

    def delivered(self, target: WorkflowStep) -> WorkflowStep:
        """Append delivery order without changing the anchor's outcome or links."""
        if (
            target.workflow_id != self.workflow_id
            or not target.deferred_delivery
            or target.outcome.kind in ("pending", "streaming", "waiting")
        ):
            raise ValueError("delivery requires a terminal invocation in this workflow")
        if isinstance(self.operation, WorkflowCompleteOperation):
            raise ValueError("cannot deliver after workflow completion")
        if target.id in self.deliveries:
            raise ValueError("invocation was already delivered at this position")
        return self.model_copy(
            update={
                "deliveries": [*self.deliveries, target.id],
                "revision": self.revision + 1,
            }
        )

    @pydantic.model_validator(mode="after")
    def validate_relationships(self) -> typing.Self:
        """Validate local links and outcome shape without loading referenced histories."""
        if self.previous_step_id == self.id:
            raise ValueError("a step cannot be its own predecessor")
        if self.authorized is not None and self.operation.kind != "llm.tool":
            raise ValueError("authorization is only valid for tool calls")
        if self.authorization_rule_id is not None and (
            self.operation.kind != "llm.tool" or self.authorized is None
        ):
            raise ValueError("authorization rule requires a tool decision")
        if self.resume_step_id is not None and (
            not isinstance(self.operation, WorkflowCompleteOperation)
            or self.outcome.kind != "failed"
            or self.resume_step_id != self.previous_step_id
        ):
            raise ValueError(
                "a failure resume boundary must be its completion predecessor"
            )
        if self.deferred_delivery and not isinstance(
            self.operation, (StepOperation, WorkflowJoinOperation)
        ):
            raise ValueError("deferred delivery requires an activity or join")
        if isinstance(self.operation, WorkflowResumeOperation):
            if self.operation.step_id == self.id:
                raise ValueError("a resume cannot reference itself")
            if self.outcome != StepCompleted():
                raise ValueError("a resume requires an empty completed outcome")
        if self.outcome.kind == "waiting" and not isinstance(
            self.operation, WorkflowJoinOperation
        ):
            raise ValueError("waiting state requires a workflow join operation")
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
            elif self.outcome.kind not in ("failed", "waiting"):
                raise ValueError("a join requires a joined, waiting, or failed outcome")
        elif isinstance(self.outcome, StepJoined):
            raise ValueError("a joined outcome requires a workflow join operation")
        return self
