"""JSON-serializable workflow configuration and normalized step logs."""

from __future__ import annotations

import typing

import pydantic


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


StepOutcome: typing.TypeAlias = typing.Annotated[
    typing.Union[StepCompleted, StepFailed],
    pydantic.Field(discriminator="kind"),
]


class WorkflowStep(_JsonModel):
    """A finished operation linked to its predecessor without embedding history."""

    id: str = pydantic.Field(min_length=1)
    workflow_id: str = pydantic.Field(min_length=1)
    previous_step_id: typing.Optional[str] = pydantic.Field(default=None, min_length=1)
    operation: StepOperation
    outcome: StepOutcome

    @pydantic.model_validator(mode="after")
    def validate_predecessor(self) -> typing.Self:
        if self.previous_step_id == self.id:
            raise ValueError("a step cannot be its own predecessor")
        return self
