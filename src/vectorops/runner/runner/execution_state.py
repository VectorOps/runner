"""Execution-specific recovery metadata owned by the workflow history database."""

from datetime import datetime, timezone
import typing

import pydantic

from vectorops.runner.environment.target import LocalExecutionTarget

ExecutionStatus: typing.TypeAlias = typing.Literal[
    "running", "paused", "completed", "failed", "cancelled"
]


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutionCheckpoint(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    heads: dict[str, str] = pydantic.Field(default_factory=dict)
    updated_at: str = pydantic.Field(default_factory=timestamp)

    @pydantic.field_validator("heads", mode="after")
    @classmethod
    def validate_heads(cls, heads: dict[str, str]) -> dict[str, str]:
        if any(not key or not value for key, value in heads.items()):
            raise ValueError("checkpoint workflow and step IDs must not be empty")
        return heads


class ExecutionRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    id: str = pydantic.Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    root_workflow_id: str = pydantic.Field(min_length=1)
    status: ExecutionStatus = "running"
    created_at: str = pydantic.Field(default_factory=timestamp)
    updated_at: str = pydantic.Field(default_factory=timestamp)
    checkpoint: ExecutionCheckpoint = pydantic.Field(
        default_factory=ExecutionCheckpoint
    )
    completion_step_id: typing.Optional[str] = None
    target: typing.Optional[LocalExecutionTarget] = None
    referenced_targets: list[LocalExecutionTarget] = pydantic.Field(
        default_factory=list
    )
