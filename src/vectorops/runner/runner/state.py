"""Runner-owned execution registry and durable recovery checkpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import PurePosixPath
import typing

import pydantic


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutionCheckpoint(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    heads: dict[str, str] = pydantic.Field(default_factory=dict)
    updated_at: str = pydantic.Field(default_factory=timestamp)

    @pydantic.field_validator("heads", mode="after")
    @classmethod
    def validate_heads(cls, heads: dict[str, str]) -> dict[str, str]:
        if any(
            not workflow_id or not step_id for workflow_id, step_id in heads.items()
        ):
            raise ValueError("checkpoint workflow and step IDs must not be empty")
        return heads


class ExecutionRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    id: str = pydantic.Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    root_workflow_id: str = pydantic.Field(min_length=1)
    history_path: str
    status: typing.Literal["running", "completed", "failed", "cancelled"] = "running"
    created_at: str = pydantic.Field(default_factory=timestamp)
    updated_at: str = pydantic.Field(default_factory=timestamp)
    checkpoint: ExecutionCheckpoint = pydantic.Field(
        default_factory=ExecutionCheckpoint
    )
    completion_step_id: typing.Optional[str] = None

    @pydantic.model_validator(mode="after")
    def validate_path(self) -> typing.Self:
        expected = PurePosixPath("executions", self.id, "history.sqlite3")
        if self.history_path != expected.as_posix():
            raise ValueError("history path must identify this execution's database")
        return self


class RunnerState(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    schema_version: typing.Literal[1] = 1
    executions: dict[str, ExecutionRecord] = pydantic.Field(default_factory=dict)

    @pydantic.model_validator(mode="after")
    def validate_registry(self) -> typing.Self:
        if any(key != record.id for key, record in self.executions.items()):
            raise ValueError("execution registry keys must match record IDs")
        return self
