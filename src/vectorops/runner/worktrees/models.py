"""Durable worktree records and management request contracts."""

from datetime import datetime, timezone
from typing import Literal, Optional, Self
from uuid import UUID

import pydantic


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorktreeError(ValueError):
    pass


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class Settings(Model):
    max_worktrees: int = pydantic.Field(default=20, ge=0, strict=True)
    git_executable: str = pydantic.Field(default="git", min_length=1)
    timeout_seconds: float = pydantic.Field(default=120, gt=0)


class Create(Model):
    name: str = pydantic.Field(min_length=1, max_length=200)
    description: str = pydantic.Field(default="", max_length=4000)
    branch: Optional[str] = pydantic.Field(default=None, min_length=1, max_length=1024)
    base_ref: str = pydantic.Field(default="HEAD", min_length=1, max_length=1024)

    @pydantic.field_validator("name", "branch", "base_ref")
    @classmethod
    def nonempty(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and (not value.strip() or "\0" in value):
            raise ValueError("names and references must be nonempty without NULs")
        return value


class CreateRequest(Create):
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)


class Reference(Model):
    worktree_id: UUID


class Get(Reference):
    refresh: bool = False


class Delete(Reference):
    force: bool = pydantic.Field(default=False, strict=True)


class Update(Reference):
    name: str = pydantic.Field(min_length=1, max_length=200)
    description: Optional[str] = pydantic.Field(default=None, max_length=4000)

    @pydantic.field_validator("name")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip() or "\0" in value:
            raise ValueError("name must be nonempty without NULs")
        return value


class Query(Model):
    after: Optional[UUID] = None
    limit: int = pydantic.Field(default=100, ge=1, le=500)
    include_deleted: bool = False
    refresh: bool = False


class Origin(Model):
    kind: Literal["user", "workflow"] = "user"
    execution_id: Optional[str] = None
    workflow_id: Optional[str] = None
    step_id: Optional[str] = None

    @pydantic.model_validator(mode="after")
    def identity(self) -> Self:
        if self.kind == "workflow" and (not self.workflow_id or not self.step_id):
            raise ValueError("workflow provenance requires workflow and step IDs")
        if self.kind == "user" and any(
            (self.execution_id, self.workflow_id, self.step_id)
        ):
            raise ValueError("user provenance cannot contain workflow identity")
        return self


class Repository(Model):
    root: str
    identity: str


class Inspection(Model):
    head: str
    branch: Optional[str] = None
    dirty: bool
    inspected_at: str = pydantic.Field(default_factory=timestamp)


class Record(Model):
    id: UUID
    name: str
    description: str = ""
    path: str
    repository: Repository
    branch: str
    base_commit: str
    origin: Origin
    status: Literal["creating", "ready", "deleting", "deleted", "failed", "missing"]
    created_at: str = pydantic.Field(default_factory=timestamp)
    updated_at: str = pydantic.Field(default_factory=timestamp)
    last_used_at: Optional[str] = None
    inspection: Optional[Inspection] = None
    error: Optional[str] = None


class Page(Model):
    worktrees: list[Record]
    next_after: Optional[UUID] = None
