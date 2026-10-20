"""Bounded history windows and opaque, query-bound keyset cursors."""

import base64
import typing

import pydantic

from vectorops.runner import models


class HistoryQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$")
    selection: typing.Literal["tail", "head"] = "tail"
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)
    workflow_id: typing.Optional[str] = None
    include_children: bool = True
    step_ids: typing.Optional[list[str]] = pydantic.Field(default=None, max_length=500)
    head_step_id: typing.Optional[str] = None
    before_step_id: typing.Optional[str] = None
    cursor: typing.Optional[str] = pydantic.Field(default=None, max_length=4096)

    def scope(self) -> str:
        excluded = {"cursor", "limit"}
        if self.before_step_id is None:
            excluded.add("before_step_id")
        return self.model_dump_json(exclude=excluded)


class HistoryItem(pydantic.BaseModel):
    position: int
    step: models.WorkflowStep


class HistoryPage(pydantic.BaseModel):
    execution_id: str
    items: list[HistoryItem]
    next_cursor: typing.Optional[str] = None


class HistoryCursor(pydantic.BaseModel):
    scope: str
    boundary: int = pydantic.Field(ge=0)
    position: int = pydantic.Field(ge=0)

    def encode(self) -> str:
        return base64.urlsafe_b64encode(self.model_dump_json().encode()).decode()

    @classmethod
    def decode(cls, value: str, scope: str) -> typing.Self:
        try:
            cursor = cls.model_validate_json(
                base64.b64decode(value, altchars=b"-_", validate=True)
            )
        except ValueError as exc:
            raise ValueError("invalid history cursor") from exc
        if cursor.scope != scope:
            raise ValueError("history cursor belongs to another query")
        return cursor
