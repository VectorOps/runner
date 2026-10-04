"""Runner-owned execution discovery, separate from per-execution history."""

from __future__ import annotations

import typing

import pydantic


class RunnerState(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    schema_version: typing.Literal[1] = 1
    last_execution_id: typing.Optional[str] = pydantic.Field(
        default=None, min_length=1, pattern=r"^[A-Za-z0-9_-]+$"
    )
    active_execution_ids: list[
        typing.Annotated[str, pydantic.Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")]
    ] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("active_execution_ids")
    @classmethod
    def unique_ids(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("active execution IDs must be unique")
        return values
