"""Minimal runner-owned project pointer."""

from __future__ import annotations

import typing

import pydantic


class RunnerState(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    schema_version: typing.Literal[1] = 1
    last_execution_id: typing.Optional[str] = pydantic.Field(
        default=None, min_length=1, pattern=r"^[A-Za-z0-9_-]+$"
    )
