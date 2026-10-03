"""Public execution inspection and JSONL export schemas."""

import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import execution_state


class ExecutionStats(pydantic.BaseModel):
    workflows: int
    steps: int
    operations: dict[str, int] = pydantic.Field(default_factory=dict)
    outcomes: dict[str, int] = pydantic.Field(default_factory=dict)


class ExecutionStatus(pydantic.BaseModel):
    execution: execution_state.ExecutionRecord
    stats: ExecutionStats


class ExportHeader(pydantic.BaseModel):
    kind: typing.Literal["execution.header"] = "execution.header"
    version: typing.Literal[1] = 1
    execution: execution_state.ExecutionRecord
    workflows: list[models.Workflow]
