"""Public execution inspection and JSONL export schemas."""

import typing

import pydantic

from vectorops.runner import models, usage as usage_models
from vectorops.runner.runner import execution_state


class ExecutionStats(pydantic.BaseModel):
    workflows: int
    steps: int
    operations: dict[str, int] = pydantic.Field(default_factory=dict)
    outcomes: dict[str, int] = pydantic.Field(default_factory=dict)


class ExecutionStatus(pydantic.BaseModel):
    execution: execution_state.ExecutionRecord
    stats: ExecutionStats


class ExecutionSummary(pydantic.BaseModel):
    execution_id: str
    reference: typing.Optional[str] = None
    root_workflow_id: typing.Optional[str] = None
    name: typing.Optional[str] = None
    status: typing.Literal[
        "starting", "running", "paused", "stopping", "completed", "failed", "cancelled"
    ]
    pending_inputs: int = 0
    awaiting_authentication: bool = False
    active: bool = False
    usage: typing.Optional[usage_models.ExecutionUsage] = None


class ExportHeader(pydantic.BaseModel):
    kind: typing.Literal["execution.header"] = "execution.header"
    version: typing.Literal[1] = 1
    execution: execution_state.ExecutionRecord
    workflows: list[models.Workflow]
