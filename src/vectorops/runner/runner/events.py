"""Executor step snapshots for future UI and transport listeners."""

from collections.abc import Callable
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import inspection


class StepEvent(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal[
        "step.created", "step.updated", "step.completed", "step.failed"
    ]
    execution_id: typing.Optional[str] = None
    step: models.WorkflowStep


StepEventSink: typing.TypeAlias = Callable[[StepEvent], None]


class WorkflowUpdated(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal["workflow.updated"] = "workflow.updated"
    execution_id: str
    workflow: models.Workflow


class ExecutionUpdated(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal["execution.updated"] = "execution.updated"
    execution_id: str
    summary: inspection.ExecutionSummary


HistoryChangeSink: typing.TypeAlias = Callable[[tuple[str, ...]], None]
ExecutionChangeSink: typing.TypeAlias = Callable[[str, tuple[str, ...]], None]
