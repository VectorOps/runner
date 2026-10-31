"""Executor step snapshots for future UI and transport listeners."""

from collections.abc import Callable
from dataclasses import dataclass
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import execution_state, inspection


class StepEvent(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal[
        "step.created", "step.updated", "step.completed", "step.failed"
    ]
    execution_id: typing.Optional[str] = None
    step: models.WorkflowStep
    position: typing.Optional[int] = None


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


class ExecutionStateUpdated(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal["execution.state_updated"] = "execution.state_updated"
    execution_id: str
    execution: execution_state.ExecutionRecord


HistoryChangeSink: typing.TypeAlias = Callable[[tuple[str, ...]], None]
ExecutionChangeSink: typing.TypeAlias = Callable[[str, tuple[str, ...]], None]


@dataclass(frozen=True)
class Publication:
    """Internal invalidation categories; unrelated snapshots need not be loaded."""

    workflows: tuple[str, ...] = ()
    summary: bool = False
    execution: bool = False


PublicationSink: typing.TypeAlias = Callable[[Publication], None]
ExecutionPublicationSink: typing.TypeAlias = Callable[[str, Publication], None]
