"""Executor step snapshots for future UI and transport listeners."""

from collections.abc import Callable
import typing

import pydantic

from vectorops.runner import models


class StepEvent(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    kind: typing.Literal[
        "step.created", "step.updated", "step.completed", "step.failed"
    ]
    execution_id: typing.Optional[str] = None
    step: models.WorkflowStep


StepEventSink: typing.TypeAlias = Callable[[StepEvent], None]
