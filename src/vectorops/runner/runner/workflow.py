"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.project import RunnerProject


class WorkflowRunner:
    def __init__(self, path: typing.Union[str, Path], history: WorkflowHistory) -> None:
        self.path = Path(path)
        self.history = history

    async def run(
        self,
        workflow_name: str,
        callable_name: str = "workflow",
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        workflow_id: typing.Optional[str] = None,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[dict[str, str]] = None,
    ) -> pydantic.JsonValue:
        """Replay through a selected step, then append new execution history."""
        if continue_from_step_id is not None:
            if not continue_from_step_id:
                raise ValueError("continuation step ID must not be empty")
            step = await self.history.get_step(continue_from_step_id)
            configuration = await self.history.get_workflow(step.workflow_id)
            if configuration.definition_ref != f"{workflow_name}:{callable_name}":
                raise ValueError("workflow target differs from recorded configuration")
            if workflow_id is not None and workflow_id != configuration.id:
                raise ValueError("workflow ID differs from recorded configuration")
            if input is not None and input != configuration.input:
                raise ValueError("workflow input differs from recorded configuration")
        else:
            configuration = models.Workflow(
                id=workflow_id if workflow_id is not None else uuid4().hex,
                definition_ref=f"{workflow_name}:{callable_name}",
                input={} if input is None else input,
            )
        project = await RunnerProject.open(self.path)
        completion = await WorkflowExecutor(project, self.history).execute(
            configuration,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
        )
        if completion.outcome.kind == "failed":
            raise ActivityError(completion.outcome.error)
        if completion.outcome.kind != "completed":
            raise RuntimeError("executor returned an invalid workflow completion")
        return completion.outcome.result
