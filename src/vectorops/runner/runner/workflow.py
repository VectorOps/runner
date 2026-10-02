"""Basic workspace runner with an explicit placeholder for history continuation."""

from __future__ import annotations

from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.project import RunnerProject


class WorkflowRunner:
    def __init__(self, path: typing.Union[str, Path]) -> None:
        self.path = Path(path)

    async def run(
        self,
        workflow_name: str,
        callable_name: str = "workflow",
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        workflow_id: typing.Optional[str] = None,
        continue_from_step_id: typing.Optional[str] = None,
    ) -> pydantic.JsonValue:
        """Invoke a fresh workflow; reject continuation until history replay exists."""
        if continue_from_step_id is not None:
            if not continue_from_step_id:
                raise ValueError("continuation step ID must not be empty")
            raise NotImplementedError("continuing from a step requires history replay")
        configuration = models.Workflow(
            id=workflow_id if workflow_id is not None else uuid4().hex,
            definition_ref=f"{workflow_name}:{callable_name}",
            input={} if input is None else input,
        )
        project = await RunnerProject.open(self.path)
        context = WorkflowExecutionContext(project=project, workflow=configuration)
        return await WorkflowExecutor(project).execute(
            workflow_name, callable_name, context
        )
