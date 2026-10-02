"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.sqlite_history import SQLiteHistory


class WorkflowRunner:
    def __init__(
        self,
        path: typing.Union[str, Path],
        history: typing.Optional[WorkflowHistory] = None,
    ) -> None:
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
        project = await RunnerProject.open(self.path)
        if self.history is None:
            async with CachedHistory(
                SQLiteHistory(project.data_path / "history.sqlite3")
            ) as history:
                return await self._run(
                    project,
                    history,
                    workflow_name,
                    callable_name,
                    input=input,
                    workflow_id=workflow_id,
                    continue_from_step_id=continue_from_step_id,
                    child_heads=child_heads,
                )
        return await self._run(
            project,
            self.history,
            workflow_name,
            callable_name,
            input=input,
            workflow_id=workflow_id,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
        )

    async def _run(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        workflow_name: str,
        callable_name: str,
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]],
        workflow_id: typing.Optional[str],
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
    ) -> pydantic.JsonValue:
        if continue_from_step_id is not None:
            if not continue_from_step_id:
                raise ValueError("continuation step ID must not be empty")
            step = await history.get_step(continue_from_step_id)
            configuration = await history.get_workflow(step.workflow_id)
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
        completion = await WorkflowExecutor(project, history).execute(
            configuration,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
        )
        if completion.outcome.kind == "failed":
            raise ActivityError(completion.outcome.error)
        if completion.outcome.kind != "completed":
            raise RuntimeError("executor returned an invalid workflow completion")
        return completion.outcome.result
