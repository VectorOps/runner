"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

import asyncio
from pathlib import Path
import shutil
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.retention import ExecutionRetention
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner import state
from vectorops.runner.runner import execution_state as execution_models
from vectorops.runner.runner.state_store import RunnerStateStore


class WorkflowRunner:
    def __init__(
        self,
        path: typing.Union[str, Path],
        history: typing.Optional[WorkflowHistory] = None,
    ) -> None:
        self.path = Path(path)
        self.history = history
        self.last_execution_id: typing.Optional[str] = None

    async def run(
        self,
        workflow_name: str,
        callable_name: str = "workflow",
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        workflow_id: typing.Optional[str] = None,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[dict[str, str]] = None,
        execution_id: typing.Optional[str] = None,
    ) -> pydantic.JsonValue:
        """Replay through a selected step, then append new execution history."""
        project = await RunnerProject.open(self.path)
        if self.history is None:
            async with RunnerStateStore(project.data_path) as store:
                if execution_id is None:
                    if continue_from_step_id is not None:
                        raise ValueError(
                            "execution ID is required for persistent continuation"
                        )
                    execution_id = uuid4().hex
                    root_id = workflow_id if workflow_id is not None else uuid4().hex
                    record = execution_models.ExecutionRecord(
                        id=execution_id,
                        root_workflow_id=root_id,
                    )
                    configuration = models.Workflow(
                        id=root_id,
                        definition_ref=f"{workflow_name}:{callable_name}",
                        input={} if input is None else input,
                    )
                else:
                    database = await self._database(project, execution_id)
                    record = await database.get_execution()
                    if (
                        workflow_id is not None
                        and workflow_id != record.root_workflow_id
                    ):
                        raise ValueError(
                            "workflow ID differs from execution configuration"
                        )
                    workflow_id = record.root_workflow_id
                    configuration = None
                self.last_execution_id = execution_id
                history_path = (
                    project.data_path / "executions" / execution_id / "history.sqlite3"
                )
                resolved = await asyncio.to_thread(history_path.resolve)
                if not resolved.is_relative_to(project.data_path):
                    raise ValueError("execution history escapes project data")
                if configuration is None and not await asyncio.to_thread(
                    history_path.is_file
                ):
                    raise FileNotFoundError("execution history is missing")
                async with CachedHistory(SQLiteHistory(history_path)) as history:
                    database = SQLiteHistory(history_path)
                    if configuration is not None:
                        await history.save_workflow(configuration)
                    else:
                        configuration = await history.get_workflow(
                            record.root_workflow_id
                        )
                        if (
                            configuration.definition_ref
                            != f"{workflow_name}:{callable_name}"
                        ):
                            raise ValueError(
                                "workflow target differs from execution configuration"
                            )
                        if input is not None and input != configuration.input:
                            raise ValueError(
                                "workflow input differs from execution configuration"
                            )
                    heads = dict(record.checkpoint.heads)
                    if child_heads is not None:
                        heads.update(child_heads)
                    root_head = (
                        continue_from_step_id
                        if continue_from_step_id is not None
                        else heads.get(record.root_workflow_id)
                    )
                    if root_head is not None:
                        selected = await history.get_step(root_head)
                        if selected.workflow_id != record.root_workflow_id:
                            raise ValueError(
                                "continuation must target the root workflow"
                            )
                        heads[record.root_workflow_id] = root_head
                    record = record.model_copy(
                        update={
                            "status": "running",
                            "checkpoint": execution_models.ExecutionCheckpoint(
                                heads=heads
                            ),
                            "completion_step_id": None,
                        }
                    )
                    checkpointer = ExecutionCheckpointer(database, history, record)
                    await database.save_execution(record, replace_heads=True)
                    await checkpointer.persist()
                    await store.save(state.RunnerState(last_execution_id=execution_id))
                    try:
                        async with checkpointer.tracking():
                            result = await self._run(
                                project,
                                history,
                                workflow_name,
                                callable_name,
                                input=configuration.input,
                                workflow_id=record.root_workflow_id,
                                continue_from_step_id=root_head,
                                child_heads=heads,
                                reporter=checkpointer,
                            )
                    except asyncio.CancelledError:
                        await checkpointer.persist("cancelled")
                        raise
                    except Exception:
                        await checkpointer.persist("failed")
                        raise
                    await checkpointer.persist("completed")
                    await history.flush()
                    await ExecutionRetention(
                        project.data_path, project.config.retention
                    ).apply((await store.load()).last_execution_id)
                    return result
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
        reporter: typing.Optional[ExecutionCheckpointer] = None,
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
        completion = await WorkflowExecutor(project, history, reporter).execute(
            configuration,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
        )
        if completion.outcome.kind == "failed":
            raise ActivityError(completion.outcome.error)
        if completion.outcome.kind != "completed":
            raise RuntimeError("executor returned an invalid workflow completion")
        return completion.outcome.result

    async def project_state(self) -> state.RunnerState:
        project = await RunnerProject.open(self.path)
        return await RunnerStateStore(project.data_path).load()

    async def _database(
        self, project: RunnerProject, execution_id: str
    ) -> SQLiteHistory:
        state.RunnerState(last_execution_id=execution_id)
        path = project.data_path / "executions" / execution_id / "history.sqlite3"
        resolved = await asyncio.to_thread(path.resolve)
        if not resolved.is_relative_to(project.data_path):
            raise ValueError("execution history escapes project data")
        if not await asyncio.to_thread(path.is_file):
            raise FileNotFoundError("execution history is missing")
        return SQLiteHistory(path)

    async def execution_state(
        self, execution_id: str
    ) -> execution_models.ExecutionRecord:
        project = await RunnerProject.open(self.path)
        return await (await self._database(project, execution_id)).get_execution()

    async def recovery_candidates(self) -> list[execution_models.ExecutionRecord]:
        pointer = await self.project_state()
        if pointer.last_execution_id is None:
            return []
        record = await self.execution_state(pointer.last_execution_id)
        return [record] if record.status == "running" else []

    async def resume(
        self, execution_id: typing.Optional[str] = None
    ) -> pydantic.JsonValue:
        project = await RunnerProject.open(self.path)
        if execution_id is None:
            execution_id = (await self.project_state()).last_execution_id
        if execution_id is None:
            raise ValueError("no last execution is recorded")
        database = await self._database(project, execution_id)
        record = await database.get_execution()
        configuration = await database.get_workflow(record.root_workflow_id)
        workflow_name, _, callable_name = configuration.definition_ref.rpartition(":")
        return await self.run(workflow_name, callable_name, execution_id=execution_id)

    async def remove_execution(self, execution_id: str) -> None:
        project = await RunnerProject.open(self.path)
        async with RunnerStateStore(project.data_path) as store:
            database = await self._database(project, execution_id)
            record = await database.get_execution()
            if record.status == "running":
                raise ValueError("active execution history cannot be removed")
            pointer = await store.load()
            if pointer.last_execution_id == execution_id:
                await store.save(state.RunnerState())
            await asyncio.to_thread(shutil.rmtree, database.path.parent)
