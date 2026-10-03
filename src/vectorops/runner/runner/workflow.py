"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from pathlib import Path
import shutil
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner import diagnostics
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.retention import ExecutionRetention
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner import state
from vectorops.runner.runner import execution_state as execution_models
from vectorops.runner.runner import inspection
from vectorops.runner.runner import history_query
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.state_store import RunnerStateStore


class WorkflowRunner:
    def __init__(
        self,
        path: typing.Union[str, Path],
        history: typing.Optional[WorkflowHistory] = None,
        *,
        event_sink: typing.Optional[StepEventSink] = None,
        state_store: typing.Optional[RunnerStateStore] = None,
        mailbox_sink: typing.Optional[mailbox.EventSink] = None,
        diagnostic_log: typing.Optional[diagnostics.DiagnosticLog] = None,
    ) -> None:
        self.path = Path(path)
        self.history = history
        self.event_sink = event_sink
        self._state_store = state_store
        self._diagnostic_log = diagnostic_log
        self.mailbox_sink = mailbox_sink
        self._mailboxes: dict[str, mailbox.Mailbox] = {}
        self.last_execution_id: typing.Optional[str] = None
        self._executions: set[asyncio.Task[pydantic.JsonValue]] = set()

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
        new_execution_id: typing.Optional[str] = None,
    ) -> pydantic.JsonValue:
        """Replay through a selected step, then append new execution history."""
        task = asyncio.create_task(
            self._execute(
                workflow_name,
                callable_name,
                input=input,
                workflow_id=workflow_id,
                continue_from_step_id=continue_from_step_id,
                child_heads=child_heads,
                execution_id=execution_id,
                new_execution_id=new_execution_id,
            )
        )
        self._executions.add(task)
        try:
            return await task
        finally:
            self._executions.discard(task)

    def stop(self) -> None:
        """Cancel active executions without waiting for cascading cleanup."""
        for task in tuple(self._executions):
            if not task.done() and not task.cancelling():
                task.cancel()

    @asynccontextmanager
    async def _writer(self, project: RunnerProject) -> AsyncIterator[RunnerStateStore]:
        if self._state_store is not None:
            if self._diagnostic_log is None:
                yield self._state_store
            else:
                with self._diagnostic_log.activate():
                    yield self._state_store
        else:
            async with RunnerStateStore(project.data_path) as store:
                async with diagnostics.DiagnosticLog(
                    project.data_path, project.config.logging
                ) as logs:
                    with logs.activate():
                        yield store

    def suspend_inputs(self) -> bool:
        """Preserve durable waits when shutting down, unlike an explicit stop."""
        waiting = any(owner.waiting for owner in self._mailboxes.values())
        if waiting:
            for owner in self._mailboxes.values():
                owner.suspending = True
            self.stop()
        return waiting

    async def pending_inputs(self, query: mailbox.RequestQuery) -> mailbox.RequestPage:
        return await (await self._selected_database(query.execution_id)).list_inputs(
            query
        )

    async def input_request(
        self, reference: mailbox.RequestRef
    ) -> mailbox.InputRequest:
        return await (await self._selected_database(reference.execution_id)).get_input(
            reference.request_id
        )

    async def answer_input(self, answer: mailbox.Answer) -> mailbox.InputRequest:
        owner = self._mailboxes.get(answer.execution_id)
        if owner is not None:
            return await owner.answer(answer)
        project = await RunnerProject.open(self.path)
        async with self._writer(project):
            database = await self._database(project, answer.execution_id)
            return await mailbox.Mailbox(database, self.mailbox_sink).answer(answer)

    async def _execute(
        self,
        workflow_name: str,
        callable_name: str,
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]],
        workflow_id: typing.Optional[str],
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
        execution_id: typing.Optional[str],
        new_execution_id: typing.Optional[str],
    ) -> pydantic.JsonValue:
        project = await RunnerProject.open(self.path)
        if new_execution_id is not None and (
            execution_id is not None or self.history is not None
        ):
            raise ValueError("new execution ID requires a new persistent execution")
        if self.history is None:
            async with self._writer(project) as store:
                if execution_id is None:
                    if continue_from_step_id is not None:
                        raise ValueError(
                            "execution ID is required for persistent continuation"
                        )
                    execution_id = (
                        uuid4().hex if new_execution_id is None else new_execution_id
                    )
                    root_id = workflow_id if workflow_id is not None else uuid4().hex
                    record = execution_models.ExecutionRecord(
                        id=execution_id,
                        root_workflow_id=root_id,
                    )
                    if await asyncio.to_thread(
                        (project.data_path / "executions" / execution_id).exists
                    ):
                        raise FileExistsError("execution already exists")
                    configuration = models.Workflow(
                        id=root_id,
                        definition_ref=f"{workflow_name}:{callable_name}",
                        input={} if input is None else input,
                    )
                    configuration = await WorkflowExecutor(
                        project,
                        SQLiteHistory(
                            project.data_path
                            / "executions"
                            / execution_id
                            / "history.sqlite3"
                        ),
                    ).configure(configuration)
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
                async with CachedHistory(
                    SQLiteHistory(history_path),
                    write_through=self._state_store is not None,
                ) as history:
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
                        if root_head != record.checkpoint.heads.get(
                            record.root_workflow_id
                        ):
                            pending = await database.list_inputs(
                                mailbox.RequestQuery(execution_id=execution_id, limit=1)
                            )
                            if pending.requests:
                                raise ValueError(
                                    "cannot rewind an execution with pending input requests"
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
                    owner = mailbox.Mailbox(database, self.mailbox_sink)
                    self._mailboxes[execution_id] = owner
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
                                execution_id=execution_id,
                                mailbox_owner=owner,
                            )
                    except asyncio.CancelledError:
                        if owner.suspending:
                            await checkpointer.persist("running")
                        else:
                            await database.cancel_inputs()
                            await checkpointer.persist("cancelled")
                        raise
                    except Exception:
                        await database.cancel_inputs()
                        await checkpointer.persist("failed")
                        raise
                    finally:
                        self._mailboxes.pop(execution_id, None)
                    await database.cancel_inputs()
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
        execution_id: typing.Optional[str] = None,
        mailbox_owner: typing.Optional[mailbox.Mailbox] = None,
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
        executor = WorkflowExecutor(
            project, history, reporter, execution_id, self.event_sink, mailbox_owner
        )
        if continue_from_step_id is None:
            configuration = await executor.configure(configuration)
        completion = await executor.execute(
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

    async def _selected_database(
        self, execution_id: typing.Optional[str]
    ) -> SQLiteHistory:
        project = await RunnerProject.open(self.path)
        if execution_id is None:
            execution_id = (
                await RunnerStateStore(project.data_path).load()
            ).last_execution_id
        if execution_id is None:
            raise ValueError("no last execution is recorded")
        return await self._database(project, execution_id)

    async def inspect_execution(
        self, execution_id: typing.Optional[str] = None
    ) -> inspection.ExecutionStatus:
        return await (await self._selected_database(execution_id)).inspect_execution()

    async def query_history(
        self, query: history_query.HistoryQuery
    ) -> history_query.HistoryPage:
        return await (await self._selected_database(query.execution_id)).query(query)

    async def execution_ids(
        self, *, after: typing.Optional[str] = None, limit: int = 100
    ) -> list[str]:
        project = await RunnerProject.open(self.path)

        def listing() -> list[str]:
            directory = project.data_path / "executions"
            if not directory.exists():
                return []
            return sorted(
                path.name
                for path in directory.iterdir()
                if (path / "history.sqlite3").is_file()
                and (after is None or path.name > after)
            )[:limit]

        return await asyncio.to_thread(listing)

    async def resolve_file(self, filename: str) -> str:
        """Translate a workspace-relative file to the executor's workflow reference."""
        project = await RunnerProject.open(self.path)

        def resolve() -> str:
            relative = Path(filename)
            if relative.is_absolute() or relative.suffix != ".py":
                raise ValueError("file must be a workspace-relative Python filename")
            path = (project.root / relative).resolve()
            if not path.is_relative_to(project.root):
                raise ValueError("file must remain inside the workspace")
            if not path.is_file():
                raise FileNotFoundError(filename)
            return "workspace:" + path.relative_to(project.root).as_posix()

        return await asyncio.to_thread(resolve)

    async def export_execution(
        self, output: Path, execution_id: typing.Optional[str] = None
    ) -> None:
        await (await self._selected_database(execution_id)).export_execution(output)

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
