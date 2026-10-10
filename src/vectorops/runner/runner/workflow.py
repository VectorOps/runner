"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from functools import partial
import shutil
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner import diagnostics
from vectorops.runner.references import WorkflowReference
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment import factory as environments
from vectorops.runner.environment.lifecycle import close_environment
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.pool import SharedPool
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
from vectorops.runner.runner.resources import ExecutionResources, finish_cleanup


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
        authentication: typing.Optional[AuthenticationService] = None,
        mcp_pool: typing.Optional[SharedPool] = None,
        project: typing.Optional[RunnerProject] = None,
    ) -> None:
        self.path = Path(path)
        self.history = history
        self.event_sink = event_sink
        self._state_store = state_store
        self._diagnostic_log = diagnostic_log
        self.authentication = authentication or AuthenticationService()
        self.mcp_pool = mcp_pool
        self.mailbox_sink = mailbox_sink
        self._mailboxes: dict[str, mailbox.Mailbox] = {}
        self._suspending: set[str] = set()
        self._stop_requests: set[str] = set()
        self.last_execution_id: typing.Optional[str] = None
        self._executions: set[asyncio.Task[pydantic.JsonValue]] = set()
        self._project = project
        self._project_lock = asyncio.Lock()

    async def project(self) -> RunnerProject:
        async with self._project_lock:
            if self._project is None:
                self._project = await RunnerProject.open(self.path)
            return self._project

    async def run(
        self,
        reference: str,
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        workflow_id: typing.Optional[str] = None,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[dict[str, str]] = None,
        execution_id: typing.Optional[str] = None,
        new_execution_id: typing.Optional[str] = None,
        on_started: typing.Optional[Callable[[], None]] = None,
    ) -> pydantic.JsonValue:
        """Replay through a selected step, then append new execution history."""
        task = asyncio.create_task(
            self._execute(
                str(WorkflowReference.parse(reference)),
                input=input,
                workflow_id=workflow_id,
                continue_from_step_id=continue_from_step_id,
                child_heads=child_heads,
                execution_id=execution_id,
                new_execution_id=new_execution_id,
                on_started=on_started,
            )
        )
        self._executions.add(task)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done() and not task.cancelling():
                task.cancel()
            try:
                await finish_cleanup(task)
            except (Exception, asyncio.CancelledError):
                pass
            raise
        finally:
            self._executions.discard(task)

    def stop(self) -> None:
        """Cancel active executions without waiting for cascading cleanup."""
        for task in tuple(self._executions):
            if not task.done() and not task.cancelling():
                task.cancel()

    def request_stop(self, execution_id: str) -> None:
        self._stop_requests.add(execution_id)

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

    @asynccontextmanager
    async def _execution_writer(
        self, project: RunnerProject, execution_id: str
    ) -> AsyncIterator[RunnerStateStore]:
        async with self._writer(project) as store:
            async with store.reserve(execution_id):
                cancelled = False
                try:
                    yield store
                except asyncio.CancelledError:
                    cancelled = True
                    raise
                finally:

                    async def settle() -> None:
                        try:
                            record = await (
                                await self._database(project, execution_id)
                            ).get_execution()
                        except (FileNotFoundError, KeyError):
                            return
                        if (
                            cancelled
                            and execution_id not in self._suspending
                            and record.status == "running"
                        ):
                            database = await self._database(project, execution_id)
                            await database.cancel_inputs()
                            await database.save_execution(
                                record.model_copy(update={"status": "cancelled"})
                            )
                            await store.deactivate(execution_id)
                        elif record.status != "running":
                            await store.deactivate(execution_id)

                    await finish_cleanup(asyncio.create_task(settle()))
                    self._suspending.discard(execution_id)
                    self._stop_requests.discard(execution_id)

    def suspend_inputs(self, execution_id: typing.Optional[str] = None) -> bool:
        """Preserve durable waits when shutting down, unlike an explicit stop."""
        if execution_id is not None:
            owner = self._mailboxes.get(execution_id)
            waiting = self.authentication.waiting_for(execution_id) or (
                owner is not None and bool(owner.waiting)
            )
            if waiting and owner is not None:
                owner.suspending = True
                self._suspending.add(execution_id)
            return waiting
        waiting = self.authentication.waiting or any(
            owner.waiting for owner in self._mailboxes.values()
        )
        if waiting:
            for identifier, owner in self._mailboxes.items():
                owner.suspending = True
                self._suspending.add(identifier)
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
        project = await self.project()
        async with self._writer(project):
            database = await self._database(project, answer.execution_id)
            return await mailbox.Mailbox(database, self.mailbox_sink).answer(answer)

    async def _execute(
        self,
        reference: str,
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]],
        workflow_id: typing.Optional[str],
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
        execution_id: typing.Optional[str],
        new_execution_id: typing.Optional[str],
        on_started: typing.Optional[Callable[[], None]],
    ) -> pydantic.JsonValue:
        project = await self.project()
        if new_execution_id is not None and (
            execution_id is not None or self.history is not None
        ):
            raise ValueError("new execution ID requires a new persistent execution")
        if self.history is None:
            identifier = execution_id or new_execution_id or uuid4().hex
            async with self._execution_writer(project, identifier) as store:
                if execution_id is None:
                    if continue_from_step_id is not None:
                        raise ValueError(
                            "execution ID is required for persistent continuation"
                        )
                    execution_id = identifier
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
                        definition_ref=reference,
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
                        if configuration.definition_ref != reference:
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
                    await store.activate(execution_id)
                    owner = mailbox.Mailbox(database, self.mailbox_sink)
                    self._mailboxes[execution_id] = owner
                    try:
                        if on_started is not None:
                            on_started()
                        if execution_id in self._stop_requests:
                            raise asyncio.CancelledError
                        async with checkpointer.tracking():
                            result = await self._run(
                                project,
                                history,
                                reference,
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
                    async with store.coordination:
                        pointer = await store.load()
                        await ExecutionRetention(
                            project.data_path, project.config.retention
                        ).apply(
                            pointer.last_execution_id,
                            protected_ids=store.reserved
                            | store.scheduled
                            | set(pointer.active_execution_ids),
                        )
                    return result
        return await self._run(
            project,
            self.history,
            reference,
            input=input,
            workflow_id=workflow_id,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
        )

    async def _run(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        reference: str,
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
            if configuration.definition_ref != reference:
                raise ValueError("workflow target differs from recorded configuration")
            if workflow_id is not None and workflow_id != configuration.id:
                raise ValueError("workflow ID differs from recorded configuration")
            if input is not None and input != configuration.input:
                raise ValueError("workflow input differs from recorded configuration")
        else:
            configuration = models.Workflow(
                id=workflow_id if workflow_id is not None else uuid4().hex,
                definition_ref=reference,
                input={} if input is None else input,
            )
        async with ExecutionResources() as resources:
            mcp = MCPRuntime(self.authentication, execution_id, pool=self.mcp_pool)
            resources.register("mcp", mcp.close)
            environment = None
            if project.config.environment is not None:
                environment = await environments.open_environment(
                    project.config.environment
                )
                resources.register(
                    "environment", partial(close_environment, environment)
                )
            executor = WorkflowExecutor(
                project,
                history,
                reporter,
                execution_id,
                self.event_sink,
                mailbox_owner,
                self.authentication,
                environment=environment,
                resources=resources,
                mcp=mcp,
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
        project = await self.project()
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
        project = await self.project()
        return await (await self._database(project, execution_id)).get_execution()

    async def _selected_database(
        self, execution_id: typing.Optional[str]
    ) -> SQLiteHistory:
        project = await self.project()
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

    async def execution_summary(self, execution_id: str) -> inspection.ExecutionSummary:
        return await (await self._selected_database(execution_id)).execution_summary()

    async def query_history(
        self, query: history_query.HistoryQuery
    ) -> history_query.HistoryPage:
        return await (await self._selected_database(query.execution_id)).query(query)

    async def execution_ids(
        self, *, after: typing.Optional[str] = None, limit: int = 100
    ) -> list[str]:
        project = await self.project()

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

    async def export_execution(
        self, output: Path, execution_id: typing.Optional[str] = None
    ) -> None:
        await (await self._selected_database(execution_id)).export_execution(output)

    async def recovery_candidates(self) -> list[execution_models.ExecutionRecord]:
        pointer = await self.project_state()
        identifiers = set(pointer.active_execution_ids)
        if pointer.last_execution_id is not None:
            identifiers.add(pointer.last_execution_id)
        after = None
        while page := await self.execution_ids(after=after):
            identifiers.update(page)
            after = page[-1]
        records = []
        for identifier in sorted(identifiers):
            try:
                records.append(await self.execution_state(identifier))
            except KeyError:
                if (
                    identifier in pointer.active_execution_ids
                    or identifier == pointer.last_execution_id
                ):
                    raise
        return [record for record in records if record.status == "running"]

    async def resume(
        self,
        execution_id: typing.Optional[str] = None,
        *,
        on_started: typing.Optional[Callable[[], None]] = None,
    ) -> pydantic.JsonValue:
        project = await self.project()
        if execution_id is None:
            execution_id = (await self.project_state()).last_execution_id
        if execution_id is None:
            raise ValueError("no last execution is recorded")
        database = await self._database(project, execution_id)
        record = await database.get_execution()
        configuration = await database.get_workflow(record.root_workflow_id)
        return await self.run(
            configuration.definition_ref,
            execution_id=execution_id,
            on_started=on_started,
        )

    async def remove_execution(self, execution_id: str) -> None:
        project = await self.project()
        async with RunnerStateStore(project.data_path) as store:
            database = await self._database(project, execution_id)
            record = await database.get_execution()
            if record.status == "running":
                raise ValueError("active execution history cannot be removed")
            pointer = await store.load()
            if pointer.last_execution_id == execution_id:
                await store.save(pointer.model_copy(update={"last_execution_id": None}))
            await store.deactivate(execution_id)
            await asyncio.to_thread(shutil.rmtree, database.path.parent)
