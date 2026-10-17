"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from functools import partial
import shutil
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models, usage
from vectorops.runner import diagnostics
from vectorops.runner import tool_authorization, tool_authorization_store, tool_policy
from vectorops.runner.references import WorkflowReference
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.lifecycle import close_processes
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.execution_index import ExecutionIndex
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner import events
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.retention import ExecutionRetention
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner import state
from vectorops.runner.runner import execution_state as execution_models
from vectorops.runner.runner import inspection, summary_history
from vectorops.runner.runner import history_query
from vectorops.runner.runner import mailbox
from vectorops.runner.runner import inbox
from vectorops.runner.runner import branching
from vectorops.runner.runner.pause import ExecutionPause
from vectorops.runner.runner.llm import RunnerLLM
from vectorops.runner.runner.naming import WorkflowNaming
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.resources import ExecutionResources, finish_cleanup
from vectorops.runner.runner.scheduler import Scheduler


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
        workspace_directory: typing.Optional[str] = None,
        change_sink: typing.Optional[events.ExecutionChangeSink] = None,
        tool_policy: typing.Optional[tool_policy.ToolPolicy] = None,
    ) -> None:
        self.path = Path(path)
        self.workspace_directory = workspace_directory
        self.history = history
        self.event_sink = event_sink
        self.change_sink = change_sink
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
        self._execution_index: typing.Optional[ExecutionIndex] = None
        self._naming: typing.Optional[WorkflowNaming] = None
        self._usage_ready = False
        self._usage_lock = asyncio.Lock()
        self._scheduler = Scheduler()
        self._tool_policy = tool_policy

    async def project(self) -> RunnerProject:
        async with self._project_lock:
            if self._project is None:
                self._project = await RunnerProject.open(
                    self.path, workspace_directory=self.workspace_directory
                )
            return self._project

    async def execution_index(self) -> ExecutionIndex:
        project = await self.project()
        if self._execution_index is None:
            self._execution_index = ExecutionIndex(
                project.data_path, self._execution_projection, database=project.database
            )
        return self._execution_index

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

    async def aclose(self) -> None:
        """Stop executions and join background naming before releasing resources."""
        self.stop()
        await asyncio.gather(*self._executions, return_exceptions=True)
        if self._naming is not None:
            await self._naming.aclose()

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
            async with RunnerStateStore(project.data_path, project.database) as store:
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
                            and record.status in ("running", "paused")
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

    def request_suspend(self, execution_id: str) -> None:
        """Preserve execution state when interrupting work for server shutdown."""
        self._suspending.add(execution_id)
        owner = self._mailboxes.get(execution_id)
        if owner is not None:
            owner.suspending = True
            if owner.pause is not None:
                owner.pause.frozen = True

    def suspend_inputs(self, execution_id: typing.Optional[str] = None) -> bool:
        """Preserve durable waits when shutting down, unlike an explicit stop."""
        if execution_id is not None:
            owner = self._mailboxes.get(execution_id)
            waiting = owner is not None and bool(owner.waiting)
            if waiting:
                self.request_suspend(execution_id)
            return waiting
        waiting = any(owner.waiting for owner in self._mailboxes.values())
        if waiting:
            for identifier in self._mailboxes:
                self.request_suspend(identifier)
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
            request = await owner.answer(answer)
        else:
            project = await self.project()
            async with self._writer(project):
                database = await self._database(project, answer.execution_id)
                request = await mailbox.Mailbox(database, self.mailbox_sink).answer(
                    answer
                )
        if request.request_type == "chat.message.v1":
            await self._schedule_naming(answer.execution_id, request.workflow_id)
        return request

    async def _schedule_naming(self, execution_id: str, workflow_id: str) -> None:
        project = await self.project()
        database = await self._database(project, execution_id)
        (await self._workflow_naming()).schedule(database, workflow_id)

    async def _workflow_naming(self) -> WorkflowNaming:
        if self._naming is None:
            project = await self.project()
            self._naming = WorkflowNaming(
                RunnerLLM(project.configuration, self.authentication)
            )
        return self._naming

    def inboxes(self, execution_id: str) -> inbox.Registry:
        owner = self._mailboxes.get(execution_id)
        if (
            owner is None
            or owner.inboxes is None
            or owner.suspending
            or execution_id in self._stop_requests
        ):
            raise inbox.InboxConflict("execution inboxes are not active")
        return owner.inboxes

    def list_mailboxes(self, execution_id: str) -> inbox.Listing:
        return self.inboxes(execution_id).listing()

    def inspect_mailbox(self, address: inbox.Address) -> inbox.Status:
        return self.inboxes(address.execution_id).get(address).inspect()

    def send_message(self, request: inbox.Send) -> inbox.Message:
        return self.inboxes(request.execution_id).get(request).send(request.payload)

    def list_messages(self, query: inbox.MessageQuery) -> inbox.MessagePage:
        return self.inboxes(query.execution_id).get(query).messages(query)

    def remove_message(
        self, reference: inbox.MessageRef
    ) -> typing.Optional[inbox.Message]:
        return (
            self.inboxes(reference.execution_id)
            .get(reference)
            .remove(reference.message_id)
        )

    def pop_message(self, address: inbox.Address) -> typing.Optional[inbox.Message]:
        return self.inboxes(address.execution_id).get(address).pop()

    async def accept_branch(self, request: branching.Branch) -> branching.Operation:
        project = await self.project()
        async with self._writer(project):
            database = await self._database(project, request.execution_id)
            return await database.accept_branch(request)

    async def stop_unloaded(self, execution_id: str) -> None:
        project = await self.project()
        async with self._writer(project) as store:
            async with store.reserve(execution_id):
                database = await self._database(project, execution_id)
                await database.stop_execution()
                await store.deactivate(execution_id)

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
            await self.initialize_usage()
            is_new = execution_id is None
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
                    branch = await database.apply_branch()
                    if branch is not None:
                        if self.mailbox_sink is not None:
                            self.mailbox_sink(
                                {
                                    "kind": "execution.branch_" + branch.status,
                                    "execution_id": execution_id,
                                    "operation_id": branch.id,
                                    "input_step_id": branch.input_step_id,
                                    "error": branch.error,
                                }
                            )
                        if branch.status == "failed":
                            raise branching.BranchConflict(
                                branch.error or "branch preparation failed"
                            )
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
                async with (
                    SQLiteHistory(
                        history_path,
                        usage_store=project.usage,
                        summary_store=project.summaries,
                        event_sink=self.event_sink,
                        mailbox_sink=self.mailbox_sink,
                        change_sink=(
                            partial(self.change_sink, execution_id)
                            if self.change_sink is not None
                            else None
                        ),
                    ) as database,
                    CachedHistory(
                        database,
                        write_through=self._state_store is not None,
                        cache_workflows=False,
                    ) as history,
                ):
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
                    self.last_execution_id = execution_id
                    await checkpointer.persist()
                    await store.activate(execution_id)

                    def changed(status: str) -> None:
                        if self.mailbox_sink is not None:
                            self.mailbox_sink(
                                {
                                    "kind": "execution." + status,
                                    "execution_id": execution_id,
                                }
                            )

                    pause = ExecutionPause(database, checkpointer.persist, changed)
                    queues = inbox.Registry(
                        execution_id, record.root_workflow_id, self.mailbox_sink
                    )
                    owner = mailbox.Mailbox(
                        database, self.mailbox_sink, pause, inboxes=queues
                    )
                    self._mailboxes[execution_id] = owner
                    if execution_id in self._suspending:
                        self.request_suspend(execution_id)
                    try:
                        if on_started is not None:
                            on_started()
                        if not is_new:
                            await self._schedule_naming(
                                execution_id, record.root_workflow_id
                            )
                        if (
                            execution_id in self._stop_requests
                            or execution_id in self._suspending
                        ):
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
                                tool_grants=database,
                            )
                    except asyncio.CancelledError:
                        if owner.suspending:
                            await checkpointer.persist()
                        else:
                            await database.cancel_inputs()
                            await checkpointer.persist("cancelled")
                        raise
                    except Exception as exc:
                        current = await database.get_execution()
                        head = current.checkpoint.heads.get(record.root_workflow_id)
                        terminal = (
                            await history.get_step(head) if head is not None else None
                        )
                        if (
                            terminal is None
                            or terminal.operation.kind != "workflow.complete"
                            or terminal.outcome.kind != "failed"
                        ):
                            await WorkflowExecutor(
                                project,
                                history,
                                checkpointer,
                                execution_id,
                                self.event_sink,
                            ).fail(configuration, exc, head)
                        await database.cancel_inputs()
                        await checkpointer.persist("failed")
                        raise
                    finally:
                        queues.close()
                        self._mailboxes.pop(execution_id, None)
                    await database.cancel_inputs()
                    await checkpointer.persist("completed")
                    await history.flush()
                    async with store.coordination:
                        pointer = await store.load()
                        removed = await ExecutionRetention(
                            project.data_path, project.config.retention
                        ).apply(
                            pointer.last_execution_id,
                            protected_ids=store.reserved
                            | store.scheduled
                            | set(pointer.active_execution_ids),
                        )
                        if removed:
                            await project.summaries.remove(removed)
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
        tool_grants: typing.Optional[tool_authorization_store.WorkflowGrants] = None,
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
        elif execution_id is not None:
            assert workflow_id is not None
            configuration = await history.get_workflow(workflow_id)
        else:
            configuration = models.Workflow(
                id=workflow_id if workflow_id is not None else uuid4().hex,
                definition_ref=reference,
                input={} if input is None else input,
            )
        authorization = tool_authorization.ToolAuthorizationService(
            tool_authorization_store.AuthorizationStore(
                project.configuration, tool_grants
            ),
            self._tool_policy,
        )
        self._tool_policy = authorization.policy
        with authorization.activate():
            return await self._run_configured(
                project,
                history,
                configuration,
                continue_from_step_id=continue_from_step_id,
                child_heads=child_heads,
                reporter=reporter,
                execution_id=execution_id,
                mailbox_owner=mailbox_owner,
            )

    async def _run_configured(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
        reporter: typing.Optional[ExecutionCheckpointer],
        execution_id: typing.Optional[str],
        mailbox_owner: typing.Optional[mailbox.Mailbox],
    ) -> pydantic.JsonValue:
        async with ExecutionResources() as resources:
            mcp = MCPRuntime(self.authentication, execution_id, pool=self.mcp_pool)
            resources.register("mcp", mcp.close)
            environment = None
            if project.config.environment is not None:
                environment = await LocalEnvironment.open(project.config.environment)
                resources.register(
                    "processes", partial(close_processes, environment.processes)
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
                scheduler=self._scheduler,
            )
            if continue_from_step_id is None and execution_id is None:
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
        return await RunnerStateStore(project.data_path, project.database).load()

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
        return SQLiteHistory(
            path,
            usage_store=project.usage,
            summary_store=project.summaries,
            event_sink=self.event_sink,
            mailbox_sink=self.mailbox_sink,
            change_sink=(
                partial(self.change_sink, execution_id)
                if self.change_sink is not None
                else None
            ),
        )

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
            execution_id = (await self.project_state()).last_execution_id
        if execution_id is None:
            raise ValueError("no last execution is recorded")
        return await self._database(project, execution_id)

    async def inspect_execution(
        self, execution_id: typing.Optional[str] = None
    ) -> inspection.ExecutionStatus:
        return await (await self._selected_database(execution_id)).inspect_execution()

    async def execution_summary(self, execution_id: str) -> inspection.ExecutionSummary:
        project = await self.project()
        try:
            return await project.summaries.get(execution_id)
        except KeyError:
            snapshot = await self._execution_projection(execution_id)
            await project.summaries.record(snapshot)
            return snapshot.summary

    async def _execution_projection(
        self, execution_id: str
    ) -> summary_history.Snapshot:
        snapshot = await (
            await self._selected_database(execution_id)
        ).summary_snapshot()
        if snapshot.summary.execution_id != execution_id:
            raise ValueError("execution identity mismatch")
        return snapshot

    async def execution_summaries(
        self, execution_ids: Sequence[str]
    ) -> dict[str, inspection.ExecutionSummary]:
        return await (await self.project()).summaries.get_many(execution_ids)

    async def workflow_state(
        self, execution_id: str, workflow_id: str
    ) -> models.Workflow:
        return await (await self._selected_database(execution_id)).get_workflow(
            workflow_id
        )

    async def initialize_usage(self) -> None:
        """Reconcile retained histories once before accepting work or reading counters."""
        async with self._usage_lock:
            if self._usage_ready:
                return
            project = await self.project()
            identifiers = await (await self.execution_index()).discover()
            async with project.usage.lock:
                for identifier in identifiers:
                    database = await self._database(project, identifier)
                    revision, total = await database.usage_snapshot()
                    await project.usage.record(identifier, revision, total)
            await (await self.execution_index()).reconcile()
            self._usage_ready = True

    async def runner_usage(self, *, reset: bool = False) -> usage.RunnerUsage:
        await self.initialize_usage()
        project = await self.project()
        async with project.usage.lock:
            return await project.usage.read(reset=reset)

    async def query_history(
        self, query: history_query.HistoryQuery
    ) -> history_query.HistoryPage:
        return await (await self._selected_database(query.execution_id)).query(query)

    async def execution_ids(
        self,
        *,
        after: typing.Optional[str] = None,
        limit: typing.Optional[int] = 100,
        active: Sequence[str] = (),
        active_only: bool = False,
    ) -> list[str]:
        """Page newest-updated executions, breaking timestamp ties by ID.

        Unindexed active IDs precede archives in reverse activation order.
        ID cursors resume in the current ordering rather than a frozen snapshot.
        """
        return await (await self.execution_index()).identifiers(
            after=after, limit=limit, active=active, active_only=active_only
        )

    async def export_execution(
        self, output: Path, execution_id: typing.Optional[str] = None
    ) -> None:
        await (await self._selected_database(execution_id)).export_execution(output)

    async def recovery_candidates(self) -> list[execution_models.ExecutionRecord]:
        pointer = await self.project_state()
        identifiers = set(pointer.active_execution_ids)
        if pointer.last_execution_id is not None:
            identifiers.add(pointer.last_execution_id)
        identifiers.update(await (await self.execution_index()).discover())
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
        candidates = []
        for record in records:
            database = await self._database(await self.project(), record.id)
            if record.status == "running" or await database.pending_branch():
                candidates.append(record)
        return candidates

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
        await self.initialize_usage()
        async with RunnerStateStore(project.data_path, project.database) as store:
            database = await self._database(project, execution_id)
            record = await database.get_execution()
            if record.status in ("running", "paused"):
                raise ValueError("active execution history cannot be removed")
            async with project.usage.lock:
                revision, total = await database.usage_snapshot()
                await project.usage.record(execution_id, revision, total)
            pointer = await store.load()
            if pointer.last_execution_id == execution_id:
                await store.save(pointer.model_copy(update={"last_execution_id": None}))
            await store.deactivate(execution_id)
            await asyncio.to_thread(shutil.rmtree, database.path.parent)
            await project.summaries.remove([execution_id])
