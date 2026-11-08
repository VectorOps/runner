"""Workspace runner initialized from configuration or a selected history step."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from functools import partial
import shutil
import typing
from uuid import UUID, uuid4

import pydantic

from vectorops.runner import models, usage, presentation as presentation_models
from vectorops.runner import diagnostics, global_config
from vectorops.runner import tool_authorization, tool_authorization_store, tool_policy
from vectorops.runner import tool_rule_api
from vectorops.runner.references import WorkflowReference
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.worktrees.runtime import ExecutionTargets
from vectorops.runner.worktrees import models as worktree_models
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
from vectorops.runner.runner import disk_usage, inspection, summary_history
from vectorops.runner.runner.summary_runtime import SummaryRuntime
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
        config_dir: typing.Optional[Path] = None,
        change_sink: typing.Optional[events.ExecutionChangeSink] = None,
        publication_sink: typing.Optional[events.ExecutionPublicationSink] = None,
        tool_policy: typing.Optional[tool_policy.ToolPolicy] = None,
    ) -> None:
        self.path = Path(path)
        self.workspace_directory = workspace_directory
        self.config_dir = global_config.directory(config_dir)
        self.history = history
        self.event_sink = event_sink
        self.change_sink = change_sink
        self.publication_sink = publication_sink
        self._state_store = state_store
        self._diagnostic_log = diagnostic_log
        self.authentication = authentication or AuthenticationService(
            global_config.GlobalConfiguration(self.config_dir)
        )
        self.mcp_pool = mcp_pool
        self.mailbox_sink = mailbox_sink
        self._mailboxes: dict[str, mailbox.Mailbox] = {}
        self._summaries: dict[str, SummaryRuntime] = {}
        self._suspending: set[str] = set()
        self._stop_requests: set[str] = set()
        self._deleting: set[str] = set()
        self.last_execution_id: typing.Optional[str] = None
        self._executions: set[asyncio.Task[pydantic.JsonValue]] = set()
        self._execution_tasks: dict[str, asyncio.Task[pydantic.JsonValue]] = {}
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
                    self.path,
                    workspace_directory=self.workspace_directory,
                    config_dir=self.config_dir,
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
        base_project_path: typing.Optional[str] = None,
        worktree_id: typing.Optional[UUID] = None,
        presentation: typing.Optional[
            presentation_models.WorkflowPresentationOverride
        ] = None,
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
                base_project_path=base_project_path,
                worktree_id=worktree_id,
                presentation=presentation,
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
        """Stop resident work or mark an execution that is still starting."""
        self._stop_requests.add(execution_id)
        self._suspending.discard(execution_id)
        owner = self._mailboxes.get(execution_id)
        if owner is not None:
            owner.suspending = False
            if owner.pause is not None:
                owner.pause.frozen = True
            task = self._execution_tasks.get(execution_id)
            if task is not None and not task.done() and not task.cancelling():
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
                if execution_id in self._deleting:
                    raise ValueError("execution deletion is in progress")
                task = asyncio.current_task()
                assert task is not None
                self._execution_tasks[execution_id] = task
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

                    try:
                        await finish_cleanup(asyncio.create_task(settle()))
                    finally:
                        runtime = self._summaries.pop(execution_id, None)
                        if runtime is not None:
                            await runtime.close()
                        self._execution_tasks.pop(execution_id, None)
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

    async def authorization_rules(
        self, method: str, params: dict[str, pydantic.JsonValue]
    ) -> pydantic.JsonValue:
        scope = tool_rule_api.RuleScope.model_validate(
            {
                key: value
                for key, value in params.items()
                if key in tool_rule_api.RuleScope.model_fields
            }
        )
        project = await self.project()
        database = (
            await self._database(project, scope.execution_id)
            if scope.execution_id is not None
            else None
        )
        if database is not None:
            await database.get_workflow(scope.workflow_id or "")
        return await tool_rule_api.RuleManagement(
            tool_authorization_store.WorkspaceRules(project.database), database
        ).dispatch(method, params)

    async def answer_input(self, answer: mailbox.Answer) -> mailbox.InputRequest:
        if answer.execution_id in self._deleting:
            raise mailbox.MailboxConflict("execution deletion is in progress")
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
        naming = await self._workflow_naming()
        if execution_id not in self._deleting:
            naming.schedule(database, workflow_id, execution_id=execution_id)

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

    async def stop_unloaded(
        self, execution_id: str, *, require_paused: bool = False
    ) -> None:
        project = await self.project()
        async with self._writer(project) as store:
            async with store.reserve(execution_id):
                database = await self._database(project, execution_id)
                await database.stop_execution(require_paused=require_paused)
                await store.deactivate(execution_id)

    async def delete_worktree(
        self, request: worktree_models.Delete
    ) -> worktree_models.Record:
        """Delete on behalf of a user; workflow tools use the non-cancelling policy."""
        return await (await self.project()).worktrees.delete(
            request, workflows=self, cancel_workflows=True
        )

    async def stop_execution(
        self,
        execution_id: str,
        *,
        require_paused: bool = False,
        allow_terminal: bool = False,
    ) -> typing.Optional[asyncio.Task[pydantic.JsonValue]]:
        """Commit cancellation before signalling resident work; tolerate completion only when requested."""
        project = await self.project()
        database = await self._database(project, execution_id)
        task = self._execution_tasks.get(execution_id)
        try:
            if task is None:
                await self.stop_unloaded(execution_id, require_paused=require_paused)
            else:
                await database.stop_execution(
                    require_paused=require_paused and not task.cancelling()
                )
        except ValueError:
            if not allow_terminal or (await database.get_execution()).status in (
                "running",
                "paused",
            ):
                raise
        if task is not None and not task.done():
            self.request_stop(execution_id)
        elif (
            self._state_store is not None
            and execution_id in self._state_store.scheduled
        ):
            self.request_stop(execution_id)
        return task

    async def prepare_worktree_deletion(
        self,
        worktree: worktree_models.Record,
        execution_ids: set[str],
        *,
        force: bool,
        cancel: bool,
    ) -> Callable[[], typing.Awaitable[None]]:
        """Check indexed canonical owners separately from live attachment cleanup."""
        project = await self.project()
        waiting: set[asyncio.Task[pydantic.JsonValue]] = set()

        async def wait_for_cleanup() -> None:
            await asyncio.gather(
                *(asyncio.shield(task) for task in waiting), return_exceptions=True
            )

        identifiers = await (await self.execution_index()).worktree_executions(
            worktree.id, worktree.path
        )
        selected: dict[str, execution_models.ExecutionRecord] = {}
        identifiers.update(execution_ids)
        for identifier in sorted(identifiers):
            owner = self._mailboxes.get(identifier)
            if owner is not None and owner.pause is not None:
                await owner.pause.flush()
            try:
                database = await self._database(project, identifier)
                record = await database.get_execution()
            except (FileNotFoundError, KeyError):
                continue
            task = self._execution_tasks.get(identifier)
            if identifier in execution_ids and task is not None:
                waiting.add(task)
            if record.status not in ("running", "paused"):
                continue
            for target in await database.referenced_targets():
                if await project.worktrees.owning_worktree(target) == worktree.id:
                    selected[identifier] = record
                    break
        if not cancel:
            if selected:
                raise worktree_models.WorktreeError(
                    "worktree is still in use by a workflow; only user deletion can cancel executions"
                )
            return wait_for_cleanup
        if not force:
            running = [
                identifier
                for identifier, record in selected.items()
                if record.status == "running"
                and not (
                    identifier in self._execution_tasks
                    and self._execution_tasks[identifier].cancelling()
                )
            ]
            if running:
                raise worktree_models.WorktreeError(
                    "worktree has running executions; force is required: "
                    + ", ".join(running)
                )
        for identifier in selected:
            try:
                task = await self.stop_execution(
                    identifier, require_paused=not force, allow_terminal=True
                )
            except ValueError as exc:
                raise worktree_models.WorktreeError(str(exc)) from exc
            if task is not None:
                waiting.add(task)
        return wait_for_cleanup

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
        base_project_path: typing.Optional[str],
        worktree_id: typing.Optional[UUID],
        presentation: typing.Optional[presentation_models.WorkflowPresentationOverride],
    ) -> pydantic.JsonValue:
        project = await self.project()
        environment_config = project.config.environment
        assert environment_config is not None
        if base_project_path is not None and worktree_id is not None:
            raise ValueError("base_project_path and worktree_id are mutually exclusive")
        if (base_project_path is not None or worktree_id is not None) and (
            execution_id is not None or continue_from_step_id is not None
        ):
            raise ValueError(
                "base_project_path and worktree_id can only be set for a new execution"
            )
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
                        target=(
                            await project.worktrees.resolve(worktree_id)
                            if worktree_id is not None
                            else await LocalEnvironment.resolve_target(
                                environment_config, base_project_path
                            )
                        ),
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
                    configuration = configuration.model_copy(
                        update={
                            "presentation": configuration.presentation.with_overrides(
                                presentation
                            )
                        }
                    )
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
                    if record.target is None:
                        record = record.model_copy(
                            update={
                                "target": await LocalEnvironment.resolve_target(
                                    environment_config
                                )
                            }
                        )
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
                runtime = SummaryRuntime(
                    project.summaries,
                    (
                        partial(self.change_sink, execution_id)
                        if self.change_sink is not None
                        else None
                    ),
                    publication_sink=(
                        partial(self.publication_sink, execution_id)
                        if self.publication_sink is not None
                        else None
                    ),
                )
                self._summaries[execution_id] = runtime
                async with (
                    SQLiteHistory(
                        history_path,
                        workspace_rules=tool_authorization_store.WorkspaceRules(
                            project.database
                        ),
                        usage_store=project.usage,
                        summary_store=project.summaries,
                        summary_runtime=runtime,
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
                    await database.publish_authorization_rules()
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
                    terminal_replay = False
                    if root_head is not None:
                        selected = await history.get_step(root_head)
                        terminal_replay = (
                            selected.operation.kind == "workflow.complete"
                            and selected.outcome.kind in ("completed", "failed")
                        )
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
                    assert record.target is not None
                    references = [record.target]
                    if not is_new and not terminal_replay:
                        references.extend(await database.referenced_targets())
                    async with project.worktrees.protect_references(references):
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
                                target=record.target,
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
            target=(
                await project.worktrees.resolve(worktree_id)
                if worktree_id is not None
                else await LocalEnvironment.resolve_target(
                    environment_config, base_project_path
                )
            ),
            input=input,
            workflow_id=workflow_id,
            continue_from_step_id=continue_from_step_id,
            child_heads=child_heads,
            presentation=presentation,
        )

    async def _run(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        reference: str,
        *,
        target: LocalExecutionTarget,
        input: typing.Optional[dict[str, pydantic.JsonValue]],
        workflow_id: typing.Optional[str],
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
        reporter: typing.Optional[ExecutionCheckpointer] = None,
        execution_id: typing.Optional[str] = None,
        mailbox_owner: typing.Optional[mailbox.Mailbox] = None,
        tool_grants: typing.Optional[tool_authorization_store.WorkflowGrants] = None,
        presentation: typing.Optional[
            presentation_models.WorkflowPresentationOverride
        ] = None,
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
                project.configuration,
                tool_grants,
                tool_authorization_store.WorkspaceRules(project.database),
            ),
            self._tool_policy,
        )
        self._tool_policy = authorization.policy
        with authorization.activate():
            return await self._run_configured(
                project,
                history,
                configuration,
                target=target,
                continue_from_step_id=continue_from_step_id,
                child_heads=child_heads,
                reporter=reporter,
                execution_id=execution_id,
                mailbox_owner=mailbox_owner,
                presentation=presentation,
            )

    async def _run_configured(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        configuration: models.Workflow,
        *,
        target: LocalExecutionTarget,
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[dict[str, str]],
        reporter: typing.Optional[ExecutionCheckpointer],
        execution_id: typing.Optional[str],
        mailbox_owner: typing.Optional[mailbox.Mailbox],
        presentation: typing.Optional[presentation_models.WorkflowPresentationOverride],
    ) -> pydantic.JsonValue:
        async with ExecutionResources() as resources:
            mcp = MCPRuntime(self.authentication, execution_id, pool=self.mcp_pool)
            resources.register("mcp", mcp.close)
            environment = None
            targets = None
            if project.config.environment is not None:
                target_history = (
                    await self._database(project, execution_id)
                    if execution_id is not None
                    else None
                )
                targets = ExecutionTargets(
                    project.worktrees,
                    project.config.environment,
                    execution_id,
                    workflows=self,
                    reference_target=(
                        target_history.reference_target
                        if target_history is not None
                        else None
                    ),
                )
                resources.register("targets", targets.close)
                environment = await targets.open(target)
            executor = WorkflowExecutor(
                project,
                history,
                reporter,
                execution_id,
                self.event_sink,
                mailbox_owner,
                self.authentication,
                environment=environment,
                targets=targets,
                target=target,
                resources=resources,
                mcp=mcp,
                scheduler=self._scheduler,
            )
            if continue_from_step_id is None and execution_id is None:
                configuration = await executor.configure(configuration)
                configuration = configuration.model_copy(
                    update={
                        "presentation": configuration.presentation.with_overrides(
                            presentation
                        )
                    }
                )
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
            workspace_rules=tool_authorization_store.WorkspaceRules(project.database),
            usage_store=project.usage,
            summary_store=project.summaries,
            summary_runtime=self._summaries.get(execution_id),
            publication_sink=(
                partial(self.publication_sink, execution_id)
                if self.publication_sink is not None
                else None
            ),
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

    async def runner_disk_usage(self, query: disk_usage.Query) -> disk_usage.Report:
        project = await self.project()
        return await disk_usage.measure(
            project.workspace_path, project.data_path, query
        )

    async def query_history(
        self,
        query: history_query.HistoryQuery,
        *,
        page_fits: typing.Optional[Callable[[history_query.HistoryPage], bool]] = None,
    ) -> history_query.HistoryPage:
        return await (await self._selected_database(query.execution_id)).query(
            query, page_fits=page_fits
        )

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
                await (
                    await self._database(await self.project(), identifier)
                ).publish_authorization_rules()
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

    async def remove_execution(self, execution_id: str) -> bool:
        """Drain paused work and hard-delete its history and workspace-owned records."""
        return await finish_cleanup(
            asyncio.create_task(self._remove_execution(execution_id))
        )

    async def _remove_execution(self, execution_id: str) -> bool:
        state.RunnerState(last_execution_id=execution_id)
        if execution_id in self._deleting:
            raise ValueError("execution deletion is in progress")
        self._deleting.add(execution_id)
        try:
            return await self._delete_execution(execution_id)
        finally:
            self._deleting.discard(execution_id)

    async def _delete_execution(self, execution_id: str) -> bool:
        """Hold writer ownership through removal; retries also clean missing histories."""
        project = await self.project()
        await self.initialize_usage()
        directory = project.data_path / "executions" / execution_id
        if await asyncio.to_thread(directory.resolve) != directory:
            raise ValueError("execution storage must not contain symlinks")
        database: typing.Optional[SQLiteHistory]
        task = self._execution_tasks.get(execution_id)
        if task is not None:
            database = await self._database(project, execution_id)
            if (await database.get_execution()).status == "running":
                raise ValueError("active execution is running; stop it before deletion")
            await self.stop_execution(
                execution_id, require_paused=True, allow_terminal=True
            )
            await asyncio.gather(asyncio.shield(task), return_exceptions=True)
        async with self._writer(project) as store:
            async with store.reserve(execution_id):
                try:
                    database = await self._database(project, execution_id)
                except FileNotFoundError:
                    database = None
                if database is not None:
                    record = await database.get_execution()
                    if record.status == "running":
                        raise ValueError(
                            "active execution is running; stop it before deletion"
                        )
                    if record.status == "paused":
                        await database.stop_execution(require_paused=True)
                    async with project.usage.lock:
                        revision, total = await database.usage_snapshot()
                        await project.usage.record(execution_id, revision, total)
                if self._naming is not None:
                    await self._naming.cancel_execution(execution_id)
                await project.worktrees.release_execution(execution_id)
                existed = await asyncio.to_thread(directory.exists)
                if existed:
                    await asyncio.to_thread(shutil.rmtree, directory)
                await project.summaries.remove([execution_id])
                async with project.usage.lock:
                    await project.usage.remove(execution_id)
                await project.database.remove_execution_receipts(execution_id)
                await store.forget(execution_id)
                if self.last_execution_id == execution_id:
                    self.last_execution_id = None
                return existed
