"""Transactional SQLite history with replaceable unfinished activity snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import typing
from uuid import UUID, uuid4

import pydantic

from vectorops.runner import models, usage, tool_policy
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.tool_authorization_store import (
    SQLiteWorkflowGrants,
    WorkspaceRules,
)
from vectorops.runner.runner import execution_state as state
from vectorops.runner.runner import inspection, sql
from vectorops.runner.runner import history_query as queries
from vectorops.runner.runner import mailbox
from vectorops.runner.runner import branching
from vectorops.runner.runner import events
from vectorops.runner import diagnostics
from vectorops.runner.runner.sqlite_writer import SQLiteWriter
from vectorops.runner.runner.usage_history import UsageHistory
from vectorops.runner.runner.usage_store import UsageStore
from vectorops.runner.runner import summary_history
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.summary_runtime import SummaryRuntime
from vectorops.runner.runner.resources import finish_cleanup

T = typing.TypeVar("T")
WORKFLOWS: typing.Final = sql.Table("workflows")
STEPS: typing.Final = sql.Table("steps")
EXECUTION: typing.Final = sql.Table("execution")
HEADS: typing.Final = sql.Table("workflow_heads")
INPUTS: typing.Final = sql.Table("input_requests")
BRANCHES: typing.Final = sql.Table("branch_operations")
RULES: typing.Final = sql.Table("tool_authorization_rules")
RECEIPTS: typing.Final = sql.Table("tool_rule_receipts")
OUTBOX: typing.Final = sql.Table("tool_rule_outbox")
REPLAY: typing.Final = sql.Table("replay")


class ExecutionMetadata(pydantic.BaseModel):
    id: str
    status: state.ExecutionStatus


async def _offload(function: Callable[[], T]) -> T:
    """Finish database work before cancellation can trigger connection cleanup."""
    task = asyncio.create_task(asyncio.to_thread(function))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        finally:
            raise


class SQLiteHistory:
    def __init__(
        self,
        path: typing.Union[str, Path],
        *,
        batch_size: int = 128,
        usage_store: typing.Optional[UsageStore] = None,
        summary_store: typing.Optional[SummaryStore] = None,
        summary_runtime: typing.Optional[SummaryRuntime] = None,
        change_sink: typing.Optional[events.HistoryChangeSink] = None,
        publication_sink: typing.Optional[events.PublicationSink] = None,
        event_sink: typing.Optional[events.StepEventSink] = None,
        mailbox_sink: typing.Optional[mailbox.EventSink] = None,
        workspace_rules: typing.Optional[WorkspaceRules] = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("replay batch size must be positive")
        self.path = Path(path).expanduser().absolute()
        self.batch_size = batch_size
        self._ready = False
        self._initialization_lock = asyncio.Lock()
        self._writer: typing.Optional[SQLiteWriter] = None
        self._closed = False
        self._usage_store = usage_store
        self._summary_store = summary_store
        self._summary_runtime = summary_runtime or (
            SummaryRuntime(
                summary_store, change_sink, publication_sink=publication_sink
            )
            if summary_store is not None
            else None
        )
        self._change_sink = change_sink
        self._event_sink = event_sink
        self._mailbox_sink = mailbox_sink
        self._workspace_rules = workspace_rules

    async def __aenter__(self) -> typing.Self:
        if self._closed or self._writer is not None:
            raise RuntimeError("history writer is already open or closed")
        await self._prepare()
        self._writer = SQLiteWriter(self._connect)
        if self._summary_runtime is not None:
            self._summary_runtime.active = True
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        if self._writer is not None:
            await self._writer.aclose()
        if self._summary_runtime is not None:
            await self._summary_runtime.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    async def _prepare(self) -> None:
        if self._ready:
            return
        async with self._initialization_lock:
            if not self._ready:
                async with asyncio.timeout(30):
                    while True:
                        try:
                            await _offload(self._initialize)
                            break
                        except sqlite3.OperationalError as exc:
                            if exc.sqlite_errorcode not in (
                                sqlite3.SQLITE_BUSY,
                                sqlite3.SQLITE_LOCKED,
                            ):
                                raise
                            await asyncio.sleep(0.01)
                self._ready = True

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise ValueError(f"unsupported history schema version: {version}")
            if version == 1:
                connection.execute("PRAGMA locking_mode = EXCLUSIVE")
            connection.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS workflows (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS steps (
                    id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL REFERENCES workflows(id),
                    previous_step_id TEXT REFERENCES steps(id),
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS steps_workflow ON steps(workflow_id);
                CREATE INDEX IF NOT EXISTS steps_previous ON steps(previous_step_id);
                CREATE TABLE IF NOT EXISTS execution (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workflow_heads (
                    workflow_id TEXT PRIMARY KEY REFERENCES workflows(id),
                    step_id TEXT NOT NULL REFERENCES steps(id)
                );
                CREATE TABLE IF NOT EXISTS input_requests (
                    request_id TEXT PRIMARY KEY REFERENCES steps(id),
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS branch_operations (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL
                );
                """)
            with connection:
                summary_history.SummaryHistory.initialize(connection)
                UsageHistory.initialize(connection)
                SQLiteWorkflowGrants.initialize(connection)
                connection.execute("PRAGMA user_version = 2")

    async def _run(
        self,
        operation: Callable[[sqlite3.Connection], T],
    ) -> T:
        return await self._invoke(operation)

    @contextmanager
    def _transaction(
        self, connection: sqlite3.Connection, changes: summary_history.Changes
    ) -> typing.Iterator[typing.Optional[summary_history.Snapshot]]:
        """Commit business writes, references and one source revision atomically."""
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("PRAGMA user_version").fetchone()[0] != 2:
                raise ValueError("unsupported history schema version")
            runtime = self._summary_runtime
            base = runtime.working.base(connection) if runtime is not None else None
            if runtime is not None and base is not runtime.working.snapshot:
                changes.execution_changed = True
            yield base
            if changes.changed:
                revision = connection.execute(
                    "UPDATE summary_state SET revision = revision + 1 WHERE id = 1 RETURNING revision"
                ).fetchone()[0]
                if runtime is not None:
                    if base is not None:
                        changes.snapshot = summary_history.WorkingSummary.project(
                            base, changes, revision
                        )
                    else:
                        try:
                            changes.snapshot = summary_history.SummaryHistory.snapshot(
                                connection
                            )
                        except KeyError:
                            pass

    async def _mutate(
        self,
        operation: Callable[[sqlite3.Connection], T],
        changes: summary_history.Changes,
        *,
        notifications: typing.Optional[
            list[typing.Union[events.StepEvent, mailbox.InputsCancelled]]
        ] = None,
    ) -> T:
        async def persist() -> T:
            result = await self._invoke(operation)
            runtime = self._summary_runtime
            if changes.snapshot is not None and runtime is not None:
                runtime.working.snapshot = changes.snapshot
            try:
                if self._usage_store is not None and changes.accounting is not None:
                    async with self._usage_store.lock:
                        await self._usage_store.record(
                            self.path.parent.name,
                            changes.accounting.revision,
                            changes.accounting.totals,
                        )
            finally:
                if changes.snapshot is not None and runtime is not None:
                    await runtime.changed(
                        changes.snapshot,
                        tuple(sorted(changes.workflows)),
                        urgency=changes.urgency,
                        notify=changes.notify,
                        execution_changed=changes.execution_changed,
                    )
                for event in notifications or ():
                    try:
                        if isinstance(event, events.StepEvent):
                            if self._event_sink is not None:
                                self._event_sink(event)
                        elif self._mailbox_sink is not None:
                            self._mailbox_sink(event.model_dump(mode="json"))
                    except Exception as exc:
                        diagnostics.log(
                            "history.sink_failed",
                            level=diagnostics.Level.ERROR,
                            error=exc,
                        )
                if (
                    changes.changed
                    and runtime is None
                    and self._change_sink is not None
                    and (changes.notify or changes.workflows)
                ):
                    try:
                        self._change_sink(tuple(sorted(changes.workflows)))
                    except Exception as exc:
                        diagnostics.log(
                            "history.sink_failed",
                            level=diagnostics.Level.ERROR,
                            error=exc,
                        )
            return result

        async def run() -> T:
            if self._summary_runtime is None:
                return await persist()
            async with self._summary_runtime.lock:
                return await persist()

        return await finish_cleanup(asyncio.create_task(run()))

    async def _invoke(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        if self._closed:
            raise RuntimeError("history writer is closed")
        await self._prepare()
        if self._writer is not None:
            return await self._writer.run(operation)

        def invoke() -> T:
            with closing(self._connect()) as connection:
                return operation(connection)

        return await _offload(invoke)

    @staticmethod
    def _workflow(connection: sqlite3.Connection, workflow_id: str) -> models.Workflow:
        row = sql.execute(
            connection,
            sql.Query.from_(WORKFLOWS)
            .select(WORKFLOWS.payload)
            .where(WORKFLOWS.id == workflow_id),
        ).fetchone()
        if row is None:
            raise KeyError(workflow_id)
        return models.Workflow.model_validate_json(row[0])

    @staticmethod
    def _step(connection: sqlite3.Connection, step_id: str) -> models.WorkflowStep:
        row = connection.execute(
            "SELECT payload FROM steps WHERE id = ?", (step_id,)
        ).fetchone()
        if row is None:
            raise KeyError(step_id)
        return models.WorkflowStep.model_validate_json(row[0])

    async def save_workflow(self, workflow: models.Workflow) -> None:
        payload = workflow.model_copy(update={"usage": None}).model_dump_json()
        changes = summary_history.Changes()

        def save(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes):
                row = sql.execute(
                    connection,
                    sql.Query.from_(WORKFLOWS)
                    .select(WORKFLOWS.payload)
                    .where(WORKFLOWS.id == workflow.id),
                ).fetchone()
                if row is not None:
                    if not models.Workflow.model_validate_json(
                        row[0]
                    ).same_configuration(workflow):
                        raise ValueError("workflow configuration conflict")
                    return
                sql.execute(
                    connection,
                    sql.Query.into(WORKFLOWS).insert(workflow.id, payload),
                )
                changes.changed = True
                changes.workflows.add(workflow.id)
                changes.targets = summary_history.TargetReferences.add(
                    connection, [workflow.target] if workflow.target is not None else []
                )

        await self._mutate(save, changes)

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        return await self._run(
            lambda connection: self._workflow(connection, workflow_id)
        )

    async def referenced_targets(self) -> list[LocalExecutionTarget]:
        """Return all durable bindings, including completed and historical children."""

        def read(connection: sqlite3.Connection) -> list[LocalExecutionTarget]:
            with connection:
                connection.execute("BEGIN")
                runtime = self._summary_runtime
                if runtime is not None and runtime.working.snapshot is not None:
                    snapshot = runtime.working.snapshot
                    version = connection.execute(
                        "SELECT source_id, revision FROM summary_state WHERE id = 1"
                    ).fetchone()
                    if version == (snapshot.source_id, snapshot.revision):
                        return list(snapshot.targets)
                return summary_history.TargetReferences.read(connection)

        return await self._run(read)

    async def reference_target(self, target: LocalExecutionTarget) -> None:
        """Retain runtime worktree references before using their files or processes."""
        changes = summary_history.Changes()

        def write(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes):
                execution = self._execution(connection)
                if target in execution.referenced_targets:
                    return
                execution = execution.model_copy(
                    update={
                        "referenced_targets": [*execution.referenced_targets, target]
                    }
                )
                sql.execute(
                    connection,
                    sql.Query.update(EXECUTION)
                    .set(EXECUTION.payload, execution.model_dump_json())
                    .where(EXECUTION.singleton == 1),
                )
                changes.changed = True
                changes.execution_changed = True
                changes.targets = summary_history.TargetReferences.add(
                    connection, [target]
                )

        await self._mutate(write, changes)

    async def set_workflow_name(self, workflow_id: str, name: str) -> models.Workflow:
        """Set presentation metadata once without replacing execution configuration."""
        return await self._set_workflow_text(workflow_id, "name", name)

    async def set_workflow_description(
        self, workflow_id: str, description: str
    ) -> models.Workflow:
        """Set the generated execution description without changing its workflow name."""
        return await self._set_workflow_text(workflow_id, "description", description)

    async def _set_workflow_text(
        self,
        workflow_id: str,
        field: typing.Literal["name", "description"],
        value: str,
    ) -> models.Workflow:
        """Commit write-once text metadata and publish the updated snapshots."""
        changes = summary_history.Changes()

        def update(connection: sqlite3.Connection) -> models.Workflow:
            with self._transaction(connection, changes):
                workflow = self._workflow(connection, workflow_id)
                current = workflow.name if field == "name" else workflow.description
                if current is not None:
                    return workflow
                updated = models.Workflow.model_validate(
                    {**workflow.model_dump(), field: value}
                )
                sql.execute(
                    connection,
                    sql.Query.update(WORKFLOWS)
                    .set(WORKFLOWS.payload, updated.model_dump_json())
                    .where(WORKFLOWS.id == workflow_id),
                )
                changes.changed = True
                changes.workflows.add(workflow_id)
                changes.metadata[workflow_id] = (updated.name, updated.description)
                return updated

        return await self._mutate(update, changes)

    async def first_input(
        self, workflow_id: str, request_type: str
    ) -> typing.Optional[mailbox.InputRequest]:
        """Read the first accepted input, ignoring requests with no user response."""

        def read(
            connection: sqlite3.Connection,
        ) -> typing.Optional[mailbox.InputRequest]:
            row = sql.execute(
                connection,
                sql.Query.from_(INPUTS)
                .select(INPUTS.payload)
                .where(sql.json_extract(INPUTS.payload, "$.workflow_id") == workflow_id)
                .where(
                    sql.json_extract(INPUTS.payload, "$.request_type") == request_type
                )
                .where(sql.json_extract(INPUTS.payload, "$.status") == "answered")
                .orderby(
                    sql.json_extract(INPUTS.payload, "$.answered_at"), INPUTS.rowid
                )
                .limit(1),
            ).fetchone()
            return (
                None
                if row is None
                else mailbox.StoredInputRequest.model_validate_json(row[0]).public()
            )

        return await self._run(read)

    async def save_execution(
        self,
        execution: state.ExecutionRecord,
        *,
        replace_heads: bool = False,
        preserve_status: bool = False,
    ) -> None:
        changes = summary_history.Changes()

        def save(connection: sqlite3.Connection) -> None:
            saved = execution
            with self._transaction(connection, changes):
                self._workflow(connection, execution.root_workflow_id)
                existing = sql.execute(
                    connection,
                    sql.Query.from_(EXECUTION)
                    .select(EXECUTION.payload)
                    .where(EXECUTION.singleton == 1),
                ).fetchone()
                if existing is not None:
                    previous = state.ExecutionRecord.model_validate_json(existing[0])
                    if (previous.id, previous.root_workflow_id) != (
                        execution.id,
                        execution.root_workflow_id,
                    ):
                        raise ValueError("execution identity cannot change")
                    if (
                        previous.target is not None
                        and previous.target != execution.target
                    ):
                        raise ValueError("execution target cannot change")
                    if preserve_status:
                        saved = execution.model_copy(update={"status": previous.status})
                    saved = saved.model_copy(
                        update={
                            "referenced_targets": list(
                                dict.fromkeys(
                                    [
                                        *previous.referenced_targets,
                                        *saved.referenced_targets,
                                    ]
                                )
                            )
                        }
                    )
                    if not replace_heads:
                        authoritative = self._execution(connection)
                        saved = saved.model_copy(
                            update={
                                "checkpoint": saved.checkpoint.model_copy(
                                    update={"heads": authoritative.checkpoint.heads}
                                ),
                                "completion_step_id": authoritative.completion_step_id,
                            }
                        )
                for workflow_id, step_id in execution.checkpoint.heads.items():
                    if self._step(connection, step_id).workflow_id != workflow_id:
                        raise ValueError("checkpoint head belongs to another workflow")
                old_heads = dict(
                    connection.execute(
                        "SELECT workflow_id, step_id FROM workflow_heads"
                    )
                )
                heads_changed = (
                    existing is None or replace_heads
                ) and old_heads != saved.checkpoint.heads
                payload = saved.model_dump_json()
                if (
                    existing is not None
                    and existing[0] == payload
                    and not heads_changed
                ):
                    return
                sql.execute(
                    connection,
                    sql.Query.into(EXECUTION)
                    .insert(1, payload)
                    .on_conflict(EXECUTION.singleton)
                    .do_update(EXECUTION.payload),
                )
                if heads_changed:
                    sql.execute(connection, sql.Query.from_(HEADS).delete())
                    sql.insert_many(
                        connection,
                        HEADS,
                        execution.checkpoint.heads.items(),
                    )
                    if old_heads.get(
                        saved.root_workflow_id
                    ) != saved.checkpoint.heads.get(saved.root_workflow_id):
                        changes.context_changed = True
                        changes.context = UsageHistory.root_context(
                            connection, saved.root_workflow_id
                        )
                changes.changed = changes.notify = True
                changes.execution_changed = True
                changes.status, changes.updated_at = saved.status, saved.updated_at
                if existing is None or previous.status != saved.status:
                    changes.urgency = summary_history.Urgency.IMMEDIATE
                references = list(saved.referenced_targets)
                if saved.target is not None:
                    references.append(saved.target)
                if existing is not None:
                    retained = set(previous.referenced_targets)
                    if previous.target is not None:
                        retained.add(previous.target)
                    references = [
                        target for target in references if target not in retained
                    ]
                changes.targets = summary_history.TargetReferences.add(
                    connection, references
                )

        await self._mutate(save, changes)

    async def checkpoint_execution(
        self,
        execution_id: str,
        *,
        status: typing.Optional[state.ExecutionStatus] = None,
    ) -> None:
        """Refresh activity metadata without reading or validating retained heads.

        Head ownership belongs to append and branch transactions. Terminal saves
        materialize the legacy execution envelope from those authoritative rows,
        while ordinary checkpoints leave its redundant head map untouched.
        """
        changes = summary_history.Changes()

        def checkpoint(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes) as base:
                if base is not None:
                    if base.summary.execution_id != execution_id:
                        raise ValueError("execution identity mismatch")
                    previous = base.summary.status
                else:
                    row = connection.execute(
                        "SELECT json_extract(payload, '$.id'), json_extract(payload, '$.status') "
                        "FROM execution WHERE singleton = 1"
                    ).fetchone()
                    if row is None:
                        raise KeyError("execution state")
                    if row[0] != execution_id:
                        raise ValueError("execution identity mismatch")
                    previous = row[1]
                updated_at = state.timestamp()
                connection.execute(
                    "UPDATE execution SET payload = json_set(payload, '$.updated_at', ?, "
                    "'$.checkpoint.updated_at', ?) WHERE singleton = 1",
                    (updated_at, updated_at),
                )
                if status is not None:
                    connection.execute(
                        "UPDATE execution SET payload = json_set(payload, '$.status', ?) WHERE singleton = 1",
                        (status,),
                    )
                    changes.status = status
                    if previous != status:
                        changes.urgency = summary_history.Urgency.IMMEDIATE
                if status in ("completed", "failed", "cancelled"):
                    connection.execute(
                        "UPDATE execution SET payload = json_set(payload, '$.checkpoint.heads', "
                        "json((SELECT json_group_object(workflow_id, step_id) FROM workflow_heads)), "
                        "'$.completion_step_id', (SELECT steps.id FROM workflow_heads "
                        "JOIN steps ON steps.id = workflow_heads.step_id "
                        "WHERE workflow_heads.workflow_id = json_extract(execution.payload, '$.root_workflow_id') "
                        "AND json_extract(steps.payload, '$.operation.kind') = 'workflow.complete')) "
                        "WHERE singleton = 1"
                    )
                changes.changed = changes.notify = True
                changes.execution_changed = True
                changes.updated_at = updated_at

        await self._mutate(checkpoint, changes)

    async def set_paused(self, requests: set[str]) -> bool:
        """Pause only if every observed dependency is still unanswered at commit."""
        if not requests:
            return False
        changes = summary_history.Changes()

        def pause(connection: sqlite3.Connection) -> bool:
            with self._transaction(connection, changes):
                execution = self._execution_metadata(connection)
                if execution.status != "running" or any(
                    self._input(connection, request).status != "pending"
                    for request in requests
                ):
                    return False
                self._set_status(connection, "paused", changes)
                return True

        return await self._mutate(pause, changes)

    @staticmethod
    def _set_status(
        connection: sqlite3.Connection,
        status: state.ExecutionStatus,
        changes: summary_history.Changes,
    ) -> None:
        updated_at = state.timestamp()
        sql.execute(
            connection,
            sql.Query.update(EXECUTION)
            .set(
                EXECUTION.payload,
                sql.json_set(
                    EXECUTION.payload,
                    "$.status",
                    status,
                    "$.updated_at",
                    updated_at,
                ),
            )
            .where(EXECUTION.singleton == 1),
        )
        changes.changed = changes.notify = True
        changes.execution_changed = True
        changes.status, changes.updated_at = status, updated_at
        changes.urgency = summary_history.Urgency.IMMEDIATE

    async def wake_execution(self) -> bool:
        changes = summary_history.Changes()

        def wake(connection: sqlite3.Connection) -> bool:
            with self._transaction(connection, changes):
                if self._execution_metadata(connection).status != "paused":
                    return False
                self._set_status(connection, "running", changes)
                return True

        return await self._mutate(wake, changes)

    async def stop_execution(self, *, require_paused: bool = False) -> None:
        """Cancel an unloaded execution and its requests in one transaction."""
        changes = summary_history.Changes()
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def stop(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes):
                status = self._execution_metadata(connection).status
                if status not in ("running", "paused"):
                    raise ValueError("execution is not active")
                if require_paused and status != "paused":
                    raise ValueError("execution is running; force is required")
                self._cancel_inputs(connection, notifications, changes)
                self._set_status(connection, "cancelled", changes)

        await self._mutate(stop, changes, notifications=notifications)

    def _branch_plan(
        self, connection: sqlite3.Connection, operation: branching.Operation
    ) -> branching.Plan:
        return branching.plan(
            operation,
            self._execution(connection),
            lambda identifier: self._step(connection, identifier),
            lambda identifier: self._input(connection, identifier),
        )

    async def accept_branch(self, request: branching.Branch) -> branching.Operation:
        """Validate before interruption and durably accept an execution-scoped key."""
        changes = summary_history.Changes()

        def accept(connection: sqlite3.Connection) -> branching.Operation:
            with self._transaction(connection, changes):
                row = sql.execute(
                    connection,
                    sql.Query.from_(BRANCHES)
                    .select(BRANCHES.payload)
                    .where(BRANCHES.idempotency_key == request.idempotency_key),
                ).fetchone()
                if row is not None:
                    previous = branching.Operation.model_validate_json(row[0])
                    if previous.request != request:
                        raise branching.BranchConflict(
                            "idempotency key already used for different arguments"
                        )
                    return previous
                if sql.execute(
                    connection,
                    sql.Query.from_(BRANCHES)
                    .select(1)
                    .where(
                        sql.json_extract(BRANCHES.payload, "$.status") == "accepted"
                    ),
                ).fetchone():
                    raise branching.BranchConflict("another branch is being prepared")
                operation = branching.Operation(
                    request=request,
                    input_step_id=(
                        uuid4().hex
                        if isinstance(request.action, branching.InsertInput)
                        else None
                    ),
                )
                self._branch_plan(connection, operation)
                sql.execute(
                    connection,
                    sql.Query.into(BRANCHES).insert(
                        operation.id,
                        request.idempotency_key,
                        operation.model_dump_json(),
                    ),
                )
                changes.changed = True
                return operation

        return await self._mutate(accept, changes)

    async def pending_branch(self) -> bool:
        return await self._run(
            lambda connection: sql.execute(
                connection,
                sql.Query.from_(BRANCHES)
                .select(1)
                .where(sql.json_extract(BRANCHES.payload, "$.status") == "accepted"),
            ).fetchone()
            is not None
        )

    async def apply_branch(self) -> typing.Optional[branching.Operation]:
        """Commit path selection and synthetic input atomically after writer cleanup."""
        changes = summary_history.Changes()
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def apply(
            connection: sqlite3.Connection,
        ) -> typing.Optional[branching.Operation]:
            with self._transaction(connection, changes) as base:
                row = sql.execute(
                    connection,
                    sql.Query.from_(BRANCHES)
                    .select(BRANCHES.payload)
                    .where(
                        sql.json_extract(BRANCHES.payload, "$.status") == "accepted"
                    ),
                ).fetchone()
                if row is None:
                    return None
                operation = branching.Operation.model_validate_json(row[0])
                try:
                    plan = self._branch_plan(connection, operation)
                except (ValueError, KeyError) as exc:
                    operation = operation.model_copy(
                        update={"status": "failed", "error": str(exc)}
                    )
                else:
                    self._cancel_inputs(connection, notifications, changes)
                    step = plan.input_step
                    if step is not None:
                        cursor = sql.execute(
                            connection,
                            sql.Query.into(STEPS).insert(
                                step.id,
                                step.workflow_id,
                                step.previous_step_id,
                                step.model_dump_json(),
                            ),
                        )
                        assert cursor.lastrowid is not None
                        changes.workflows.update(UsageHistory.link(connection, step))
                        changes.workflows.add(step.workflow_id)
                        changes.account(UsageHistory.update(connection, step))
                        notifications.append(
                            events.StepEvent(
                                kind="step.completed",
                                execution_id=operation.request.execution_id,
                                step=step,
                                position=cursor.lastrowid,
                            )
                        )
                        assert plan.input_request is not None
                        sql.execute(
                            connection,
                            sql.Query.into(INPUTS).insert(
                                step.id, plan.input_request.model_dump_json()
                            ),
                        )
                        changes.pending_delta += int(
                            plan.input_request.status == "pending"
                        )
                    sql.execute(connection, sql.Query.from_(HEADS).delete())
                    sql.insert_many(connection, HEADS, plan.heads.items())
                    SQLiteWorkflowGrants.retain(connection, plan.retained_step_ids)
                    self._set_status(connection, "running", changes)
                    if base is not None:
                        changes.context_changed = True
                        changes.context = UsageHistory.root_context(
                            connection, base.summary.root_workflow_id or ""
                        )
                    operation = operation.model_copy(update={"status": "applied"})
                sql.execute(
                    connection,
                    sql.Query.update(BRANCHES)
                    .set(BRANCHES.payload, operation.model_dump_json())
                    .where(BRANCHES.id == operation.id),
                )
                changes.changed = changes.notify = True
                changes.execution_changed = True
                return operation

        return await self._mutate(apply, changes, notifications=notifications)

    async def get_execution(self) -> state.ExecutionRecord:
        def read(connection: sqlite3.Connection) -> state.ExecutionRecord:
            with connection:
                connection.execute("BEGIN")
                return self._execution(connection)

        return await self._run(read)

    async def usage_snapshot(self) -> tuple[int, usage.Totals]:
        return await self._invoke(UsageHistory.snapshot)

    @staticmethod
    def _execution_metadata(connection: sqlite3.Connection) -> ExecutionMetadata:
        row = connection.execute(
            "SELECT json_extract(payload, '$.id'), json_extract(payload, '$.status') "
            "FROM execution WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise KeyError("execution state")
        return ExecutionMetadata(id=row[0], status=row[1])

    @staticmethod
    def _execution(connection: sqlite3.Connection) -> state.ExecutionRecord:
        row = sql.execute(
            connection,
            sql.Query.from_(EXECUTION)
            .select(EXECUTION.payload)
            .where(EXECUTION.singleton == 1),
        ).fetchone()
        if row is None:
            raise KeyError("execution state")
        execution = state.ExecutionRecord.model_validate_json(row[0])
        heads = dict(
            sql.execute(
                connection,
                sql.Query.from_(HEADS).select(HEADS.workflow_id, HEADS.step_id),
            )
        )
        root_head = heads.get(execution.root_workflow_id)
        completion_id = None
        if (
            root_head is not None
            and SQLiteHistory._step(connection, root_head).operation.kind
            == "workflow.complete"
        ):
            completion_id = root_head
        return execution.model_copy(
            update={
                "checkpoint": execution.checkpoint.model_copy(update={"heads": heads}),
                "completion_step_id": completion_id,
            }
        )

    def _connect_readonly(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path.as_uri() + "?mode=ro", uri=True, timeout=30
        )
        try:
            if connection.execute("PRAGMA user_version").fetchone()[0] not in (1, 2):
                raise ValueError("unsupported history schema version")
            connection.execute("BEGIN")
            return connection
        except BaseException:
            connection.close()
            raise

    async def execution_summary(self) -> inspection.ExecutionSummary:
        """Read persisted totals and project context from the authoritative root path."""

        def read() -> inspection.ExecutionSummary:
            with closing(self._connect_readonly()) as connection:
                return summary_history.SummaryHistory.summary(connection)

        return await _offload(read)

    async def summary_snapshot(self) -> summary_history.Snapshot:
        """Prepare legacy projection metadata, then capture one committed revision."""
        await self._prepare()

        def read() -> summary_history.Snapshot:
            with closing(self._connect_readonly()) as connection:
                return summary_history.SummaryHistory.snapshot(connection)

        return await _offload(read)

    async def inspect_execution(self) -> inspection.ExecutionStatus:
        """Count all retained steps from one committed, read-only database snapshot."""

        def read() -> inspection.ExecutionStatus:
            with closing(self._connect_readonly()) as connection:
                execution = self._execution(connection)
                stats = inspection.ExecutionStats(
                    workflows=sql.execute(
                        connection,
                        sql.Query.from_(WORKFLOWS).select(sql.functions.Count("*")),
                    ).fetchone()[0],
                    steps=0,
                )
                cursor = sql.execute(
                    connection,
                    sql.Query.from_(STEPS).select(STEPS.payload).orderby(STEPS.rowid),
                )
                while rows := cursor.fetchmany(self.batch_size):
                    for row in rows:
                        step = models.WorkflowStep.model_validate_json(row[0])
                        stats.steps += 1
                        kind = step.operation.kind
                        stats.operations[kind] = stats.operations.get(kind, 0) + 1
                        outcome = step.outcome.kind
                        stats.outcomes[outcome] = stats.outcomes.get(outcome, 0) + 1
                return inspection.ExecutionStatus(execution=execution, stats=stats)

        return await _offload(read)

    async def export_execution(self, output: Path) -> None:
        """Write a header and all retained steps from one read-only snapshot to JSONL."""

        def export() -> None:
            with closing(self._connect_readonly()) as connection:
                header = inspection.ExportHeader(
                    execution=self._execution(connection),
                    workflows=[
                        models.Workflow.model_validate_json(row[0])
                        for row in sql.execute(
                            connection,
                            sql.Query.from_(WORKFLOWS)
                            .select(WORKFLOWS.payload)
                            .orderby(WORKFLOWS.rowid),
                        )
                    ],
                )
                with output.open("x", encoding="utf-8") as stream:
                    try:
                        stream.write(header.model_dump_json() + "\n")
                        cursor = sql.execute(
                            connection,
                            sql.Query.from_(STEPS)
                            .select(STEPS.payload)
                            .orderby(STEPS.rowid),
                        )
                        while rows := cursor.fetchmany(self.batch_size):
                            for row in rows:
                                step = models.WorkflowStep.model_validate_json(row[0])
                                stream.write(step.model_dump_json() + "\n")
                        stream.flush()
                    except BaseException:
                        output.unlink()
                        raise

        await _offload(export)

    async def query(
        self,
        query: queries.HistoryQuery,
        *,
        page_fits: typing.Optional[Callable[[queries.HistoryPage], bool]] = None,
    ) -> queries.HistoryPage:
        """Bound a snapshot window by count and an optional monotone size predicate.

        Keep at least one item so callers can report an oversized record instead
        of returning an empty page that cannot advance.
        """

        def read() -> queries.HistoryPage:
            with closing(self._connect_readonly()) as connection:
                execution = self._execution(connection)
                if execution.id != query.execution_id:
                    raise ValueError("execution identity mismatch")
                cursor = (
                    queries.HistoryCursor.decode(query.cursor, query.scope())
                    if query.cursor
                    else None
                )
                boundary = (
                    cursor.boundary
                    if cursor
                    else sql.execute(
                        connection,
                        sql.Query.from_(STEPS).select(
                            sql.functions.Coalesce(sql.functions.Max(STEPS.rowid), 0)
                        ),
                    ).fetchone()[0]
                )
                statement = (
                    sql.Query.from_(STEPS)
                    .select(STEPS.rowid, STEPS.payload)
                    .where(STEPS.rowid <= boundary)
                )
                if query.before_step_id is not None:
                    statement = statement.where(
                        STEPS.rowid
                        < self._step_position(connection, query.before_step_id)
                    )
                if cursor:
                    statement = statement.where(
                        STEPS.rowid < cursor.position
                        if query.selection == "tail"
                        else STEPS.rowid > cursor.position
                    )
                workflow_id = query.workflow_id
                if workflow_id is None and not query.include_children:
                    workflow_id = execution.root_workflow_id
                if workflow_id is not None:
                    statement = statement.where(STEPS.workflow_id == workflow_id)
                if query.step_ids is not None:
                    statement = statement.where(
                        STEPS.id.isin(sql.json_values(query.step_ids))
                    )
                if query.head_step_id is not None:
                    self._step(connection, query.head_step_id)
                    path = sql.Table("path")
                    ancestors = (
                        sql.Query.from_(STEPS)
                        .select(STEPS.id, STEPS.previous_step_id)
                        .where(STEPS.id == query.head_step_id)
                        .union(
                            sql.Query.from_(STEPS)
                            .join(path)
                            .on(STEPS.id == path.previous_step_id)
                            .select(STEPS.id, STEPS.previous_step_id)
                        )
                    )
                    statement = statement.with_(ancestors, "path").where(
                        STEPS.id.isin(sql.Query.from_(path).select(path.id))
                    )
                direction = (
                    sql.Order.desc if query.selection == "tail" else sql.Order.asc
                )
                rows = sql.execute(
                    connection,
                    statement.orderby(STEPS.rowid, order=direction).limit(
                        query.limit + 1
                    ),
                ).fetchall()
                selected = rows[: query.limit]
                items = [
                    queries.HistoryItem(
                        position=row[0],
                        step=models.WorkflowStep.model_validate_json(row[1]),
                    )
                    for row in selected
                ]

                def page_for(count: int) -> queries.HistoryPage:
                    next_cursor = None
                    if count and len(rows) > count:
                        next_cursor = queries.HistoryCursor(
                            scope=query.scope(),
                            boundary=boundary,
                            position=items[count - 1].position,
                        ).encode()
                    return queries.HistoryPage(
                        execution_id=execution.id,
                        items=sorted(items[:count], key=lambda item: item.position),
                        next_cursor=next_cursor,
                    )

                page = page_for(len(items))
                if page_fits is None or len(items) < 2 or page_fits(page):
                    return page
                low, high = 1, len(items) - 1
                while low < high:
                    middle = (low + high + 1) // 2
                    if page_fits(page_for(middle)):
                        low = middle
                    else:
                        high = middle - 1
                return page_for(low)

        return await _offload(read)

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        return await self._run(lambda connection: self._step(connection, step_id))

    async def get_step_position(self, step_id: str) -> int:
        return await self._run(
            lambda connection: self._step_position(connection, step_id)
        )

    @staticmethod
    def _step_position(connection: sqlite3.Connection, step_id: str) -> int:
        row = sql.execute(
            connection,
            sql.Query.from_(STEPS).select(STEPS.rowid).where(STEPS.id == step_id),
        ).fetchone()
        if row is None:
            raise KeyError(step_id)
        position: int = row[0]
        return position

    async def first_completion(
        self, workflow_ids: Sequence[str]
    ) -> typing.Optional[models.WorkflowStep]:
        def read(
            connection: sqlite3.Connection,
        ) -> typing.Optional[models.WorkflowStep]:
            row = sql.execute(
                connection,
                sql.Query.from_(STEPS)
                .join(HEADS)
                .on(STEPS.id == HEADS.step_id)
                .select(STEPS.payload)
                .where(HEADS.workflow_id.isin(sql.json_values(list(workflow_ids))))
                .where(
                    sql.json_extract(STEPS.payload, "$.operation.kind")
                    == "workflow.complete"
                )
                .where(
                    sql.json_extract(STEPS.payload, "$.outcome.kind").isin(
                        ["completed", "failed", "cancelled"]
                    )
                )
                .orderby(STEPS.rowid)
                .limit(1),
            ).fetchone()
            return (
                None if row is None else models.WorkflowStep.model_validate_json(row[0])
            )

        return await self._run(read)

    async def record_delivery(self, head_id: str, step_id: str) -> models.WorkflowStep:
        changes = summary_history.Changes()

        def record(connection: sqlite3.Connection) -> models.WorkflowStep:
            with self._transaction(connection, changes):
                head = self._step(connection, head_id)
                target = self._step(connection, step_id)
                updated = head.delivered(target)
                if updated == head:
                    return updated
                sql.execute(
                    connection,
                    sql.Query.update(STEPS)
                    .set(STEPS.payload, updated.model_dump_json())
                    .where(STEPS.id == head_id),
                )
                changes.changed = True
                return updated

        return await self._mutate(record, changes)

    @staticmethod
    def _input(
        connection: sqlite3.Connection, request_id: str
    ) -> mailbox.StoredInputRequest:
        row = sql.execute(
            connection,
            sql.Query.from_(INPUTS)
            .select(INPUTS.payload)
            .where(INPUTS.request_id == request_id),
        ).fetchone()
        if row is None:
            raise KeyError(request_id)
        return mailbox.StoredInputRequest.model_validate_json(row[0])

    async def register_input(
        self, request_id: str, spec: mailbox.InputSpec
    ) -> mailbox.InputRequest:
        """Register only against an existing pending activity, never an inbox."""
        changes = summary_history.Changes()

        def register(connection: sqlite3.Connection) -> mailbox.InputRequest:
            with self._transaction(connection, changes):
                step = self._step(connection, request_id)
                execution = self._execution_metadata(connection)
                if execution.status not in ("running", "paused"):
                    raise mailbox.MailboxConflict("execution is not running")
                if (
                    step.operation.kind not in (mailbox.KIND, "llm.tool")
                    or step.outcome.kind != "pending"
                ):
                    raise mailbox.MailboxConflict("step is not an input wait")
                try:
                    existing = self._input(connection, request_id)
                except KeyError:
                    existing = None
                if existing is not None:
                    if existing.specification() != spec:
                        raise mailbox.MailboxConflict("input request contract changed")
                    return existing.public()
                request = mailbox.StoredInputRequest(
                    **spec.model_dump(),
                    request_id=request_id,
                    execution_id=execution.id,
                    workflow_id=step.workflow_id,
                )
                sql.execute(
                    connection,
                    sql.Query.into(INPUTS).insert(
                        request_id, request.model_dump_json()
                    ),
                )
                changes.changed = changes.notify = True
                changes.pending_delta = 1
                changes.urgency = summary_history.Urgency.IMMEDIATE
                return request.public()

        return await self._mutate(register, changes)

    async def get_input(self, request_id: str) -> mailbox.InputRequest:
        request = await self._run(
            lambda connection: self._input(connection, request_id).public()
        )
        if request.status == "answered" and request.request_type == "tool.approval.v1":
            response = tool_policy.ToolApprovalResponse.model_validate(request.response)
            if response.scope == "workspace":
                await self.publish_authorization_rules()
        return request

    async def get_input_spec(self, request_id: str) -> mailbox.InputSpec:
        return await self._run(
            lambda connection: self._input(connection, request_id).specification()
        )

    async def authorization_rules(
        self, workflow_id: str, tool_name: typing.Optional[str] = None
    ) -> list[tool_policy.AuthorizationRule]:
        return await self._run(
            lambda connection: SQLiteWorkflowGrants.list(
                connection, workflow_id, tool_name
            )
        )

    async def save_authorization_rule(
        self,
        workflow_id: str,
        value: tool_policy.RuleInput,
        rule_id: typing.Optional[UUID] = None,
    ) -> tool_policy.AuthorizationRule:
        changes = summary_history.Changes()

        def save(connection: sqlite3.Connection) -> tool_policy.AuthorizationRule:
            with self._transaction(connection, changes):
                self._workflow(connection, workflow_id)
                rule = SQLiteWorkflowGrants.save(
                    connection, workflow_id, value, rule_id
                )
                changes.changed = True
                return rule

        return await self._mutate(save, changes)

    async def delete_authorization_rule(self, workflow_id: str, rule_id: UUID) -> None:
        changes = summary_history.Changes()

        def delete(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes):
                if (
                    sql.execute(
                        connection,
                        sql.Query.from_(RULES)
                        .delete()
                        .where(
                            (RULES.workflow_id == workflow_id)
                            & (RULES.id == str(rule_id))
                        ),
                    ).rowcount
                    != 1
                ):
                    raise KeyError(str(rule_id))
                changes.changed = True

        await self._mutate(delete, changes)

    async def approval_rule_id(self, request_id: str) -> typing.Optional[UUID]:
        await self.publish_authorization_rules()

        def read(connection: sqlite3.Connection) -> typing.Optional[UUID]:
            row = sql.execute(
                connection,
                sql.Query.from_(RECEIPTS)
                .select(RECEIPTS.rule_id)
                .where(RECEIPTS.request_id == request_id),
            ).fetchone()
            return UUID(row[0]) if row is not None else None

        return await self._run(read)

    async def publish_authorization_rules(self) -> None:
        if self._workspace_rules is None:
            return
        pending = await self._run(
            lambda connection: [
                tool_policy.AuthorizationRule.model_validate_json(row[0])
                for row in sql.execute(
                    connection,
                    sql.Query.from_(OUTBOX)
                    .select(OUTBOX.payload)
                    .where(OUTBOX.delivered == 0),
                )
            ]
        )
        for rule in pending:
            canonical_id = await self._workspace_rules.import_rule(rule)
            changes = summary_history.Changes()

            def delivered(connection: sqlite3.Connection) -> None:
                with self._transaction(connection, changes):
                    if not connection.execute(
                        "SELECT 1 FROM tool_rule_outbox WHERE id = ? AND delivered = 0",
                        (str(rule.id),),
                    ).fetchone():
                        return
                    sql.execute(
                        connection,
                        sql.Query.update(RECEIPTS)
                        .set(RECEIPTS.rule_id, str(canonical_id))
                        .where(
                            RECEIPTS.request_id
                            == sql.Query.from_(OUTBOX)
                            .select(OUTBOX.request_id)
                            .where(OUTBOX.id == str(rule.id))
                        ),
                    )
                    sql.execute(
                        connection,
                        sql.Query.update(OUTBOX)
                        .set(OUTBOX.delivered, 1)
                        .where(OUTBOX.id == str(rule.id)),
                    )
                    changes.changed = True

            await self._mutate(delivered, changes)

    async def list_inputs(self, query: mailbox.RequestQuery) -> mailbox.RequestPage:
        def read(connection: sqlite3.Connection) -> mailbox.RequestPage:
            with connection:
                connection.execute("BEGIN")
                execution = self._execution_metadata(connection)
                if execution.id != query.execution_id:
                    raise ValueError("execution identity mismatch")
                statement = (
                    sql.Query.from_(INPUTS)
                    .select(INPUTS.payload)
                    .where(sql.json_extract(INPUTS.payload, "$.status") == "pending")
                )
                for field, value in (
                    ("$.workflow_id", query.workflow_id),
                    ("$.channel", query.channel),
                ):
                    if value is not None:
                        statement = statement.where(
                            sql.json_extract(INPUTS.payload, field) == value
                        )
                if query.after is not None:
                    statement = statement.where(INPUTS.request_id > query.after)
                rows = sql.execute(
                    connection,
                    statement.orderby(INPUTS.request_id).limit(query.limit + 1),
                ).fetchall()
                requests = [
                    mailbox.StoredInputRequest.model_validate_json(row[0]).public()
                    for row in rows
                ]
                return mailbox.RequestPage(
                    requests=requests[: query.limit],
                    next_after=(
                        requests[query.limit - 1].request_id
                        if len(requests) > query.limit
                        else None
                    ),
                )

        return await self._run(read)

    async def answer_input(self, answer: mailbox.Answer) -> mailbox.InputRequest:
        """Commit the winning response and its retry receipt in one transaction."""
        changes = summary_history.Changes()

        def respond(connection: sqlite3.Connection) -> mailbox.InputRequest:
            with self._transaction(connection, changes):
                request = self._input(connection, answer.request_id)
                if request.execution_id != answer.execution_id:
                    raise mailbox.MailboxConflict("execution identity mismatch")
                if sql.execute(
                    connection,
                    sql.Query.from_(BRANCHES)
                    .select(1)
                    .where(
                        sql.json_extract(BRANCHES.payload, "$.status") == "accepted"
                    ),
                ).fetchone():
                    raise mailbox.MailboxConflict("branch transition is in progress")
                if request.status == "answered":
                    if (
                        request.idempotency_key == answer.idempotency_key
                        and request.response == answer.response
                    ):
                        return request.public()
                    raise mailbox.MailboxConflict("input request is already answered")
                execution = self._execution_metadata(connection)
                if request.status != "pending" or execution.status not in (
                    "running",
                    "paused",
                ):
                    raise mailbox.MailboxConflict("input request is closed")
                request.validate_response(answer.response)
                if request.request_type == "tool.approval.v1":
                    tool_policy.response_rule(request.payload, answer.response)
                    response = tool_policy.ToolApprovalResponse.model_validate(
                        answer.response
                    )
                    if response.scope == "workspace" and self._workspace_rules is None:
                        raise ValueError("workspace rule storage is unavailable")
                request = request.model_copy(
                    update={
                        "status": "answered",
                        "response": answer.response,
                        "idempotency_key": answer.idempotency_key,
                        "answered_at": state.timestamp(),
                    }
                )
                sql.execute(
                    connection,
                    sql.Query.update(INPUTS)
                    .set(INPUTS.payload, request.model_dump_json())
                    .where(INPUTS.request_id == request.request_id),
                )
                if execution.status == "paused":
                    self._set_status(connection, "running", changes)
                step = self._step(connection, request.request_id)
                SQLiteWorkflowGrants.record_answer(
                    connection,
                    operation_kind=step.operation.kind,
                    workflow_id=step.workflow_id,
                    request_id=request.request_id,
                    payload=request.payload,
                    response=answer.response,
                )
                changes.changed = changes.notify = True
                changes.pending_delta = -1
                return request.public()

        result = await self._mutate(respond, changes)
        await self.publish_authorization_rules()
        return result

    @staticmethod
    def _cancel_inputs(
        connection: sqlite3.Connection,
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]],
        changes: summary_history.Changes,
        request_id: typing.Optional[str] = None,
    ) -> None:
        statement = (
            sql.Query.update(INPUTS)
            .set(INPUTS.payload, sql.json_set(INPUTS.payload, "$.status", "cancelled"))
            .where(sql.json_extract(INPUTS.payload, "$.status") == "pending")
        )
        if request_id is not None:
            statement = statement.where(INPUTS.request_id == request_id)
        rows = sql.execute(
            connection, statement.returning(INPUTS.request_id)
        ).fetchall()
        if rows:
            changes.changed = changes.notify = True
            changes.pending_delta -= len(rows)
            notifications.append(
                mailbox.InputsCancelled(
                    execution_id=SQLiteHistory._execution_metadata(connection).id,
                    request_ids=sorted(row[0] for row in rows),
                )
            )

    async def cancel_inputs(self, request_id: typing.Optional[str] = None) -> None:
        changes = summary_history.Changes()
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def cancel(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes):
                self._cancel_inputs(connection, notifications, changes, request_id)

        await self._mutate(cancel, changes, notifications=notifications)

    async def append_step(self, step: models.WorkflowStep) -> None:
        payload = step.model_dump_json()
        changes = summary_history.Changes()

        def append(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes) as base:
                self._workflow(connection, step.workflow_id)
                if step.previous_step_id is not None:
                    previous = self._step(connection, step.previous_step_id)
                    if previous.workflow_id != step.workflow_id:
                        raise ValueError("predecessor belongs to another workflow")
                    if (
                        previous.outcome.kind in ("streaming", "pending")
                        and not previous.deferred_delivery
                        and step.resume_step_id is None
                    ):
                        raise ValueError(
                            "unfinished activity steps cannot have descendants"
                        )
                try:
                    sql.execute(
                        connection,
                        sql.Query.into(STEPS).insert(
                            step.id, step.workflow_id, step.previous_step_id, payload
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("duplicate step ID") from exc
                sql.execute(
                    connection,
                    sql.Query.into(HEADS)
                    .insert(step.workflow_id, step.id)
                    .on_conflict(HEADS.workflow_id)
                    .do_update(HEADS.step_id),
                )
                changes.workflows.update(UsageHistory.link(connection, step))
                changes.account(UsageHistory.update(connection, step))
                changes.changed = True
                changes.execution_changed = True
                changes.workflows.add(step.workflow_id)
                if (
                    base is not None
                    and base.summary.root_workflow_id == step.workflow_id
                ):
                    changes.context_changed = True
                    changes.context = UsageHistory.root_context(
                        connection, step.workflow_id
                    )

        await self._mutate(append, changes)

    async def update_step(self, step: models.WorkflowStep) -> None:
        await self.update_steps((step,))

    async def update_steps(self, steps: Sequence[models.WorkflowStep]) -> None:
        """Validate and persist a snapshot batch in one durable transaction."""
        snapshots = tuple((step, step.model_dump_json()) for step in steps)
        if not snapshots:
            return
        changes = summary_history.Changes()

        def update(connection: sqlite3.Connection) -> None:
            with self._transaction(connection, changes) as base:
                for step, payload in snapshots:
                    previous = self._step(connection, step.id)
                    if previous == step:
                        continue
                    if previous.outcome.kind not in ("streaming", "pending", "waiting"):
                        raise ValueError("terminal steps cannot be updated")
                    if (
                        previous.workflow_id,
                        previous.previous_step_id,
                        previous.operation,
                        previous.deferred_delivery,
                        previous.deliveries,
                    ) != (
                        step.workflow_id,
                        step.previous_step_id,
                        step.operation,
                        step.deferred_delivery,
                        step.deliveries,
                    ):
                        raise ValueError("step identity and links cannot change")
                    step.validate_authorization_update(previous)
                    connection.execute(
                        "UPDATE steps SET payload = ? WHERE id = ?",
                        (payload, step.id),
                    )
                    changes.account(UsageHistory.update(connection, step, previous))
                    changes.changed = True
                    if (
                        base is not None
                        and base.summary.root_workflow_id == step.workflow_id
                        and step.operation.kind == "llm.generate"
                        and UsageHistory.context(step) is not None
                    ):
                        changes.context_changed = True
                        changes.context = UsageHistory.root_context(
                            connection, step.workflow_id
                        )

        await self._mutate(update, changes)

    def _snapshot_path(self, workflow_id: str, head_step_id: str) -> sqlite3.Connection:
        """Materialize the selected path in SQLite temporary storage, not Python lists."""
        connection = self._connect()
        try:
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                "CREATE TEMP TABLE replay (depth INTEGER PRIMARY KEY, id TEXT UNIQUE, payload TEXT NOT NULL)"
            )
            with connection:
                connection.execute("BEGIN")
                self._workflow(connection, workflow_id)
                current: typing.Optional[str] = head_step_id
                depth = 0
                while current is not None:
                    step = self._step(connection, current)
                    if step.workflow_id != workflow_id:
                        raise ValueError("replay step belongs to another workflow")
                    if (
                        depth
                        and step.outcome.kind in ("streaming", "pending")
                        and not step.deferred_delivery
                    ):
                        raise ValueError("unfinished activity must be the replay head")
                    try:
                        sql.execute(
                            connection,
                            sql.Query.into(REPLAY).insert(
                                depth, step.id, step.model_dump_json()
                            ),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise ValueError("cycle in replay path") from exc
                    current = step.previous_step_id
                    depth += 1
            return connection
        except BaseException:
            connection.close()
            raise

    async def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        await self._prepare()
        connection: typing.Optional[sqlite3.Connection] = None

        def prepare() -> None:
            nonlocal connection
            connection = self._snapshot_path(workflow_id, head_step_id)

        try:
            await _offload(prepare)
            assert connection is not None
            database = connection
            cursor = await _offload(
                lambda: sql.execute(
                    database,
                    sql.Query.from_(REPLAY)
                    .select(REPLAY.payload)
                    .orderby(REPLAY.depth, order=sql.Order.desc),
                )
            )
            while True:
                rows = await _offload(lambda: cursor.fetchmany(self.batch_size))
                if not rows:
                    break
                for row in rows:
                    yield models.WorkflowStep.model_validate_json(row[0])
        finally:
            if connection is not None:
                await _offload(connection.close)
