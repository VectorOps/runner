"""Transactional SQLite history with replaceable unfinished activity snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import closing
from pathlib import Path
import sqlite3
import typing
from uuid import uuid4

from vectorops.runner import models, tool_policy, usage
from vectorops.runner.runner import execution_state as state
from vectorops.runner.runner import inspection
from vectorops.runner.runner import history_query as queries
from vectorops.runner.runner import mailbox
from vectorops.runner.runner import branching
from vectorops.runner.runner import events
from vectorops.runner import diagnostics
from vectorops.runner.runner.sqlite_writer import SQLiteWriter
from vectorops.runner.runner.usage_history import UsageHistory
from vectorops.runner.runner.usage_store import UsageStore

T = typing.TypeVar("T")


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
        change_sink: typing.Optional[events.HistoryChangeSink] = None,
        event_sink: typing.Optional[events.StepEventSink] = None,
        mailbox_sink: typing.Optional[mailbox.EventSink] = None,
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
        self._change_sink = change_sink
        self._event_sink = event_sink
        self._mailbox_sink = mailbox_sink

    async def __aenter__(self) -> typing.Self:
        if self._closed or self._writer is not None:
            raise RuntimeError("history writer is already open or closed")
        await self._prepare()
        self._writer = SQLiteWriter(self._connect)
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        if self._writer is not None:
            await self._writer.aclose()

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
            if version not in (0, 1):
                raise ValueError(f"unsupported history schema version: {version}")
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
                CREATE TABLE IF NOT EXISTS tool_approval_grants (
                    workflow_id TEXT NOT NULL REFERENCES workflows(id),
                    tool_name TEXT NOT NULL,
                    request_id TEXT NOT NULL REFERENCES input_requests(request_id),
                    PRIMARY KEY (workflow_id, tool_name)
                );
                CREATE TABLE IF NOT EXISTS branch_operations (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL
                );
                PRAGMA user_version = 1;
                COMMIT;
                """)
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                UsageHistory.initialize(connection)

    async def _run(
        self,
        operation: Callable[[sqlite3.Connection], T],
        *,
        account: bool = False,
        workflow_ids: tuple[str, ...] = (),
        notify: bool = False,
        notifications: typing.Optional[
            list[typing.Union[events.StepEvent, mailbox.InputsCancelled]]
        ] = None,
    ) -> T:
        if notifications is not None or (
            self._change_sink is not None and (notify or workflow_ids or account)
        ):
            changed: typing.Optional[tuple[str, ...]] = None
            committed: tuple[
                typing.Union[events.StepEvent, mailbox.InputsCancelled], ...
            ] = ()

            def tracked(connection: sqlite3.Connection) -> T:
                nonlocal changed, committed
                before = connection.total_changes
                result = operation(connection)
                committed = tuple(notifications or ())
                if self._change_sink is not None and connection.total_changes != before:
                    identifiers = set(workflow_ids)
                    if account:
                        for identifier in workflow_ids:
                            identifiers.update(
                                UsageHistory.ancestors(connection, identifier)
                            )
                    changed = tuple(sorted(identifiers))
                return result

            try:
                return await self._accounted(tracked, account=account)
            finally:
                for event in committed:
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
                if changed is not None:
                    assert self._change_sink is not None
                    try:
                        self._change_sink(changed)
                    except Exception as exc:
                        diagnostics.log(
                            "history.sink_failed",
                            level=diagnostics.Level.ERROR,
                            error=exc,
                        )
        return await self._accounted(operation, account=account)

    async def _accounted(
        self, operation: Callable[[sqlite3.Connection], T], *, account: bool
    ) -> T:
        if self._usage_store is None or not account:
            return await self._invoke(operation)
        store = self._usage_store
        async with store.lock:

            async def persist() -> T:
                result = await self._invoke(operation)
                revision, total = await self._invoke(UsageHistory.snapshot)
                await store.record(self.path.parent.name, revision, total)
                return result

            task = asyncio.create_task(persist())
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                try:
                    await task
                finally:
                    raise

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
        row = connection.execute(
            "SELECT payload FROM workflows WHERE id = ?", (workflow_id,)
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

        def save(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload FROM workflows WHERE id = ?", (workflow.id,)
                ).fetchone()
                if row is not None:
                    if not models.Workflow.model_validate_json(
                        row[0]
                    ).same_configuration(workflow):
                        raise ValueError("workflow configuration conflict")
                    return
                connection.execute(
                    "INSERT INTO workflows VALUES (?, ?)", (workflow.id, payload)
                )

        await self._run(save, workflow_ids=(workflow.id,))

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        return await self._run(
            lambda connection: self._workflow(connection, workflow_id)
        )

    async def set_workflow_name(self, workflow_id: str, name: str) -> models.Workflow:
        """Set presentation metadata once without replacing execution configuration."""

        def update(connection: sqlite3.Connection) -> models.Workflow:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                workflow = self._workflow(connection, workflow_id)
                if workflow.name is not None:
                    return workflow
                updated = models.Workflow.model_validate(
                    {**workflow.model_dump(), "name": name}
                )
                connection.execute(
                    "UPDATE workflows SET payload = ? WHERE id = ?",
                    (updated.model_dump_json(), workflow_id),
                )
                return updated

        return await self._run(update, workflow_ids=(workflow_id,))

    async def first_input(
        self, workflow_id: str, request_type: str
    ) -> typing.Optional[mailbox.InputRequest]:
        """Read the first accepted input, ignoring requests with no user response."""

        def read(
            connection: sqlite3.Connection,
        ) -> typing.Optional[mailbox.InputRequest]:
            row = connection.execute(
                "SELECT payload FROM input_requests "
                "WHERE json_extract(payload, '$.workflow_id') = ? "
                "AND json_extract(payload, '$.request_type') = ? "
                "AND json_extract(payload, '$.status') = 'answered' "
                "ORDER BY json_extract(payload, '$.answered_at'), rowid LIMIT 1",
                (workflow_id, request_type),
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
        def save(connection: sqlite3.Connection) -> None:
            saved = execution
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                self._workflow(connection, execution.root_workflow_id)
                existing = connection.execute(
                    "SELECT payload FROM execution WHERE singleton = 1"
                ).fetchone()
                if existing is not None:
                    previous = state.ExecutionRecord.model_validate_json(existing[0])
                    if (previous.id, previous.root_workflow_id) != (
                        execution.id,
                        execution.root_workflow_id,
                    ):
                        raise ValueError("execution identity cannot change")
                    if preserve_status:
                        saved = execution.model_copy(update={"status": previous.status})
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
                connection.execute(
                    "INSERT INTO execution VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET payload = excluded.payload",
                    (saved.model_dump_json(),),
                )
                if existing is None or replace_heads:
                    connection.execute("DELETE FROM workflow_heads")
                    connection.executemany(
                        "INSERT INTO workflow_heads VALUES (?, ?)",
                        execution.checkpoint.heads.items(),
                    )

        await self._run(save, notify=True)

    async def set_paused(self, requests: set[str]) -> bool:
        """Pause only if every observed dependency is still unanswered at commit."""
        if not requests:
            return False

        def pause(connection: sqlite3.Connection) -> bool:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                execution = self._execution(connection)
                if execution.status != "running" or any(
                    self._input(connection, request).status != "pending"
                    for request in requests
                ):
                    return False
                self._set_status(connection, "paused")
                return True

        return await self._run(pause, notify=True)

    @staticmethod
    def _set_status(
        connection: sqlite3.Connection, status: state.ExecutionStatus
    ) -> None:
        connection.execute(
            "UPDATE execution SET payload = json_set(payload, '$.status', ?, '$.updated_at', ?) "
            "WHERE singleton = 1",
            (status, state.timestamp()),
        )

    async def wake_execution(self) -> bool:
        def wake(connection: sqlite3.Connection) -> bool:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                if self._execution(connection).status != "paused":
                    return False
                self._set_status(connection, "running")
                return True

        return await self._run(wake, notify=True)

    async def stop_execution(self) -> None:
        """Cancel an unloaded execution and its requests in one transaction."""
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def stop(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                if self._execution(connection).status not in ("running", "paused"):
                    raise ValueError("execution is not active")
                self._cancel_inputs(connection, notifications)
                self._set_status(connection, "cancelled")

        await self._run(stop, notify=True, notifications=notifications)

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

        def accept(connection: sqlite3.Connection) -> branching.Operation:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload FROM branch_operations WHERE idempotency_key = ?",
                    (request.idempotency_key,),
                ).fetchone()
                if row is not None:
                    previous = branching.Operation.model_validate_json(row[0])
                    if previous.request != request:
                        raise branching.BranchConflict(
                            "idempotency key already used for different arguments"
                        )
                    return previous
                if connection.execute(
                    "SELECT 1 FROM branch_operations WHERE json_extract(payload, '$.status') = 'accepted'"
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
                connection.execute(
                    "INSERT INTO branch_operations VALUES (?, ?, ?)",
                    (
                        operation.id,
                        request.idempotency_key,
                        operation.model_dump_json(),
                    ),
                )
                return operation

        return await self._run(accept)

    async def pending_branch(self) -> bool:
        return await self._run(
            lambda connection: connection.execute(
                "SELECT 1 FROM branch_operations WHERE json_extract(payload, '$.status') = 'accepted'"
            ).fetchone()
            is not None
        )

    async def apply_branch(self) -> typing.Optional[branching.Operation]:
        """Commit path selection and synthetic input atomically after writer cleanup."""
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def apply(
            connection: sqlite3.Connection,
        ) -> typing.Optional[branching.Operation]:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload FROM branch_operations WHERE json_extract(payload, '$.status') = 'accepted'"
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
                    self._cancel_inputs(connection, notifications)
                    step = plan.input_step
                    if step is not None:
                        cursor = connection.execute(
                            "INSERT INTO steps VALUES (?, ?, ?, ?)",
                            (
                                step.id,
                                step.workflow_id,
                                step.previous_step_id,
                                step.model_dump_json(),
                            ),
                        )
                        assert cursor.lastrowid is not None
                        notifications.append(
                            events.StepEvent(
                                kind="step.completed",
                                execution_id=operation.request.execution_id,
                                step=step,
                                position=cursor.lastrowid,
                            )
                        )
                        assert plan.input_request is not None
                        connection.execute(
                            "INSERT INTO input_requests VALUES (?, ?)",
                            (step.id, plan.input_request.model_dump_json()),
                        )
                    connection.execute("DELETE FROM workflow_heads")
                    connection.executemany(
                        "INSERT INTO workflow_heads VALUES (?, ?)", plan.heads.items()
                    )
                    grants = connection.execute(
                        "SELECT request_id FROM tool_approval_grants"
                    ).fetchall()
                    connection.executemany(
                        "DELETE FROM tool_approval_grants WHERE request_id = ?",
                        [
                            (row[0],)
                            for row in grants
                            if row[0] not in plan.retained_step_ids
                        ],
                    )
                    self._set_status(connection, "running")
                    operation = operation.model_copy(update={"status": "applied"})
                connection.execute(
                    "UPDATE branch_operations SET payload = ? WHERE id = ?",
                    (operation.model_dump_json(), operation.id),
                )
                return operation

        return await self._run(apply, notify=True, notifications=notifications)

    async def get_execution(self) -> state.ExecutionRecord:
        def read(connection: sqlite3.Connection) -> state.ExecutionRecord:
            with connection:
                connection.execute("BEGIN")
                return self._execution(connection)

        return await self._run(read)

    async def usage_snapshot(self) -> tuple[int, usage.Totals]:
        return await self._invoke(UsageHistory.snapshot)

    @staticmethod
    def _execution(connection: sqlite3.Connection) -> state.ExecutionRecord:
        row = connection.execute(
            "SELECT payload FROM execution WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise KeyError("execution state")
        execution = state.ExecutionRecord.model_validate_json(row[0])
        heads = dict(
            connection.execute("SELECT workflow_id, step_id FROM workflow_heads")
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
            if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise ValueError("unsupported history schema version")
            connection.execute("BEGIN")
            return connection
        except BaseException:
            connection.close()
            raise

    async def execution_summary(self) -> inspection.ExecutionSummary:
        """Read persisted totals and project context from the authoritative root path."""

        await self._prepare()

        def read() -> inspection.ExecutionSummary:
            with closing(self._connect_readonly()) as connection:
                row = connection.execute(
                    "SELECT payload FROM execution WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    raise KeyError("execution state")
                execution = state.ExecutionRecord.model_validate_json(row[0])
                workflow = self._workflow(connection, execution.root_workflow_id)
                pending = connection.execute(
                    "SELECT COUNT(*) FROM input_requests WHERE json_extract(payload, '$.status') = 'pending'"
                ).fetchone()[0]
                return inspection.ExecutionSummary(
                    execution_id=execution.id,
                    reference=workflow.definition_ref,
                    root_workflow_id=workflow.id,
                    name=workflow.name,
                    status=execution.status,
                    pending_inputs=pending,
                    usage=usage.ExecutionUsage(
                        totals=UsageHistory.snapshot(connection)[1],
                        root_context=UsageHistory.root_context(connection, workflow.id),
                    ),
                )

        return await _offload(read)

    async def inspect_execution(self) -> inspection.ExecutionStatus:
        """Count all retained steps from one committed, read-only database snapshot."""

        def read() -> inspection.ExecutionStatus:
            with closing(self._connect_readonly()) as connection:
                execution = self._execution(connection)
                stats = inspection.ExecutionStats(
                    workflows=connection.execute(
                        "SELECT COUNT(*) FROM workflows"
                    ).fetchone()[0],
                    steps=0,
                )
                cursor = connection.execute("SELECT payload FROM steps ORDER BY rowid")
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
                        for row in connection.execute(
                            "SELECT payload FROM workflows ORDER BY rowid"
                        )
                    ],
                )
                with output.open("x", encoding="utf-8") as stream:
                    try:
                        stream.write(header.model_dump_json() + "\n")
                        cursor = connection.execute(
                            "SELECT payload FROM steps ORDER BY rowid"
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

    async def query(self, query: queries.HistoryQuery) -> queries.HistoryPage:
        """Read a bounded window; cursors freeze membership, not mutable snapshots."""

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
                    else connection.execute(
                        "SELECT COALESCE(MAX(rowid), 0) FROM steps"
                    ).fetchone()[0]
                )
                clauses = ["s.rowid <= ?"]
                values: list[typing.Union[str, int]] = [boundary]
                if cursor:
                    clauses.append(
                        "s.rowid < ?" if query.selection == "tail" else "s.rowid > ?"
                    )
                    values.append(cursor.position)
                workflow_id = query.workflow_id
                if workflow_id is None and not query.include_children:
                    workflow_id = execution.root_workflow_id
                if workflow_id is not None:
                    clauses.append("s.workflow_id = ?")
                    values.append(workflow_id)
                if query.step_ids is not None:
                    clauses.append(
                        "s.id IN (" + ",".join("?" for _ in query.step_ids) + ")"
                    )
                    values.extend(query.step_ids)
                prefix = ""
                if query.head_step_id is not None:
                    self._step(connection, query.head_step_id)
                    prefix = "WITH RECURSIVE path(id, previous_step_id) AS (SELECT id, previous_step_id FROM steps WHERE id = ? UNION SELECT s.id, s.previous_step_id FROM steps s JOIN path p ON s.id = p.previous_step_id) "
                    clauses.append("s.id IN (SELECT id FROM path)")
                    values.insert(0, query.head_step_id)
                direction = "DESC" if query.selection == "tail" else "ASC"
                rows = connection.execute(
                    prefix
                    + "SELECT s.rowid, s.payload FROM steps s WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY s.rowid "
                    + direction
                    + " LIMIT ?",
                    [*values, query.limit + 1],
                ).fetchall()
                selected = rows[: query.limit]
                next_cursor = None
                if len(rows) > query.limit:
                    next_cursor = queries.HistoryCursor(
                        scope=query.scope(), boundary=boundary, position=selected[-1][0]
                    ).encode()
                items = [
                    queries.HistoryItem(
                        position=row[0],
                        step=models.WorkflowStep.model_validate_json(row[1]),
                    )
                    for row in selected
                ]
                items.sort(key=lambda item: item.position)
                return queries.HistoryPage(
                    execution_id=execution.id, items=items, next_cursor=next_cursor
                )

        return await _offload(read)

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        return await self._run(lambda connection: self._step(connection, step_id))

    async def get_step_position(self, step_id: str) -> int:
        def read(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                "SELECT rowid FROM steps WHERE id = ?", (step_id,)
            ).fetchone()
            if row is None:
                raise KeyError(step_id)
            position: int = row[0]
            return position

        return await self._run(read)

    async def record_delivery(self, head_id: str, step_id: str) -> models.WorkflowStep:
        def record(connection: sqlite3.Connection) -> models.WorkflowStep:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                head = self._step(connection, head_id)
                target = self._step(connection, step_id)
                updated = head.delivered(target)
                connection.execute(
                    "UPDATE steps SET payload = ? WHERE id = ?",
                    (updated.model_dump_json(), head_id),
                )
                return updated

        return await self._run(record)

    @staticmethod
    def _input(
        connection: sqlite3.Connection, request_id: str
    ) -> mailbox.StoredInputRequest:
        row = connection.execute(
            "SELECT payload FROM input_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise KeyError(request_id)
        return mailbox.StoredInputRequest.model_validate_json(row[0])

    async def register_input(
        self, request_id: str, spec: mailbox.InputSpec
    ) -> mailbox.InputRequest:
        """Register only against an existing pending activity, never an inbox."""

        def register(connection: sqlite3.Connection) -> mailbox.InputRequest:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                step = self._step(connection, request_id)
                execution = self._execution(connection)
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
                connection.execute(
                    "INSERT INTO input_requests VALUES (?, ?)",
                    (request_id, request.model_dump_json()),
                )
                return request.public()

        return await self._run(register, notify=True)

    async def get_input(self, request_id: str) -> mailbox.InputRequest:
        return await self._run(
            lambda connection: self._input(connection, request_id).public()
        )

    async def get_input_spec(self, request_id: str) -> mailbox.InputSpec:
        return await self._run(
            lambda connection: self._input(connection, request_id).specification()
        )

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool:
        return await self._run(
            lambda connection: connection.execute(
                "SELECT 1 FROM tool_approval_grants WHERE workflow_id = ? AND tool_name = ?",
                (workflow_id, tool_name),
            ).fetchone()
            is not None
        )

    async def list_inputs(self, query: mailbox.RequestQuery) -> mailbox.RequestPage:
        def read(connection: sqlite3.Connection) -> mailbox.RequestPage:
            with connection:
                connection.execute("BEGIN")
                execution = self._execution(connection)
                if execution.id != query.execution_id:
                    raise ValueError("execution identity mismatch")
                clauses = ["json_extract(payload, '$.status') = 'pending'"]
                values: list[typing.Union[str, int]] = []
                for field, value in (
                    ("workflow_id", query.workflow_id),
                    ("channel", query.channel),
                ):
                    if value is not None:
                        clauses.append(f"json_extract(payload, '$.{field}') = ?")
                        values.append(value)
                if query.after is not None:
                    clauses.append("request_id > ?")
                    values.append(query.after)
                rows = connection.execute(
                    "SELECT payload FROM input_requests WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY request_id LIMIT ?",
                    [*values, query.limit + 1],
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

        def respond(connection: sqlite3.Connection) -> mailbox.InputRequest:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                request = self._input(connection, answer.request_id)
                if request.execution_id != answer.execution_id:
                    raise mailbox.MailboxConflict("execution identity mismatch")
                if connection.execute(
                    "SELECT 1 FROM branch_operations WHERE json_extract(payload, '$.status') = 'accepted'"
                ).fetchone():
                    raise mailbox.MailboxConflict("branch transition is in progress")
                if request.status == "answered":
                    if (
                        request.idempotency_key == answer.idempotency_key
                        and request.response == answer.response
                    ):
                        return request.public()
                    raise mailbox.MailboxConflict("input request is already answered")
                if request.status != "pending" or self._execution(
                    connection
                ).status not in ("running", "paused"):
                    raise mailbox.MailboxConflict("input request is closed")
                request.validate_response(answer.response)
                request = request.model_copy(
                    update={
                        "status": "answered",
                        "response": answer.response,
                        "idempotency_key": answer.idempotency_key,
                        "answered_at": state.timestamp(),
                    }
                )
                connection.execute(
                    "UPDATE input_requests SET payload = ? WHERE request_id = ?",
                    (request.model_dump_json(), request.request_id),
                )
                if self._execution(connection).status == "paused":
                    self._set_status(connection, "running")
                step = self._step(connection, request.request_id)
                if step.operation.kind == "llm.tool":
                    tool_name = tool_policy.workflow_grant(
                        request.payload, answer.response
                    )
                    if tool_name is not None:
                        connection.execute(
                            "INSERT INTO tool_approval_grants VALUES (?, ?, ?) "
                            "ON CONFLICT(workflow_id, tool_name) DO NOTHING",
                            (step.workflow_id, tool_name, request.request_id),
                        )
                return request.public()

        return await self._run(respond, notify=True)

    @staticmethod
    def _cancel_inputs(
        connection: sqlite3.Connection,
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]],
        request_id: typing.Optional[str] = None,
    ) -> None:
        rows = connection.execute(
            "UPDATE input_requests SET payload = json_set(payload, '$.status', 'cancelled') "
            "WHERE json_extract(payload, '$.status') = 'pending' AND (? IS NULL OR request_id = ?) "
            "RETURNING request_id",
            (request_id, request_id),
        ).fetchall()
        if rows:
            notifications.append(
                mailbox.InputsCancelled(
                    execution_id=SQLiteHistory._execution(connection).id,
                    request_ids=sorted(row[0] for row in rows),
                )
            )

    async def cancel_inputs(self, request_id: typing.Optional[str] = None) -> None:
        notifications: list[typing.Union[events.StepEvent, mailbox.InputsCancelled]] = (
            []
        )

        def cancel(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                self._cancel_inputs(connection, notifications, request_id)

        await self._run(cancel, notify=True, notifications=notifications)

    async def append_step(self, step: models.WorkflowStep) -> None:
        payload = step.model_dump_json()

        def append(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
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
                    connection.execute(
                        "INSERT INTO steps VALUES (?, ?, ?, ?)",
                        (step.id, step.workflow_id, step.previous_step_id, payload),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("duplicate step ID") from exc
                connection.execute(
                    "INSERT INTO workflow_heads VALUES (?, ?) ON CONFLICT(workflow_id) DO UPDATE SET step_id = excluded.step_id",
                    (step.workflow_id, step.id),
                )
                UsageHistory.link(connection, step)
                UsageHistory.update(connection, step)

        await self._run(append, account=True, workflow_ids=(step.workflow_id,))

    async def update_step(self, step: models.WorkflowStep) -> None:
        await self.update_steps((step,))

    async def update_steps(self, steps: Sequence[models.WorkflowStep]) -> None:
        """Validate and persist a snapshot batch in one durable transaction."""
        snapshots = tuple((step, step.model_dump_json()) for step in steps)
        if not snapshots:
            return

        def update(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                for step, payload in snapshots:
                    previous = self._step(connection, step.id)
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
                        "UPDATE steps SET payload = ? WHERE id = ?", (payload, step.id)
                    )
                    UsageHistory.update(connection, step, previous)

        await self._run(
            update,
            account=True,
            workflow_ids=tuple({step.workflow_id for step in steps}),
        )

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
                        connection.execute(
                            "INSERT INTO replay VALUES (?, ?, ?)",
                            (depth, step.id, step.model_dump_json()),
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
                lambda: database.execute(
                    "SELECT payload FROM replay ORDER BY depth DESC"
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
