"""Transactional SQLite history with replaceable unfinished activity snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import closing
from pathlib import Path
import sqlite3
import typing

from vectorops.runner import models, tool_policy
from vectorops.runner.runner import execution_state as state
from vectorops.runner.runner import inspection
from vectorops.runner.runner import history_query as queries
from vectorops.runner.runner import mailbox

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
    def __init__(self, path: typing.Union[str, Path], *, batch_size: int = 128) -> None:
        if batch_size < 1:
            raise ValueError("replay batch size must be positive")
        self.path = Path(path).expanduser().absolute()
        self.batch_size = batch_size
        self._ready = False
        self._initialization_lock = asyncio.Lock()

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
            connection.executescript(
                """
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
                PRAGMA user_version = 1;
                COMMIT;
                """
            )

    async def _run(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        await self._prepare()

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
        payload = workflow.model_dump_json()

        def save(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload FROM workflows WHERE id = ?", (workflow.id,)
                ).fetchone()
                if row is not None:
                    if models.Workflow.model_validate_json(row[0]) != workflow:
                        raise ValueError("workflow configuration conflict")
                    return
                connection.execute(
                    "INSERT INTO workflows VALUES (?, ?)", (workflow.id, payload)
                )

        await self._run(save)

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        return await self._run(
            lambda connection: self._workflow(connection, workflow_id)
        )

    async def save_execution(
        self, execution: state.ExecutionRecord, *, replace_heads: bool = False
    ) -> None:
        def save(connection: sqlite3.Connection) -> None:
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
                for workflow_id, step_id in execution.checkpoint.heads.items():
                    if self._step(connection, step_id).workflow_id != workflow_id:
                        raise ValueError("checkpoint head belongs to another workflow")
                connection.execute(
                    "INSERT INTO execution VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET payload = excluded.payload",
                    (execution.model_dump_json(),),
                )
                if existing is None or replace_heads:
                    connection.execute("DELETE FROM workflow_heads")
                    connection.executemany(
                        "INSERT INTO workflow_heads VALUES (?, ?)",
                        execution.checkpoint.heads.items(),
                    )

        await self._run(save)

    async def get_execution(self) -> state.ExecutionRecord:
        def read(connection: sqlite3.Connection) -> state.ExecutionRecord:
            with connection:
                connection.execute("BEGIN")
                return self._execution(connection)

        return await self._run(read)

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

    @staticmethod
    def _input(connection: sqlite3.Connection, request_id: str) -> mailbox.InputRequest:
        row = connection.execute(
            "SELECT payload FROM input_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise KeyError(request_id)
        return mailbox.InputRequest.model_validate_json(row[0])

    async def register_input(
        self, request_id: str, spec: mailbox.InputSpec
    ) -> mailbox.InputRequest:
        """Register only against an existing pending activity, never an inbox."""

        def register(connection: sqlite3.Connection) -> mailbox.InputRequest:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                step = self._step(connection, request_id)
                execution = self._execution(connection)
                if execution.status != "running":
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
                    if (
                        mailbox.InputSpec.model_validate(
                            existing.model_dump(
                                include=set(mailbox.InputSpec.model_fields)
                            )
                        )
                        != spec
                    ):
                        raise mailbox.MailboxConflict("input request contract changed")
                    return existing
                request = mailbox.InputRequest(
                    **spec.model_dump(),
                    request_id=request_id,
                    execution_id=execution.id,
                    workflow_id=step.workflow_id,
                )
                connection.execute(
                    "INSERT INTO input_requests VALUES (?, ?)",
                    (request_id, request.model_dump_json()),
                )
                return request

        return await self._run(register)

    async def get_input(self, request_id: str) -> mailbox.InputRequest:
        return await self._run(lambda connection: self._input(connection, request_id))

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
                    mailbox.InputRequest.model_validate_json(row[0]) for row in rows
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
                if request.status == "answered":
                    if (
                        request.idempotency_key == answer.idempotency_key
                        and request.response == answer.response
                    ):
                        return request
                    raise mailbox.MailboxConflict("input request is already answered")
                if (
                    request.status != "pending"
                    or self._execution(connection).status != "running"
                ):
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
                return request

        return await self._run(respond)

    async def cancel_inputs(self, request_id: typing.Optional[str] = None) -> None:
        def cancel(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE input_requests SET payload = json_set(payload, '$.status', 'cancelled') "
                    "WHERE json_extract(payload, '$.status') = 'pending' AND (? IS NULL OR request_id = ?)",
                    (request_id, request_id),
                )

        await self._run(cancel)

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

        await self._run(append)

    async def update_step(self, step: models.WorkflowStep) -> None:
        payload = step.model_dump_json()

        def update(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                previous = self._step(connection, step.id)
                if previous.outcome.kind not in ("streaming", "pending", "waiting"):
                    raise ValueError("terminal steps cannot be updated")
                if (
                    previous.workflow_id,
                    previous.previous_step_id,
                    previous.operation,
                    previous.deferred_delivery,
                ) != (
                    step.workflow_id,
                    step.previous_step_id,
                    step.operation,
                    step.deferred_delivery,
                ):
                    raise ValueError("step identity and links cannot change")
                connection.execute(
                    "UPDATE steps SET payload = ? WHERE id = ?", (payload, step.id)
                )

        await self._run(update)

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
