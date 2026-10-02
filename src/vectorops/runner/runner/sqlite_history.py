"""Transactional SQLite history with replaceable unfinished activity snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import closing
from pathlib import Path
import sqlite3
import typing

from vectorops.runner import models

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
                await _offload(self._initialize)
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
                PRAGMA user_version = 1;
                COMMIT;
                """)

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

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        return await self._run(lambda connection: self._step(connection, step_id))

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
                    if previous.outcome.kind == "streaming":
                        raise ValueError(
                            "unfinished streaming steps cannot have descendants"
                        )
                try:
                    connection.execute(
                        "INSERT INTO steps VALUES (?, ?, ?, ?)",
                        (step.id, step.workflow_id, step.previous_step_id, payload),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("duplicate step ID") from exc

        await self._run(append)

    async def update_step(self, step: models.WorkflowStep) -> None:
        payload = step.model_dump_json()

        def update(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                previous = self._step(connection, step.id)
                if previous.outcome.kind != "streaming":
                    raise ValueError("terminal steps cannot be updated")
                if (
                    previous.workflow_id,
                    previous.previous_step_id,
                    previous.operation,
                ) != (step.workflow_id, step.previous_step_id, step.operation):
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
                    if depth and step.outcome.kind == "streaming":
                        raise ValueError(
                            "unfinished streaming activity must be the replay head"
                        )
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
