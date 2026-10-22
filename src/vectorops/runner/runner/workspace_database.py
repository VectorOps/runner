"""Transactional workspace persistence with bounded, scoped SQLite ownership."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from contextlib import closing
from pathlib import Path
import sqlite3
import time
import typing

from vectorops.runner.runner import sql
from vectorops.runner.runner.resources import finish_cleanup
from vectorops.runner.runner.sqlite_writer import SQLiteWriter

DOCUMENTS: typing.Final = sql.Table("documents")
REQUESTS: typing.Final = sql.Table("requests")
INDEX: typing.Final = sql.Table("execution_index")
STATE: typing.Final = sql.Table("index_state")


class WorkspaceDatabase:
    def __init__(self, data_path: Path) -> None:
        self.path = data_path / "workspace.sqlite3"
        self._writer: typing.Optional[SQLiteWriter] = None

    def _connect(self) -> sqlite3.Connection:
        """Retry WAL setup contention off the event loop, with a bounded deadline."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            connection.execute("PRAGMA busy_timeout = 30000")
            deadline = time.monotonic() + 30
            while True:
                try:
                    connection.execute("PRAGMA journal_mode = WAL")
                    break
                except sqlite3.OperationalError as exc:
                    if (
                        exc.sqlite_errorcode
                        not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(0.01)
            connection.execute("PRAGMA synchronous = FULL")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2, 3, 4):
                raise ValueError("unsupported workspace database schema version")
            if version < 4:
                with connection:
                    connection.execute("BEGIN IMMEDIATE")
                    current = connection.execute("PRAGMA user_version").fetchone()[0]
                    if current == 0:
                        self._initialize(connection)
                    elif current == 1:
                        self._initialize_summaries(connection)
                        sql.execute(
                            connection,
                            sql.Query.update(STATE)
                            .set(STATE.generation, STATE.generation + 1)
                            .set(STATE.refreshed_at, None)
                            .where(STATE.id == 1),
                        )
                    if current < 3:
                        self._initialize_worktrees(connection)
                    if current < 4:
                        self._initialize_targets(connection)
            return connection
        except BaseException:
            connection.close()
            raise

    def _initialize(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE documents (name TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE requests (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE execution_index (execution_id TEXT PRIMARY KEY, updated_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE INDEX execution_order ON execution_index (updated_at DESC, execution_id ASC)"
        )
        connection.execute(
            "CREATE TABLE index_state (id INTEGER PRIMARY KEY CHECK (id = 1), generation INTEGER NOT NULL, refreshed_at REAL)"
        )
        sql.execute(connection, sql.Query.into(STATE).insert(1, 0, None))
        self._initialize_summaries(connection)

    @staticmethod
    def _initialize_worktrees(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS worktrees (id TEXT PRIMARY KEY, operation_key TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL, payload TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS worktree_uses (id TEXT PRIMARY KEY, worktree_id TEXT NOT NULL, execution_id TEXT)"
        )
        connection.execute("PRAGMA user_version = 3")

    @staticmethod
    def _initialize_targets(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS execution_targets (execution_id TEXT NOT NULL, "
            "root TEXT NOT NULL, worktree_id TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, root, worktree_id))"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS execution_targets_worktree ON execution_targets (worktree_id, execution_id)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS execution_targets_root ON execution_targets (root, execution_id)"
        )
        connection.execute("PRAGMA user_version = 4")

    @staticmethod
    def _initialize_summaries(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE execution_summaries (execution_id TEXT PRIMARY KEY, "
            "source_id TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE summary_tombstones (execution_id TEXT NOT NULL, source_id TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, source_id))"
        )
        connection.execute("PRAGMA user_version = 2")

    def run_sync[T](self, operation: Callable[[sqlite3.Connection], T]) -> T:
        """Support startup loading off the event loop without retaining a connection."""
        with closing(self._connect()) as connection:
            return operation(connection)

    async def run[T](self, operation: Callable[[sqlite3.Connection], T]) -> T:
        if self._writer is not None:
            return await self._writer.run(operation)
        writer = SQLiteWriter(self._connect)
        try:
            return await writer.run(operation)
        finally:
            await writer.aclose()

    async def __aenter__(self) -> typing.Self:
        if self._writer is not None:
            raise RuntimeError("workspace database is already open")
        self._writer = SQLiteWriter(self._connect)
        return self

    async def __aexit__(self, *args: object) -> None:
        writer = self._writer
        if writer is not None:
            try:
                await finish_cleanup(asyncio.create_task(writer.aclose()))
            finally:
                self._writer = None

    @staticmethod
    def document(
        connection: sqlite3.Connection, name: str, initial: Callable[[], str]
    ) -> str:
        row = sql.execute(
            connection,
            sql.Query.from_(DOCUMENTS)
            .select(DOCUMENTS.payload)
            .where(DOCUMENTS.name == name),
        ).fetchone()
        if row is not None:
            return str(row[0])
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            row = sql.execute(
                connection,
                sql.Query.from_(DOCUMENTS)
                .select(DOCUMENTS.payload)
                .where(DOCUMENTS.name == name),
            ).fetchone()
            if row is not None:
                return str(row[0])
            payload = initial()
            WorkspaceDatabase.put_document(connection, name, payload)
            return payload

    @staticmethod
    def put_document(connection: sqlite3.Connection, name: str, payload: str) -> None:
        sql.execute(
            connection,
            sql.Query.into(DOCUMENTS)
            .insert(name, payload)
            .on_conflict(DOCUMENTS.name)
            .do_update(DOCUMENTS.payload),
        )

    async def load_document(self, name: str, initial: Callable[[], str]) -> str:
        return await self.run(lambda conn: self.document(conn, name, initial))

    async def save_document(self, name: str, payload: str) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                self.put_document(connection, name, payload)

        await self.run(write)

    async def update_document(
        self, name: str, initial: Callable[[], str], update: Callable[[str], str]
    ) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = sql.execute(
                    connection,
                    sql.Query.from_(DOCUMENTS)
                    .select(DOCUMENTS.payload)
                    .where(DOCUMENTS.name == name),
                ).fetchone()
                payload = initial() if row is None else str(row[0])
                self.put_document(connection, name, update(payload))

        await self.run(write)

    async def receipt(self, key: str) -> typing.Optional[tuple[str, str]]:
        def read(connection: sqlite3.Connection) -> typing.Optional[tuple[str, str]]:
            row = sql.execute(
                connection,
                sql.Query.from_(REQUESTS)
                .select(REQUESTS.fingerprint, REQUESTS.result)
                .where(REQUESTS.key == key),
            ).fetchone()
            return None if row is None else (str(row[0]), str(row[1]))

        return await self.run(read)

    async def save_receipt(self, key: str, fingerprint: str, result: str) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                sql.execute(
                    connection,
                    sql.Query.into(REQUESTS).insert(key, fingerprint, result),
                )

        await self.run(write)

    async def index_status(self) -> tuple[int, typing.Optional[float]]:
        def read(connection: sqlite3.Connection) -> tuple[int, typing.Optional[float]]:
            row = sql.execute(
                connection,
                sql.Query.from_(STATE)
                .select(STATE.generation, STATE.refreshed_at)
                .where(STATE.id == 1),
            ).fetchone()
            return int(row[0]), None if row[1] is None else float(row[1])

        return await self.run(read)

    async def remove_execution_receipts(self, execution_id: str) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                sql.execute(
                    connection,
                    sql.Query.from_(REQUESTS)
                    .delete()
                    .where(
                        sql.json_extract(REQUESTS.result, "$.execution_id")
                        == execution_id
                    ),
                )

        await self.run(write)

    async def invalidate_index(self) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                sql.execute(
                    connection,
                    sql.Query.update(STATE)
                    .set(STATE.generation, STATE.generation + 1)
                    .set(STATE.refreshed_at, None)
                    .where(STATE.id == 1),
                )

        await self.run(write)

    async def replace_index(
        self, generation: int, refreshed_at: float, entries: Sequence[tuple[str, str]]
    ) -> bool:
        """Do not publish a discovery snapshot invalidated while it was being built."""

        def write(connection: sqlite3.Connection) -> bool:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                changed = sql.execute(
                    connection,
                    sql.Query.update(STATE)
                    .set(STATE.refreshed_at, refreshed_at)
                    .set(STATE.generation, STATE.generation + 1)
                    .where((STATE.id == 1) & (STATE.generation == generation)),
                ).rowcount
                if not changed:
                    return False
                sql.execute(connection, sql.Query.from_(INDEX).delete())
                sql.insert_many(connection, INDEX, entries)
                return True

        return await self.run(write)

    async def index_page(
        self,
        after: typing.Optional[str],
        limit: typing.Optional[int],
        *,
        active: Sequence[str] = (),
        active_only: bool = False,
    ) -> list[str]:
        """Prepend unindexed active IDs in reverse activation order, then seek archives.

        Separate tie and older-timestamp seeks bound work for mixed sort directions.
        """
        if limit is not None and limit < 0:
            limit = None

        def read(connection: sqlite3.Connection) -> list[str]:
            with connection:
                connection.execute("BEGIN")
                indexed_active = (
                    [
                        str(row[0])
                        for row in sql.execute(
                            connection,
                            sql.Query.from_(INDEX)
                            .select(INDEX.execution_id)
                            .where(INDEX.execution_id.isin(sql.json_values(active)))
                            .orderby(INDEX.updated_at, order=sql.Order.desc)
                            .orderby(INDEX.execution_id),
                        )
                    ]
                    if active
                    else []
                )
                persisted = set(indexed_active)
                selected = [
                    identifier
                    for identifier in reversed(active)
                    if identifier not in persisted
                ]
                if active_only:
                    selected.extend(indexed_active)
                    if after is not None:
                        if after not in selected:
                            raise ValueError("execution index cursor does not exist")
                        selected = selected[selected.index(after) + 1 :]
                    return selected[:limit]
                cursor = after
                if cursor is not None and cursor in selected:
                    selected = selected[selected.index(cursor) + 1 :]
                    cursor = None
                elif cursor is not None:
                    selected = []
                if cursor is None and limit is not None and len(selected) >= limit:
                    return selected[:limit]
                if cursor is not None:
                    row = sql.execute(
                        connection,
                        sql.Query.from_(INDEX)
                        .select(INDEX.updated_at)
                        .where(INDEX.execution_id == cursor),
                    ).fetchone()
                    if row is None:
                        raise ValueError("execution index cursor does not exist")
                    updated_at = str(row[0])
                    selected.extend(
                        str(row[0])
                        for row in sql.execute(
                            connection,
                            sql.Query.from_(INDEX)
                            .select(INDEX.execution_id)
                            .where(
                                (INDEX.updated_at == updated_at)
                                & (INDEX.execution_id > cursor)
                            )
                            .orderby(INDEX.execution_id)
                            .limit(-1 if limit is None else limit),
                        )
                    )
                    if limit is not None and len(selected) >= limit:
                        return selected
                    statement = (
                        sql.Query.from_(INDEX)
                        .select(INDEX.execution_id)
                        .where(INDEX.updated_at < updated_at)
                    )
                else:
                    statement = sql.Query.from_(INDEX).select(INDEX.execution_id)
                selected.extend(
                    str(row[0])
                    for row in sql.execute(
                        connection,
                        statement.orderby(INDEX.updated_at, order=sql.Order.desc)
                        .orderby(INDEX.execution_id)
                        .limit(-1 if limit is None else limit - len(selected)),
                    )
                )
                return selected

        return await self.run(read)
