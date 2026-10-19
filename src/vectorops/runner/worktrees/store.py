"""Transactional workspace persistence for worktree records and usage leases."""

from collections.abc import Callable
from dataclasses import dataclass
import sqlite3
from typing import Final, Optional
from uuid import UUID

from vectorops.runner.runner import sql
from vectorops.runner.runner.workspace_database import WorkspaceDatabase

from . import models as m

WORKTREES: Final = sql.Table("worktrees")
USES: Final = sql.Table("worktree_uses")


@dataclass(frozen=True)
class Usage:
    id: UUID
    execution_id: Optional[str]


class Store:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database

    @staticmethod
    def _find(connection: sqlite3.Connection, identifier: UUID) -> Optional[m.Record]:
        row = sql.execute(
            connection,
            sql.Query.from_(WORKTREES)
            .select(WORKTREES.payload)
            .where(WORKTREES.id == str(identifier)),
        ).fetchone()
        return None if row is None else m.Record.model_validate_json(row[0])

    @staticmethod
    def _get(connection: sqlite3.Connection, identifier: UUID) -> m.Record:
        record = Store._find(connection, identifier)
        if record is None:
            raise m.WorktreeError(f"unknown worktree: {identifier}")
        return record

    async def find(self, identifier: UUID) -> Optional[m.Record]:
        return await self.database.run(
            lambda connection: self._find(connection, identifier)
        )

    async def get(self, identifier: UUID) -> m.Record:
        return await self.database.run(
            lambda connection: self._get(connection, identifier)
        )

    async def lookup(self, key: str, fingerprint: str) -> Optional[m.Record]:
        def read(connection: sqlite3.Connection) -> Optional[m.Record]:
            row = sql.execute(
                connection,
                sql.Query.from_(WORKTREES)
                .select(WORKTREES.fingerprint, WORKTREES.payload)
                .where(WORKTREES.operation_key == key),
            ).fetchone()
            if row is None:
                return None
            if row[0] != fingerprint:
                raise m.WorktreeError(
                    "idempotency key was used with different arguments"
                )
            return m.Record.model_validate_json(row[1])

        return await self.database.run(read)

    async def reserve(
        self, record: m.Record, key: str, fingerprint: str, limit: Optional[int]
    ) -> tuple[m.Record, bool]:
        def write(connection: sqlite3.Connection) -> tuple[m.Record, bool]:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = sql.execute(
                    connection,
                    sql.Query.from_(WORKTREES)
                    .select(WORKTREES.fingerprint, WORKTREES.payload)
                    .where(WORKTREES.operation_key == key),
                ).fetchone()
                if row is not None:
                    if row[0] != fingerprint:
                        raise m.WorktreeError(
                            "idempotency key was used with different arguments"
                        )
                    return m.Record.model_validate_json(row[1]), False
                count = sql.execute(
                    connection,
                    sql.Query.from_(WORKTREES)
                    .select(sql.functions.Count("*"))
                    .where(
                        sql.json_extract(WORKTREES.payload, "$.status") != "deleted"
                    ),
                ).fetchone()[0]
                if limit is not None and count >= limit:
                    raise m.WorktreeError(
                        "workspace worktree limit reached; explicitly delete a worktree or increase the limit"
                    )
                sql.execute(
                    connection,
                    sql.Query.into(WORKTREES).insert(
                        str(record.id), key, fingerprint, record.model_dump_json()
                    ),
                )
                return record, True

        return await self.database.run(write)

    async def change(
        self, identifier: UUID, update: Callable[[m.Record], m.Record]
    ) -> m.Record:
        def write(connection: sqlite3.Connection) -> m.Record:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                record = update(self._get(connection, identifier))
                sql.execute(
                    connection,
                    sql.Query.update(WORKTREES)
                    .set(WORKTREES.payload, record.model_dump_json())
                    .where(WORKTREES.id == str(identifier)),
                )
                return record

        return await self.database.run(write)

    async def page(self, query: m.Query) -> m.Page:
        def read(connection: sqlite3.Connection) -> m.Page:
            statement = (
                sql.Query.from_(WORKTREES)
                .select(WORKTREES.payload)
                .where(WORKTREES.id > (str(query.after) if query.after else ""))
            )
            if not query.include_deleted:
                statement = statement.where(
                    sql.json_extract(WORKTREES.payload, "$.status") != "deleted"
                )
            rows = sql.execute(
                connection, statement.orderby(WORKTREES.id).limit(query.limit + 1)
            ).fetchall()
            records = [m.Record.model_validate_json(row[0]) for row in rows]
            return m.Page(
                worktrees=records[: query.limit],
                next_after=(
                    records[query.limit - 1].id if len(records) > query.limit else None
                ),
            )

        return await self.database.run(read)

    async def acquire(
        self, identifier: UUID, token: UUID, execution_id: Optional[str]
    ) -> m.Record:
        def write(connection: sqlite3.Connection) -> m.Record:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                record = self._get(connection, identifier)
                if record.status != "ready":
                    raise m.WorktreeError(f"worktree is not ready: {record.status}")
                sql.execute(
                    connection,
                    sql.Query.into(USES).insert(
                        str(token), str(identifier), execution_id
                    ),
                )
                record = record.model_copy(update={"last_used_at": m.timestamp()})
                sql.execute(
                    connection,
                    sql.Query.update(WORKTREES)
                    .set(WORKTREES.payload, record.model_dump_json())
                    .where(WORKTREES.id == str(identifier)),
                )
                return record

        return await self.database.run(write)

    async def release(self, token: UUID) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                sql.execute(
                    connection,
                    sql.Query.from_(USES).delete().where(USES.id == str(token)),
                )

        await self.database.run(write)

    async def uses(self, identifier: UUID) -> list[UUID]:
        return [usage.id for usage in await self.usages(identifier)]

    async def usages(self, identifier: UUID) -> list[Usage]:
        return await self.database.run(
            lambda connection: [
                Usage(UUID(row[0]), None if row[1] is None else str(row[1]))
                for row in sql.execute(
                    connection,
                    sql.Query.from_(USES)
                    .select(USES.id, USES.execution_id)
                    .where(USES.worktree_id == str(identifier)),
                )
            ]
        )
