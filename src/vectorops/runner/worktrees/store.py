"""Transactional workspace persistence for worktree records and usage leases."""

from collections.abc import Callable
from dataclasses import dataclass
import sqlite3
from typing import Optional
from uuid import UUID

from vectorops.runner.runner.workspace_database import WorkspaceDatabase

from . import models as m


@dataclass(frozen=True)
class Usage:
    id: UUID
    execution_id: Optional[str]


class Store:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database

    @staticmethod
    def _find(connection: sqlite3.Connection, identifier: UUID) -> Optional[m.Record]:
        row = connection.execute(
            "SELECT payload FROM worktrees WHERE id = ?", (str(identifier),)
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
            row = connection.execute(
                "SELECT fingerprint, payload FROM worktrees WHERE operation_key = ?",
                (key,),
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
                row = connection.execute(
                    "SELECT fingerprint, payload FROM worktrees WHERE operation_key = ?",
                    (key,),
                ).fetchone()
                if row is not None:
                    if row[0] != fingerprint:
                        raise m.WorktreeError(
                            "idempotency key was used with different arguments"
                        )
                    return m.Record.model_validate_json(row[1]), False
                count = connection.execute(
                    "SELECT count(*) FROM worktrees WHERE json_extract(payload, '$.status') != 'deleted'"
                ).fetchone()[0]
                if limit is not None and count >= limit:
                    raise m.WorktreeError(
                        "workspace worktree limit reached; explicitly delete a worktree or increase the limit"
                    )
                connection.execute(
                    "INSERT INTO worktrees VALUES (?, ?, ?, ?)",
                    (str(record.id), key, fingerprint, record.model_dump_json()),
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
                connection.execute(
                    "UPDATE worktrees SET payload = ? WHERE id = ?",
                    (record.model_dump_json(), str(identifier)),
                )
                return record

        return await self.database.run(write)

    async def page(self, query: m.Query) -> m.Page:
        def read(connection: sqlite3.Connection) -> m.Page:
            rows = connection.execute(
                "SELECT payload FROM worktrees WHERE id > ? AND (? OR json_extract(payload, '$.status') != 'deleted') ORDER BY id LIMIT ?",
                (
                    str(query.after) if query.after else "",
                    query.include_deleted,
                    query.limit + 1,
                ),
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
                connection.execute(
                    "INSERT INTO worktree_uses VALUES (?, ?, ?)",
                    (str(token), str(identifier), execution_id),
                )
                record = record.model_copy(update={"last_used_at": m.timestamp()})
                connection.execute(
                    "UPDATE worktrees SET payload = ? WHERE id = ?",
                    (record.model_dump_json(), str(identifier)),
                )
                return record

        return await self.database.run(write)

    async def release(self, token: UUID) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute(
                    "DELETE FROM worktree_uses WHERE id = ?", (str(token),)
                )

        await self.database.run(write)

    async def uses(self, identifier: UUID) -> list[UUID]:
        return [usage.id for usage in await self.usages(identifier)]

    async def usages(self, identifier: UUID) -> list[Usage]:
        return await self.database.run(
            lambda connection: [
                Usage(UUID(row[0]), None if row[1] is None else str(row[1]))
                for row in connection.execute(
                    "SELECT id, execution_id FROM worktree_uses WHERE worktree_id = ?",
                    (str(identifier),),
                )
            ]
        )
