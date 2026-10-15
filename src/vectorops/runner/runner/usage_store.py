"""Durable workspace counters independent of execution history retention."""

import asyncio
import sqlite3
from typing import Optional

import pydantic

from vectorops.runner import usage
from vectorops.runner.runner.execution_state import timestamp
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class Entry(pydantic.BaseModel):
    revision: int
    total: usage.Totals
    baseline: usage.Totals = pydantic.Field(default_factory=usage.Totals)


class UsageStore:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database
        self.lock = asyncio.Lock()

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> usage.RunnerUsage:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS usage_executions (execution_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        )
        row = connection.execute(
            "SELECT payload FROM documents WHERE name = 'usage'"
        ).fetchone()
        return (
            usage.RunnerUsage()
            if row is None
            else usage.RunnerUsage.model_validate_json(row[0])
        )

    async def record(
        self, execution_id: str, revision: int, total: usage.Totals
    ) -> None:
        """Reconcile a durable execution snapshot, rejecting delayed older snapshots."""

        def write(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                counters = self._initialize(connection)
                row = connection.execute(
                    "SELECT payload FROM usage_executions WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()
                previous: Optional[Entry] = (
                    None if row is None else Entry.model_validate_json(row[0])
                )
                if previous is not None and previous.revision >= revision:
                    return
                entry = Entry(
                    revision=revision,
                    total=total,
                    baseline=usage.Totals() if previous is None else previous.baseline,
                )
                old = (
                    usage.Totals()
                    if previous is None
                    else previous.total.since(previous.baseline)
                )
                counters = counters.model_copy(
                    update={
                        "totals": counters.totals.replace(
                            old, entry.total.since(entry.baseline)
                        )
                    }
                )
                connection.execute(
                    "INSERT INTO usage_executions VALUES (?, ?) ON CONFLICT(execution_id) DO UPDATE SET payload = excluded.payload",
                    (execution_id, entry.model_dump_json()),
                )
                WorkspaceDatabase.put_document(
                    connection, "usage", counters.model_dump_json()
                )

        await self.database.run(write)

    async def read(self, *, reset: bool = False) -> usage.RunnerUsage:
        def write(connection: sqlite3.Connection) -> usage.RunnerUsage:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                counters = self._initialize(connection)
                if reset:
                    rows = connection.execute(
                        "SELECT execution_id, payload FROM usage_executions"
                    ).fetchall()
                    for identifier, payload in rows:
                        entry = Entry.model_validate_json(payload)
                        entry = entry.model_copy(update={"baseline": entry.total})
                        connection.execute(
                            "UPDATE usage_executions SET payload = ? WHERE execution_id = ?",
                            (entry.model_dump_json(), identifier),
                        )
                    counters = usage.RunnerUsage(
                        reset_at=timestamp(), generation=counters.generation + 1
                    )
                WorkspaceDatabase.put_document(
                    connection, "usage", counters.model_dump_json()
                )
                return counters

        return await self.database.run(write)
