"""Rebuildable workspace summaries and ordering, never workflow execution state."""

from collections.abc import Sequence
import json
import sqlite3
from time import time
from uuid import UUID

from vectorops.runner.runner import inspection, summary_history
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class SummaryStore:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database

    @staticmethod
    def _record(
        connection: sqlite3.Connection,
        snapshot: summary_history.Snapshot,
        *,
        repair: bool = False,
    ) -> None:
        identifier = snapshot.summary.execution_id
        payload = snapshot.summary.model_dump_json()
        if connection.execute(
            "SELECT 1 FROM summary_tombstones WHERE execution_id = ? AND source_id = ?",
            (identifier, snapshot.source_id),
        ).fetchone():
            return
        previous = connection.execute(
            "SELECT s.source_id, s.revision, s.payload, i.updated_at "
            "FROM execution_summaries s LEFT JOIN execution_index i USING(execution_id) "
            "WHERE s.execution_id = ?",
            (identifier,),
        ).fetchone()
        if previous is not None:
            if previous[0] == snapshot.source_id:
                if not repair and previous[1] >= snapshot.revision:
                    return
                if previous[1:] == (snapshot.revision, payload, snapshot.updated_at):
                    if repair:
                        SummaryStore._targets(connection, snapshot)
                    return
            if previous[0] != snapshot.source_id:
                connection.execute(
                    "INSERT OR IGNORE INTO summary_tombstones VALUES (?, ?)",
                    (identifier, previous[0]),
                )
        connection.execute(
            "INSERT INTO execution_summaries VALUES (?, ?, ?, ?) "
            "ON CONFLICT(execution_id) DO UPDATE SET source_id = excluded.source_id, "
            "revision = excluded.revision, payload = excluded.payload",
            (
                identifier,
                snapshot.source_id,
                snapshot.revision,
                payload,
            ),
        )
        connection.execute(
            "INSERT INTO execution_index VALUES (?, ?) ON CONFLICT(execution_id) "
            "DO UPDATE SET updated_at = excluded.updated_at",
            (identifier, snapshot.updated_at),
        )
        connection.execute(
            "UPDATE index_state SET generation = generation + 1 WHERE id = 1"
        )
        SummaryStore._targets(connection, snapshot)

    @staticmethod
    def _targets(
        connection: sqlite3.Connection, snapshot: summary_history.Snapshot
    ) -> None:
        identifier = snapshot.summary.execution_id
        connection.execute(
            "DELETE FROM execution_targets WHERE execution_id = ?", (identifier,)
        )
        connection.executemany(
            "INSERT OR IGNORE INTO execution_targets VALUES (?, ?, ?)",
            [
                (identifier, target.root, str(target.worktree_id or ""))
                for target in snapshot.targets
            ],
        )

    async def worktree_executions(self, identifier: UUID, path: str) -> set[str]:
        """Find reference candidates by UUID or an indexed raw directory-prefix range."""
        return await self.database.run(
            lambda connection: {
                str(row[0])
                for row in connection.execute(
                    "SELECT execution_id FROM execution_targets WHERE worktree_id = ? "
                    "UNION SELECT execution_id FROM execution_targets WHERE root = ? "
                    "UNION SELECT execution_id FROM execution_targets WHERE root >= ? AND root < ?",
                    (str(identifier), path, path + "/", path + "0"),
                )
            }
        )

    async def record(self, snapshot: summary_history.Snapshot) -> None:
        """Publish only newer source revisions; deleted incarnations cannot reappear."""

        def write(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                self._record(connection, snapshot)

        await self.database.run(write)

    async def get(self, execution_id: str) -> inspection.ExecutionSummary:
        return (await self.get_many((execution_id,)))[execution_id]

    async def get_many(
        self, execution_ids: Sequence[str]
    ) -> dict[str, inspection.ExecutionSummary]:
        if not execution_ids:
            return {}

        def read(
            connection: sqlite3.Connection,
        ) -> dict[str, inspection.ExecutionSummary]:
            return {
                str(row[0]): inspection.ExecutionSummary.model_validate_json(row[1])
                for row in connection.execute(
                    "SELECT execution_id, payload FROM execution_summaries "
                    "WHERE execution_id IN (SELECT value FROM json_each(?))",
                    (json.dumps(list(execution_ids)),),
                )
            }

        return await self.database.run(read)

    @staticmethod
    def _remove(connection: sqlite3.Connection, identifiers: Sequence[str]) -> None:
        selected = json.dumps(list(identifiers))
        connection.execute(
            "INSERT OR IGNORE INTO summary_tombstones "
            "SELECT execution_id, source_id FROM execution_summaries "
            "WHERE execution_id IN (SELECT value FROM json_each(?))",
            (selected,),
        )
        connection.execute(
            "DELETE FROM execution_summaries WHERE execution_id IN (SELECT value FROM json_each(?))",
            (selected,),
        )
        connection.execute(
            "DELETE FROM execution_index WHERE execution_id IN (SELECT value FROM json_each(?))",
            (selected,),
        )
        connection.execute(
            "DELETE FROM execution_targets WHERE execution_id IN (SELECT value FROM json_each(?))",
            (selected,),
        )
        connection.execute(
            "UPDATE index_state SET generation = generation + 1 WHERE id = 1"
        )

    async def remove(self, identifiers: Sequence[str]) -> None:
        def write(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                self._remove(connection, identifiers)

        await self.database.run(write)

    async def reconcile(
        self, generation: int, snapshots: Sequence[summary_history.Snapshot]
    ) -> bool:
        """Repair startup state without overwriting changes published during discovery."""

        def write(connection: sqlite3.Connection) -> bool:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                if (
                    connection.execute(
                        "SELECT generation FROM index_state WHERE id = 1"
                    ).fetchone()[0]
                    != generation
                ):
                    return False
                identifiers = [snapshot.summary.execution_id for snapshot in snapshots]
                removed = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT execution_id FROM (SELECT execution_id FROM execution_index "
                        "UNION SELECT execution_id FROM execution_summaries "
                        "UNION SELECT execution_id FROM execution_targets) "
                        "WHERE execution_id NOT IN (SELECT value FROM json_each(?))",
                        (json.dumps(identifiers),),
                    )
                ]
                self._remove(connection, removed)
                for snapshot in snapshots:
                    connection.execute(
                        "DELETE FROM summary_tombstones WHERE execution_id = ? AND source_id = ?",
                        (snapshot.summary.execution_id, snapshot.source_id),
                    )
                    self._record(connection, snapshot, repair=True)
                connection.execute(
                    "UPDATE index_state SET refreshed_at = ? WHERE id = 1", (time(),)
                )
                return True

        return await self.database.run(write)
