"""Rebuildable workspace summaries and ordering, never workflow execution state."""

from collections.abc import Sequence
import sqlite3
from time import time
from typing import Final
from uuid import UUID

from vectorops.runner.runner import inspection, summary_history, sql
from vectorops.runner.runner.workspace_database import WorkspaceDatabase

SUMMARIES: Final = sql.Table("execution_summaries")
TOMBSTONES: Final = sql.Table("summary_tombstones")
INDEX: Final = sql.Table("execution_index")
STATE: Final = sql.Table("index_state")
TARGETS: Final = sql.Table("execution_targets")


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
        if sql.execute(
            connection,
            sql.Query.from_(TOMBSTONES)
            .select(1)
            .where(
                (TOMBSTONES.execution_id == identifier)
                & (TOMBSTONES.source_id == snapshot.source_id)
            ),
        ).fetchone():
            return
        previous = sql.execute(
            connection,
            sql.Query.from_(SUMMARIES)
            .left_join(INDEX)
            .on(SUMMARIES.execution_id == INDEX.execution_id)
            .select(
                SUMMARIES.source_id,
                SUMMARIES.revision,
                SUMMARIES.payload,
                INDEX.updated_at,
            )
            .where(SUMMARIES.execution_id == identifier),
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
                sql.execute(
                    connection,
                    sql.Query.into(TOMBSTONES)
                    .insert(identifier, previous[0])
                    .on_conflict()
                    .do_nothing(),
                )
        sql.execute(
            connection,
            sql.Query.into(SUMMARIES)
            .insert(identifier, snapshot.source_id, snapshot.revision, payload)
            .on_conflict(SUMMARIES.execution_id)
            .do_update(SUMMARIES.source_id)
            .do_update(SUMMARIES.revision)
            .do_update(SUMMARIES.payload),
        )
        sql.execute(
            connection,
            sql.Query.into(INDEX)
            .insert(identifier, snapshot.updated_at)
            .on_conflict(INDEX.execution_id)
            .do_update(INDEX.updated_at),
        )
        sql.execute(
            connection,
            sql.Query.update(STATE)
            .set(STATE.generation, STATE.generation + 1)
            .where(STATE.id == 1),
        )
        if (
            repair
            or previous is None
            or previous[0] != snapshot.source_id
            or previous[1] != snapshot.targets_base_revision
        ):
            SummaryStore._targets(connection, snapshot)

    @staticmethod
    def _targets(
        connection: sqlite3.Connection, snapshot: summary_history.Snapshot
    ) -> None:
        identifier = snapshot.summary.execution_id
        previous = set(
            connection.execute(
                "SELECT root, worktree_id FROM execution_targets WHERE execution_id = ?",
                (identifier,),
            )
        )
        current = {
            (target.root, str(target.worktree_id or "")) for target in snapshot.targets
        }
        connection.executemany(
            "DELETE FROM execution_targets WHERE execution_id = ? AND root = ? AND worktree_id = ?",
            [
                (identifier, root, worktree_id)
                for root, worktree_id in previous - current
            ],
        )
        sql.insert_many(
            connection,
            TARGETS,
            [
                (identifier, root, worktree_id)
                for root, worktree_id in current - previous
            ],
            ignore=True,
        )

    async def worktree_executions(self, identifier: UUID, path: str) -> set[str]:
        """Find reference candidates by UUID or an indexed raw directory-prefix range."""
        return await self.database.run(
            lambda connection: {
                str(row[0])
                for row in sql.execute(
                    connection,
                    sql.Query.from_(TARGETS)
                    .select(TARGETS.execution_id)
                    .where(TARGETS.worktree_id == str(identifier))
                    .union(
                        sql.Query.from_(TARGETS)
                        .select(TARGETS.execution_id)
                        .where(TARGETS.root == path)
                    )
                    .union(
                        sql.Query.from_(TARGETS)
                        .select(TARGETS.execution_id)
                        .where(
                            (TARGETS.root >= path + "/") & (TARGETS.root < path + "0")
                        )
                    ),
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
                for row in sql.execute(
                    connection,
                    sql.Query.from_(SUMMARIES)
                    .select(SUMMARIES.execution_id, SUMMARIES.payload)
                    .where(SUMMARIES.execution_id.isin(sql.json_values(execution_ids))),
                )
            }

        return await self.database.run(read)

    @staticmethod
    def _remove(connection: sqlite3.Connection, identifiers: Sequence[str]) -> None:
        selected = sql.json_values(identifiers)
        sql.execute(
            connection,
            sql.Query.into(TOMBSTONES)
            .from_(SUMMARIES)
            .select(SUMMARIES.execution_id, SUMMARIES.source_id)
            .where(SUMMARIES.execution_id.isin(selected))
            .on_conflict()
            .do_nothing(),
        )
        sql.execute(
            connection,
            sql.Query.from_(SUMMARIES)
            .delete()
            .where(SUMMARIES.execution_id.isin(selected)),
        )
        sql.execute(
            connection,
            sql.Query.from_(INDEX).delete().where(INDEX.execution_id.isin(selected)),
        )
        sql.execute(
            connection,
            sql.Query.from_(TARGETS)
            .delete()
            .where(TARGETS.execution_id.isin(selected)),
        )
        sql.execute(
            connection,
            sql.Query.update(STATE)
            .set(STATE.generation, STATE.generation + 1)
            .where(STATE.id == 1),
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
                    sql.execute(
                        connection,
                        sql.Query.from_(STATE)
                        .select(STATE.generation)
                        .where(STATE.id == 1),
                    ).fetchone()[0]
                    != generation
                ):
                    return False
                identifiers = [snapshot.summary.execution_id for snapshot in snapshots]
                candidates = (
                    sql.Query.from_(INDEX)
                    .select(INDEX.execution_id)
                    .union(sql.Query.from_(SUMMARIES).select(SUMMARIES.execution_id))
                    .union(sql.Query.from_(TARGETS).select(TARGETS.execution_id))
                    .as_("candidates")
                )
                removed = [
                    str(row[0])
                    for row in sql.execute(
                        connection,
                        sql.Query.from_(candidates)
                        .select(candidates.execution_id)
                        .where(
                            candidates.execution_id.notin(sql.json_values(identifiers))
                        ),
                    )
                ]
                self._remove(connection, removed)
                for snapshot in snapshots:
                    sql.execute(
                        connection,
                        sql.Query.from_(TOMBSTONES)
                        .delete()
                        .where(
                            (TOMBSTONES.execution_id == snapshot.summary.execution_id)
                            & (TOMBSTONES.source_id == snapshot.source_id)
                        ),
                    )
                    self._record(connection, snapshot, repair=True)
                sql.execute(
                    connection,
                    sql.Query.update(STATE)
                    .set(STATE.refreshed_at, time())
                    .where(STATE.id == 1),
                )
                return True

        return await self.database.run(write)
