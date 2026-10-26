"""Revisioned sidebar and analytical snapshots owned by execution history."""

import sqlite3
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, Optional
from uuid import uuid4

import pydantic

from vectorops.runner import models, usage
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import execution_state, inspection, sql
from vectorops.runner.runner.usage_history import Change, UsageHistory

EXECUTION: Final = sql.Table("execution")
WORKFLOWS: Final = sql.Table("workflows")
INPUTS: Final = sql.Table("input_requests")
STATE: Final = sql.Table("summary_state")


class TargetReferences:
    """Own the append-only union of bindings across all retained branches."""

    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'target_references'"
        ).fetchone():
            return
        connection.execute(
            "CREATE TABLE target_references (root TEXT NOT NULL, worktree_id TEXT NOT NULL, "
            "PRIMARY KEY (root, worktree_id)) WITHOUT ROWID"
        )
        for table in ("workflows", "execution"):
            selection = (
                "SELECT json_extract(payload, '$.target.root'), "
                "coalesce(json_extract(payload, '$.target.worktree_id'), '') "
                f"FROM {table} WHERE json_type(payload, '$.target') = 'object'"
            )
            connection.execute("INSERT OR IGNORE INTO target_references " + selection)
        connection.execute(
            "INSERT OR IGNORE INTO target_references "
            "SELECT json_extract(value, '$.root'), coalesce(json_extract(value, '$.worktree_id'), '') "
            "FROM execution, json_each(execution.payload, '$.referenced_targets')"
        )

    @staticmethod
    def add(
        connection: sqlite3.Connection, targets: list[LocalExecutionTarget]
    ) -> set[LocalExecutionTarget]:
        added = set()
        for target in set(targets):
            if connection.execute(
                "INSERT OR IGNORE INTO target_references VALUES (?, ?)",
                (target.root, str(target.worktree_id or "")),
            ).rowcount:
                added.add(target)
        return added

    @staticmethod
    def read(connection: sqlite3.Connection) -> list[LocalExecutionTarget]:
        return [
            LocalExecutionTarget.model_validate(
                {"root": root, "worktree_id": identifier or None}
            )
            for root, identifier in connection.execute(
                "SELECT root, worktree_id FROM target_references ORDER BY root, worktree_id"
            )
        ]


class Snapshot(pydantic.BaseModel):
    source_id: str
    revision: int = pydantic.Field(ge=0)
    updated_at: str
    summary: inspection.ExecutionSummary
    targets: list[LocalExecutionTarget] = pydantic.Field(default_factory=list)


class SummaryHistory:
    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        """Migrate maintenance atomically, preserving source identity and retained bindings."""
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND (name GLOB 'summary_*' OR name GLOB 'references_*')"
        ).fetchall():
            connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        TargetReferences.initialize(connection)
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'summary_state'"
        ).fetchone():
            return
        connection.execute(
            "CREATE TABLE summary_state (id INTEGER PRIMARY KEY CHECK (id = 1), "
            "source_id TEXT NOT NULL, revision INTEGER NOT NULL)"
        )
        sql.execute(connection, sql.Query.into(STATE).insert(1, uuid4().hex, 0))
        connection.execute(
            "CREATE INDEX IF NOT EXISTS input_request_status "
            "ON input_requests (json_extract(payload, '$.status'))"
        )

    @staticmethod
    def _read(
        connection: sqlite3.Connection,
    ) -> tuple[execution_state.ExecutionRecord, inspection.ExecutionSummary]:
        row = sql.execute(
            connection,
            sql.Query.from_(EXECUTION)
            .select(EXECUTION.payload)
            .where(EXECUTION.singleton == 1),
        ).fetchone()
        if row is None:
            raise KeyError("execution state")
        execution = execution_state.ExecutionRecord.model_validate_json(row[0])
        row = sql.execute(
            connection,
            sql.Query.from_(WORKFLOWS)
            .select(WORKFLOWS.payload)
            .where(WORKFLOWS.id == execution.root_workflow_id),
        ).fetchone()
        if row is None:
            raise KeyError(execution.root_workflow_id)
        workflow = models.Workflow.model_validate_json(row[0])
        pending = sql.execute(
            connection,
            sql.Query.from_(INPUTS)
            .select(sql.functions.Count("*"))
            .where(sql.json_extract(INPUTS.payload, "$.status") == "pending"),
        ).fetchone()[0]
        return execution, inspection.ExecutionSummary(
            execution_id=execution.id,
            reference=workflow.definition_ref,
            root_workflow_id=workflow.id,
            name=workflow.name,
            description=workflow.description,
            status=execution.status,
            pending_inputs=pending,
            usage=usage.ExecutionUsage(
                totals=UsageHistory.snapshot(connection)[1],
                root_context=UsageHistory.root_context(connection, workflow.id),
            ),
        )

    @staticmethod
    def summary(connection: sqlite3.Connection) -> inspection.ExecutionSummary:
        return SummaryHistory._read(connection)[1]

    @staticmethod
    def targets(
        connection: sqlite3.Connection, execution: execution_state.ExecutionRecord
    ) -> list[LocalExecutionTarget]:
        """Collect execution-wide references without interpreting selected branch heads."""
        return TargetReferences.read(connection)

    @staticmethod
    def snapshot(connection: sqlite3.Connection) -> Snapshot:
        execution, summary = SummaryHistory._read(connection)
        source_id, revision = sql.execute(
            connection,
            sql.Query.from_(STATE)
            .select(STATE.source_id, STATE.revision)
            .where(STATE.id == 1),
        ).fetchone()
        return Snapshot(
            source_id=source_id,
            revision=revision,
            updated_at=execution.updated_at,
            summary=summary,
            targets=SummaryHistory.targets(connection, execution),
        )


class Urgency(StrEnum):
    NORMAL = "normal"
    IMMEDIATE = "immediate"


@dataclass
class Changes:
    """Transaction-local values produced by data owners, discarded on rollback."""

    changed: bool = False
    status: Optional[execution_state.ExecutionStatus] = None
    updated_at: Optional[str] = None
    metadata: dict[str, tuple[Optional[str], Optional[str]]] = field(
        default_factory=dict
    )
    pending_delta: int = 0
    accounting: Optional[Change] = None
    targets: set[LocalExecutionTarget] = field(default_factory=set)
    context_changed: bool = False
    context: Optional[usage.Context] = None
    workflows: set[str] = field(default_factory=set)
    notify: bool = False
    urgency: Urgency = Urgency.NORMAL
    snapshot: Optional[Snapshot] = None

    def account(self, change: Optional[Change]) -> None:
        if change is not None:
            self.accounting = change
            self.workflows.update(change.workflows)


class WorkingSummary:
    """Project explicit changes without owning accounting or transition policy."""

    def __init__(self) -> None:
        self.snapshot: Optional[Snapshot] = None

    def base(self, connection: sqlite3.Connection) -> Optional[Snapshot]:
        source, revision = connection.execute(
            "SELECT source_id, revision FROM summary_state WHERE id = 1"
        ).fetchone()
        if self.snapshot is not None and (
            self.snapshot.source_id,
            self.snapshot.revision,
        ) == (source, revision):
            return self.snapshot
        try:
            return SummaryHistory.snapshot(connection)
        except KeyError:
            return None

    @staticmethod
    def project(base: Snapshot, changes: Changes, revision: int) -> Snapshot:
        summary = base.summary.model_copy()
        if changes.status is not None:
            summary.status = changes.status
        if summary.root_workflow_id in changes.metadata:
            summary.name, summary.description = changes.metadata[
                summary.root_workflow_id
            ]
        summary.pending_inputs += changes.pending_delta
        if summary.usage is not None:
            summary.usage = usage.ExecutionUsage(
                totals=(
                    changes.accounting.totals
                    if changes.accounting is not None
                    else summary.usage.totals
                ),
                root_context=(
                    changes.context
                    if changes.context_changed
                    else summary.usage.root_context
                ),
            )
        return Snapshot(
            source_id=base.source_id,
            revision=revision,
            updated_at=changes.updated_at or base.updated_at,
            summary=summary,
            targets=sorted(
                set(base.targets) | changes.targets,
                key=lambda target: (target.root, str(target.worktree_id)),
            ),
        )
