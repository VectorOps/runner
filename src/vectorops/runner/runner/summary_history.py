"""Revisioned sidebar and analytical snapshots owned by execution history."""

import sqlite3
from uuid import uuid4

import pydantic

from vectorops.runner import models, usage
from vectorops.runner.runner import execution_state, inspection
from vectorops.runner.runner.usage_history import UsageHistory


class Snapshot(pydantic.BaseModel):
    source_id: str
    revision: int = pydantic.Field(ge=0)
    updated_at: str
    summary: inspection.ExecutionSummary


class SummaryHistory:
    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        """Version source changes transactionally, including branch and mailbox writes."""
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'summary_state'"
        ).fetchone():
            return
        connection.execute(
            "CREATE TABLE summary_state (id INTEGER PRIMARY KEY CHECK (id = 1), "
            "source_id TEXT NOT NULL, revision INTEGER NOT NULL)"
        )
        connection.execute("INSERT INTO summary_state VALUES (1, ?, 0)", (uuid4().hex,))
        connection.execute(
            "CREATE INDEX IF NOT EXISTS input_request_status "
            "ON input_requests (json_extract(payload, '$.status'))"
        )
        for table in (
            "execution",
            "workflows",
            "steps",
            "workflow_heads",
            "input_requests",
            "usage_state",
        ):
            for operation in ("INSERT", "UPDATE", "DELETE"):
                connection.execute(
                    f"CREATE TRIGGER summary_{table}_{operation.lower()} AFTER {operation} ON {table} "
                    "BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END"
                )

    @staticmethod
    def _read(
        connection: sqlite3.Connection,
    ) -> tuple[execution_state.ExecutionRecord, inspection.ExecutionSummary]:
        row = connection.execute(
            "SELECT payload FROM execution WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise KeyError("execution state")
        execution = execution_state.ExecutionRecord.model_validate_json(row[0])
        row = connection.execute(
            "SELECT payload FROM workflows WHERE id = ?", (execution.root_workflow_id,)
        ).fetchone()
        if row is None:
            raise KeyError(execution.root_workflow_id)
        workflow = models.Workflow.model_validate_json(row[0])
        pending = connection.execute(
            "SELECT COUNT(*) FROM input_requests WHERE json_extract(payload, '$.status') = 'pending'"
        ).fetchone()[0]
        return execution, inspection.ExecutionSummary(
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

    @staticmethod
    def summary(connection: sqlite3.Connection) -> inspection.ExecutionSummary:
        return SummaryHistory._read(connection)[1]

    @staticmethod
    def snapshot(connection: sqlite3.Connection) -> Snapshot:
        execution, summary = SummaryHistory._read(connection)
        source_id, revision = connection.execute(
            "SELECT source_id, revision FROM summary_state WHERE id = 1"
        ).fetchone()
        return Snapshot(
            source_id=source_id,
            revision=revision,
            updated_at=execution.updated_at,
            summary=summary,
        )
