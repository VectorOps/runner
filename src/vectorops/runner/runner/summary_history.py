"""Revisioned sidebar and analytical snapshots owned by execution history."""

import sqlite3
from typing import Final
from uuid import uuid4

import pydantic

from vectorops.runner import models, usage
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import execution_state, inspection, sql
from vectorops.runner.runner.usage_history import UsageHistory

EXECUTION: Final = sql.Table("execution")
WORKFLOWS: Final = sql.Table("workflows")
INPUTS: Final = sql.Table("input_requests")
STATE: Final = sql.Table("summary_state")


class Snapshot(pydantic.BaseModel):
    source_id: str
    revision: int = pydantic.Field(ge=0)
    updated_at: str
    summary: inspection.ExecutionSummary
    targets: list[LocalExecutionTarget] = pydantic.Field(default_factory=list)


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
        sql.execute(connection, sql.Query.into(STATE).insert(1, uuid4().hex, 0))
        connection.execute(
            "CREATE INDEX IF NOT EXISTS input_request_status "
            "ON input_requests (json_extract(payload, '$.status'))"
        )
        for statement in (
            "CREATE TRIGGER summary_execution_insert AFTER INSERT ON execution BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_execution_update AFTER UPDATE ON execution BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_execution_delete AFTER DELETE ON execution BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflows_insert AFTER INSERT ON workflows BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflows_update AFTER UPDATE ON workflows BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflows_delete AFTER DELETE ON workflows BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_steps_insert AFTER INSERT ON steps BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_steps_update AFTER UPDATE ON steps BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_steps_delete AFTER DELETE ON steps BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflow_heads_insert AFTER INSERT ON workflow_heads BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflow_heads_update AFTER UPDATE ON workflow_heads BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_workflow_heads_delete AFTER DELETE ON workflow_heads BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_input_requests_insert AFTER INSERT ON input_requests BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_input_requests_update AFTER UPDATE ON input_requests BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_input_requests_delete AFTER DELETE ON input_requests BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_usage_state_insert AFTER INSERT ON usage_state BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_usage_state_update AFTER UPDATE ON usage_state BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
            "CREATE TRIGGER summary_usage_state_delete AFTER DELETE ON usage_state BEGIN UPDATE summary_state SET revision = revision + 1 WHERE id = 1; END",
        ):
            connection.execute(statement)

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
        targets = set(execution.referenced_targets)
        if execution.target is not None:
            targets.add(execution.target)
        for (payload,) in sql.execute(
            connection,
            sql.Query.from_(WORKFLOWS)
            .select(sql.json_extract(WORKFLOWS.payload, "$.target"))
            .where(sql.json_type(WORKFLOWS.payload, "$.target") == "object"),
        ):
            targets.add(LocalExecutionTarget.model_validate_json(payload))
        return sorted(
            targets, key=lambda target: (target.root, str(target.worktree_id))
        )

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
