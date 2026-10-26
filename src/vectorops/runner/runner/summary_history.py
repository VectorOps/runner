"""Revisioned sidebar and analytical snapshots owned by execution history."""

import sqlite3
from typing import Final, Optional
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
            for action in ("INSERT", "UPDATE"):
                condition = "json_type(NEW.payload, '$.target') = 'object'"
                if action == "UPDATE":
                    condition += " AND json_extract(OLD.payload, '$.target') IS NOT json_extract(NEW.payload, '$.target')"
                connection.execute(
                    f"CREATE TRIGGER references_{table}_{action.lower()} AFTER {action} ON {table} "
                    f"WHEN {condition} BEGIN INSERT OR IGNORE INTO target_references VALUES "
                    "(json_extract(NEW.payload, '$.target.root'), "
                    "coalesce(json_extract(NEW.payload, '$.target.worktree_id'), '')); END"
                )
        connection.execute(
            "INSERT OR IGNORE INTO target_references "
            "SELECT json_extract(value, '$.root'), coalesce(json_extract(value, '$.worktree_id'), '') "
            "FROM execution, json_each(execution.payload, '$.referenced_targets')"
        )
        for action in ("INSERT", "UPDATE"):
            condition = (
                " WHEN json_extract(OLD.payload, '$.referenced_targets') IS NOT "
                "json_extract(NEW.payload, '$.referenced_targets')"
                if action == "UPDATE"
                else ""
            )
            connection.execute(
                f"CREATE TRIGGER references_runtime_{action.lower()} AFTER {action} ON execution{condition} "
                "BEGIN INSERT OR IGNORE INTO target_references "
                "SELECT json_extract(value, '$.root'), coalesce(json_extract(value, '$.worktree_id'), '') "
                "FROM json_each(NEW.payload, '$.referenced_targets'); END"
            )

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
        """Version source changes transactionally, including branch and mailbox writes."""
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


class WorkingSummary:
    """Capture transaction-local values; expose only snapshots whose writes committed.

    Temporary triggers observe every mutation through the history adapter, including
    control operations. They never publish or change committed Python state. A source
    revision check detects writes made through a separate adapter before reuse.
    """

    def __init__(self) -> None:
        self.snapshot: Optional[Snapshot] = None
        self._candidate: Optional[Snapshot] = None
        self._targets: set[LocalExecutionTarget] = set()
        self._context_dirty = False
        self.usage_revision = 0
        self._candidate_usage_revision = 0

    def begin(self, connection: sqlite3.Connection) -> None:
        source, revision = connection.execute(
            "SELECT source_id, revision FROM summary_state WHERE id = 1"
        ).fetchone()
        if self.snapshot is None or (
            self.snapshot.source_id,
            self.snapshot.revision,
        ) != (source, revision):
            try:
                self.snapshot = SummaryHistory.snapshot(connection)
            except KeyError:
                self.snapshot = None
            self.usage_revision = UsageHistory.snapshot(connection)[0]
        self._candidate = (
            self.snapshot.model_copy(deep=True) if self.snapshot is not None else None
        )
        self._targets = (
            set(self.snapshot.targets) if self.snapshot is not None else set()
        )
        self._context_dirty = False
        self._candidate_usage_revision = self.usage_revision
        connection.create_function("summary_capture", 3, self._capture)
        triggers = {
            "execution": "SELECT summary_capture('execution', '', json_object('status', json_extract(NEW.payload, '$.status'), 'updated_at', json_extract(NEW.payload, '$.updated_at')));",
            "workflows": "SELECT summary_capture('workflow', NEW.id, json_object('name', json_extract(NEW.payload, '$.name'), 'description', json_extract(NEW.payload, '$.description')));",
            "usage_state": "SELECT summary_capture('usage', NEW.revision, NEW.payload);",
            "target_references": "SELECT summary_capture('target', NEW.root, NEW.worktree_id);",
            "workflow_heads": "SELECT summary_capture('head', NEW.workflow_id, '');",
        }
        for table, statement in triggers.items():
            for action in ("INSERT", "UPDATE"):
                connection.execute(
                    f"CREATE TEMP TRIGGER IF NOT EXISTS capture_{table}_{action} AFTER {action} ON main.{table} BEGIN {statement} END"
                )
        for action, expression in (
            ("INSERT", "(json_extract(NEW.payload, '$.status') = 'pending')"),
            (
                "UPDATE",
                "(json_extract(NEW.payload, '$.status') = 'pending') - (json_extract(OLD.payload, '$.status') = 'pending')",
            ),
            ("DELETE", "-(json_extract(OLD.payload, '$.status') = 'pending')"),
        ):
            connection.execute(
                f"CREATE TEMP TRIGGER IF NOT EXISTS capture_inputs_{action} AFTER {action} ON main.input_requests "
                f"BEGIN SELECT summary_capture('pending', {expression}, ''); END"
            )
        connection.execute(
            "CREATE TEMP TRIGGER IF NOT EXISTS capture_heads_delete AFTER DELETE ON main.workflow_heads "
            "BEGIN SELECT summary_capture('head', OLD.workflow_id, ''); END"
        )

    def _capture(self, kind: str, key: str, payload: str) -> None:
        import json

        candidate = self._candidate
        if candidate is None:
            return
        summary = candidate.summary
        if kind == "execution":
            value = json.loads(payload)
            summary.status = value["status"]
            candidate.updated_at = value["updated_at"]
        elif kind == "workflow" and key == summary.root_workflow_id:
            value = json.loads(payload)
            summary.name, summary.description = value["name"], value["description"]
        elif kind == "pending":
            summary.pending_inputs += int(key)
        elif kind == "usage":
            self._candidate_usage_revision = int(key)
            assert summary.usage is not None
            summary.usage = summary.usage.model_copy(
                update={"totals": usage.Totals.model_validate_json(payload)}
            )
        elif kind == "target":
            self._targets.add(
                LocalExecutionTarget.model_validate(
                    {"root": key, "worktree_id": payload or None}
                )
            )
        elif kind == "head" and key == summary.root_workflow_id:
            self._context_dirty = True

    def step(self, step: models.WorkflowStep) -> None:
        candidate = self._candidate
        if candidate is None or candidate.summary.usage is None:
            return
        context = candidate.summary.usage.root_context
        if context is not None and context.step_id == step.id:
            candidate.summary.usage = candidate.summary.usage.model_copy(
                update={"root_context": UsageHistory.context(step)}
            )

    def commit(self, connection: sqlite3.Connection) -> Optional[Snapshot]:
        candidate = self._candidate
        if candidate is None:
            try:
                candidate = SummaryHistory.snapshot(connection)
            except KeyError:
                return None
        else:
            candidate.revision = connection.execute(
                "SELECT revision FROM summary_state WHERE id = 1"
            ).fetchone()[0]
            if self._context_dirty:
                assert candidate.summary.usage is not None
                candidate.summary.usage = candidate.summary.usage.model_copy(
                    update={
                        "root_context": UsageHistory.root_context(
                            connection, candidate.summary.root_workflow_id or ""
                        )
                    }
                )
            candidate.targets = sorted(
                self._targets, key=lambda target: (target.root, str(target.worktree_id))
            )
        self.snapshot = candidate
        self.usage_revision = self._candidate_usage_revision
        return candidate
