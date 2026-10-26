"""Transactional usage accounting owned by execution history."""

import sqlite3
from decimal import Decimal
from typing import Final, Optional

from vectorops.runner import models, usage
from vectorops.runner.runner import sql

WORKFLOWS: Final = sql.Table("workflows")
STEPS: Final = sql.Table("steps")
PARENTS: Final = sql.Table("workflow_parents")
STATE: Final = sql.Table("usage_state")
ANCESTORS: Final = sql.Table("generation_ancestors")
HEADS: Final = sql.Table("workflow_heads")


def contribution(step: models.WorkflowStep) -> usage.Totals:
    """Project one replacement snapshot without reinterpreting model pricing."""
    from vectorops.runner.api.llm import observability

    value = (step.metrics or {}).get("llm")
    if value is None:
        if step.operation.kind not in ("llm.generate", "llm.compact"):
            return usage.Totals()
        return usage.Totals(calls=1, incomplete_calls=1)
    metrics = observability.Metrics.model_validate(value)
    provider = metrics.provider_usage
    incoming = metrics.input_tokens_estimate
    outgoing = metrics.output_tokens_estimate
    if provider is not None:
        incoming, outgoing = provider.input_tokens, provider.output_tokens
    return usage.Totals(
        input_tokens=incoming or 0,
        output_tokens=outgoing or 0,
        reasoning_tokens=0 if provider is None else provider.reasoning_tokens or 0,
        cache_read_tokens=0 if provider is None else provider.cache_read_tokens or 0,
        cache_write_tokens=0 if provider is None else provider.cache_write_tokens or 0,
        cost_estimate_usd=metrics.cost_estimate_usd or Decimal(0),
        calls=1,
        estimated_calls=int(provider is None or metrics.cost_basis == "token_estimate"),
        incomplete_calls=int(
            incoming is None
            or outgoing is None
            or metrics.cost_estimate_usd is None
            or metrics.retries > 0
            or (provider is not None and provider.completeness != "final")
        ),
    )


class UsageHistory:
    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        """Backfill existing histories once, in the same transaction as the new state."""
        UsageHistory._initialize_context(connection)
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'usage_state'"
        ).fetchone():
            return
        connection.execute(
            "CREATE TABLE workflow_parents (child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE usage_state (id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL, payload TEXT NOT NULL)"
        )
        sql.execute(
            connection,
            sql.Query.into(STATE).insert(1, 0, usage.Totals().model_dump_json()),
        )
        sql.execute(
            connection,
            sql.Query.update(WORKFLOWS).set(
                WORKFLOWS.payload, sql.json_remove(WORKFLOWS.payload, "$.usage")
            ),
        )
        steps = [
            models.WorkflowStep.model_validate_json(row[0])
            for row in sql.execute(
                connection,
                sql.Query.from_(STEPS).select(STEPS.payload).orderby(STEPS.rowid),
            )
        ]
        for step in steps:
            UsageHistory.link(connection, step)
        for step in steps:
            UsageHistory.update(connection, step)

    @staticmethod
    def _initialize_context(connection: sqlite3.Connection) -> None:
        """Materialize immutable predecessor links once, preserving every branch."""
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'generation_ancestors'"
        ).fetchone():
            return
        connection.execute(
            "CREATE TABLE generation_ancestors (step_id TEXT PRIMARY KEY, generation_id TEXT)"
        )
        for row in sql.execute(
            connection,
            sql.Query.from_(STEPS).select(STEPS.payload).orderby(STEPS.rowid),
        ):
            UsageHistory._link_context(
                connection, models.WorkflowStep.model_validate_json(row[0])
            )

    @staticmethod
    def _link_context(
        connection: sqlite3.Connection, step: models.WorkflowStep
    ) -> None:
        generation_id: Optional[str] = None
        if step.operation.kind == "llm.generate":
            generation_id = step.id
        elif step.previous_step_id is not None:
            row = sql.execute(
                connection,
                sql.Query.from_(ANCESTORS)
                .select(ANCESTORS.generation_id)
                .where(ANCESTORS.step_id == step.previous_step_id),
            ).fetchone()
            if row is None:
                raise KeyError(step.previous_step_id)
            generation_id = row[0]
        sql.execute(
            connection,
            sql.Query.into(ANCESTORS)
            .insert(step.id, generation_id)
            .on_conflict()
            .do_nothing(),
        )

    @staticmethod
    def workflow_usage(
        connection: sqlite3.Connection, workflow_id: str
    ) -> usage.WorkflowUsage:
        row = sql.execute(
            connection,
            sql.Query.from_(WORKFLOWS)
            .select(sql.json_extract(WORKFLOWS.payload, "$.usage"))
            .where(WORKFLOWS.id == workflow_id),
        ).fetchone()
        if row is None:
            raise KeyError(workflow_id)
        return (
            usage.WorkflowUsage()
            if row[0] is None
            else usage.WorkflowUsage.model_validate_json(row[0])
        )

    @staticmethod
    def save_usage(
        connection: sqlite3.Connection, workflow_id: str, totals: usage.WorkflowUsage
    ) -> None:
        sql.execute(
            connection,
            sql.Query.update(WORKFLOWS)
            .set(
                WORKFLOWS.payload,
                sql.json_set(
                    WORKFLOWS.payload,
                    "$.usage",
                    sql.json_value(totals.model_dump_json()),
                ),
            )
            .where(WORKFLOWS.id == workflow_id),
        )

    @staticmethod
    def link(connection: sqlite3.Connection, step: models.WorkflowStep) -> None:
        UsageHistory._link_context(connection, step)
        if not isinstance(step.operation, models.WorkflowSpawnOperation):
            return
        child = step.operation.child_workflow_id
        parent = step.workflow_id
        row = sql.execute(
            connection,
            sql.Query.from_(PARENTS)
            .select(PARENTS.parent_id)
            .where(PARENTS.child_id == child),
        ).fetchone()
        if row is not None:
            if row[0] != parent:
                raise ValueError("workflow has multiple parents")
            return
        ancestors = UsageHistory.ancestors(connection, parent)
        if child in ancestors:
            raise ValueError("cycle in workflow tree")
        sql.execute(
            connection,
            sql.Query.into(PARENTS).insert(child, parent),
        )
        child_usage = UsageHistory.workflow_usage(connection, child)
        for identifier in ancestors:
            totals = UsageHistory.workflow_usage(connection, identifier)
            UsageHistory.save_usage(
                connection,
                identifier,
                totals.model_copy(
                    update={
                        "subtree": totals.subtree.replace(
                            usage.Cost(), child_usage.subtree
                        )
                    }
                ),
            )

    @staticmethod
    def ancestors(connection: sqlite3.Connection, workflow_id: str) -> list[str]:
        result = []
        current: Optional[str] = workflow_id
        while current is not None:
            if current in result:
                raise ValueError("cycle in workflow tree")
            result.append(current)
            row = sql.execute(
                connection,
                sql.Query.from_(PARENTS)
                .select(PARENTS.parent_id)
                .where(PARENTS.child_id == current),
            ).fetchone()
            current = None if row is None else str(row[0])
        return result

    @staticmethod
    def snapshot(connection: sqlite3.Connection) -> tuple[int, usage.Totals]:
        row = sql.execute(
            connection,
            sql.Query.from_(STATE)
            .select(STATE.revision, STATE.payload)
            .where(STATE.id == 1),
        ).fetchone()
        return int(row[0]), usage.Totals.model_validate_json(row[1])

    @staticmethod
    def update(
        connection: sqlite3.Connection,
        step: models.WorkflowStep,
        previous: Optional[models.WorkflowStep] = None,
    ) -> None:
        old = usage.Totals() if previous is None else contribution(previous)
        new = contribution(step)
        if old == new:
            return
        revision, total = UsageHistory.snapshot(connection)
        sql.execute(
            connection,
            sql.Query.update(STATE)
            .set(STATE.revision, revision + 1)
            .set(STATE.payload, total.replace(old, new).model_dump_json())
            .where(STATE.id == 1),
        )
        for identifier in UsageHistory.ancestors(connection, step.workflow_id):
            totals = UsageHistory.workflow_usage(connection, identifier)
            updated = usage.WorkflowUsage(
                own=(
                    totals.own.replace(old, new)
                    if identifier == step.workflow_id
                    else totals.own
                ),
                subtree=totals.subtree.replace(old, new),
            )
            UsageHistory.save_usage(connection, identifier, updated)

    @staticmethod
    def context(step: models.WorkflowStep) -> usage.Context:
        from vectorops.runner.api.llm import observability

        value = (step.metrics or {}).get("llm")
        if value is None:
            return usage.Context(step_id=step.id)
        return observability.Metrics.model_validate(value).context(step.id)

    @staticmethod
    def root_context(
        connection: sqlite3.Connection, root_id: str
    ) -> Optional[usage.Context]:
        """Use the latest generation on the authoritative root path, not row order."""
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'generation_ancestors'"
        ).fetchone():
            row = sql.execute(
                connection,
                sql.Query.from_(HEADS)
                .left_join(ANCESTORS)
                .on(ANCESTORS.step_id == HEADS.step_id)
                .select(
                    sql.Case()
                    .when(ANCESTORS.step_id.isnull(), HEADS.step_id)
                    .else_(ANCESTORS.generation_id)
                )
                .where(HEADS.workflow_id == root_id),
            ).fetchone()
        else:
            row = sql.execute(
                connection,
                sql.Query.from_(HEADS)
                .select(HEADS.step_id)
                .where(HEADS.workflow_id == root_id),
            ).fetchone()
        current = None if row is None or row[0] is None else str(row[0])
        while current is not None:
            row = sql.execute(
                connection,
                sql.Query.from_(STEPS).select(STEPS.payload).where(STEPS.id == current),
            ).fetchone()
            step = models.WorkflowStep.model_validate_json(row[0])
            if step.operation.kind == "llm.generate":
                return UsageHistory.context(step)
            current = step.previous_step_id
        return None
