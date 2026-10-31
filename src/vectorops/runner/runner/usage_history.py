"""Transactional usage accounting owned by execution history."""

import sqlite3
from dataclasses import dataclass
from typing import Final, Optional

from vectorops.runner import models, usage
from vectorops.runner.runner import sql

WORKFLOWS: Final = sql.Table("workflows")
STEPS: Final = sql.Table("steps")
PARENTS: Final = sql.Table("workflow_parents")
STATE: Final = sql.Table("usage_state")
ANCESTORS: Final = sql.Table("generation_ancestors")
HEADS: Final = sql.Table("workflow_heads")


@dataclass(frozen=True)
class Change:
    revision: int
    totals: usage.Totals
    workflows: tuple[str, ...]


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
    if provider is None or provider.completeness != "final":
        return usage.Totals(calls=1, incomplete_calls=1)
    cost = metrics.cost_estimate_usd if metrics.cost_basis == "provider_usage" else None
    return usage.Totals(
        input_tokens=provider.input_tokens,
        output_tokens=provider.output_tokens,
        reasoning_tokens=provider.reasoning_tokens,
        cache_read_tokens=provider.cache_read_tokens,
        cache_write_tokens=provider.cache_write_tokens,
        cost_estimate_usd=cost,
        calls=1,
        confirmed_calls=1,
        priced_calls=int(cost is not None),
        incomplete_calls=int(cost is None or metrics.retries > 0),
    )


class UsageHistory:
    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        """Backfill existing histories once, in the same transaction as the new state."""
        UsageHistory._initialize_context(connection)
        existing = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'usage_state'"
        ).fetchone()
        revision = 0
        if existing:
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(usage_state)")
            }
            if "accounting_version" in columns:
                return
            revision = UsageHistory.snapshot_revision(connection) + 1
            connection.execute("DROP TABLE usage_state")
            connection.execute("DROP TABLE workflow_parents")
            connection.execute(
                "UPDATE summary_state SET revision = revision + 1 WHERE id = 1"
            )
        connection.execute(
            "CREATE TABLE workflow_parents (child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE usage_state (id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL, payload TEXT NOT NULL, accounting_version INTEGER NOT NULL)"
        )
        sql.execute(
            connection,
            sql.Query.into(STATE).insert(
                1, revision, usage.Totals().model_dump_json(), 2
            ),
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
        row = connection.execute(
            "SELECT json_extract(payload, '$.usage') FROM workflows WHERE id = ?",
            (workflow_id,),
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
        connection.execute(
            "UPDATE workflows SET payload = json_set(payload, '$.usage', json(?)) WHERE id = ?",
            (totals.model_dump_json(), workflow_id),
        )

    @staticmethod
    def link(
        connection: sqlite3.Connection, step: models.WorkflowStep
    ) -> tuple[str, ...]:
        UsageHistory._link_context(connection, step)
        if not isinstance(step.operation, models.WorkflowSpawnOperation):
            return ()
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
            return ()
        ancestors = UsageHistory.ancestors(connection, parent)
        if child in ancestors:
            raise ValueError("cycle in workflow tree")
        sql.execute(
            connection,
            sql.Query.into(PARENTS).insert(child, parent),
        )
        child_usage = UsageHistory.workflow_usage(connection, child)
        changed = []
        for identifier in ancestors:
            totals = UsageHistory.workflow_usage(connection, identifier)
            updated = totals.model_copy(
                update={
                    "subtree": totals.subtree.replace(usage.Cost(), child_usage.subtree)
                }
            )
            if updated != totals:
                UsageHistory.save_usage(connection, identifier, updated)
                changed.append(identifier)
        return tuple(changed)

    @staticmethod
    def ancestors(connection: sqlite3.Connection, workflow_id: str) -> list[str]:
        result = []
        current: Optional[str] = workflow_id
        while current is not None:
            if current in result:
                raise ValueError("cycle in workflow tree")
            result.append(current)
            row = connection.execute(
                "SELECT parent_id FROM workflow_parents WHERE child_id = ?",
                (current,),
            ).fetchone()
            current = None if row is None else str(row[0])
        return result

    @staticmethod
    def snapshot_revision(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT revision FROM usage_state WHERE id = 1"
            ).fetchone()[0]
        )

    @staticmethod
    def snapshot(connection: sqlite3.Connection) -> tuple[int, usage.Totals]:
        row = connection.execute(
            "SELECT revision, payload FROM usage_state WHERE id = 1"
        ).fetchone()
        return int(row[0]), usage.Totals.model_validate_json(row[1])

    @staticmethod
    def update(
        connection: sqlite3.Connection,
        step: models.WorkflowStep,
        previous: Optional[models.WorkflowStep] = None,
    ) -> Optional[Change]:
        old = usage.Totals() if previous is None else contribution(previous)
        new = contribution(step)
        if old == new:
            return None
        revision, total = UsageHistory.snapshot(connection)
        total = total.replace(old, new)
        connection.execute(
            "UPDATE usage_state SET revision = ?, payload = ? WHERE id = 1",
            (revision + 1, total.model_dump_json()),
        )
        ancestors = UsageHistory.ancestors(connection, step.workflow_id)
        changed = []
        for identifier in ancestors:
            totals = UsageHistory.workflow_usage(connection, identifier)
            updated = usage.WorkflowUsage(
                own=(
                    totals.own.replace(old, new)
                    if identifier == step.workflow_id
                    else totals.own
                ),
                subtree=totals.subtree.replace(old, new),
            )
            if updated != totals:
                UsageHistory.save_usage(connection, identifier, updated)
                changed.append(identifier)
        return Change(revision + 1, total, tuple(changed))

    @staticmethod
    def context(step: models.WorkflowStep) -> Optional[usage.Context]:
        from vectorops.runner.api.llm import observability

        value = (step.metrics or {}).get("llm")
        if value is None:
            return None
        return observability.Metrics.model_validate(value).context(step.id)

    @staticmethod
    def root_context(
        connection: sqlite3.Connection, root_id: str
    ) -> Optional[usage.Context]:
        """Keep the last finalized measurement on the selected root path until replaced."""
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
                context = UsageHistory.context(step)
                if context is not None:
                    return context
            current = step.previous_step_id
        return None
