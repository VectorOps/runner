"""Transactional usage accounting owned by execution history."""

import sqlite3
from decimal import Decimal
from typing import Optional

from vectorops.runner import models, usage


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
        connection.execute(
            "INSERT INTO usage_state VALUES (1, 0, ?)",
            (usage.Totals().model_dump_json(),),
        )
        connection.execute(
            "UPDATE workflows SET payload = json_remove(payload, '$.usage')"
        )
        steps = [
            models.WorkflowStep.model_validate_json(row[0])
            for row in connection.execute("SELECT payload FROM steps ORDER BY rowid")
        ]
        for step in steps:
            UsageHistory.link(connection, step)
        for step in steps:
            UsageHistory.update(connection, step)

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
    def link(connection: sqlite3.Connection, step: models.WorkflowStep) -> None:
        if not isinstance(step.operation, models.WorkflowSpawnOperation):
            return
        child = step.operation.child_workflow_id
        parent = step.workflow_id
        row = connection.execute(
            "SELECT parent_id FROM workflow_parents WHERE child_id = ?", (child,)
        ).fetchone()
        if row is not None:
            if row[0] != parent:
                raise ValueError("workflow has multiple parents")
            return
        ancestors = UsageHistory.ancestors(connection, parent)
        if child in ancestors:
            raise ValueError("cycle in workflow tree")
        connection.execute(
            "INSERT INTO workflow_parents VALUES (?, ?)", (child, parent)
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
            row = connection.execute(
                "SELECT parent_id FROM workflow_parents WHERE child_id = ?", (current,)
            ).fetchone()
            current = None if row is None else str(row[0])
        return result

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
    ) -> None:
        old = usage.Totals() if previous is None else contribution(previous)
        new = contribution(step)
        if old == new:
            return
        revision, total = UsageHistory.snapshot(connection)
        connection.execute(
            "UPDATE usage_state SET revision = ?, payload = ? WHERE id = 1",
            (revision + 1, total.replace(old, new).model_dump_json()),
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
    def root_context(
        connection: sqlite3.Connection, root_id: str
    ) -> Optional[usage.Context]:
        """Use the latest generation on the authoritative root path, not row order."""
        from vectorops.runner.api.llm import observability

        row = connection.execute(
            "SELECT step_id FROM workflow_heads WHERE workflow_id = ?", (root_id,)
        ).fetchone()
        current = None if row is None else str(row[0])
        while current is not None:
            row = connection.execute(
                "SELECT payload FROM steps WHERE id = ?", (current,)
            ).fetchone()
            step = models.WorkflowStep.model_validate_json(row[0])
            if step.operation.kind == "llm.generate":
                value = (step.metrics or {}).get("llm")
                if value is None:
                    return usage.Context(step_id=step.id)
                metrics = observability.Metrics.model_validate(value)
                return metrics.context(step.id)
            current = step.previous_step_id
        return None
