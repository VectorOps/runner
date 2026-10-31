import asyncio
from decimal import Decimal
from pathlib import Path
import shutil
import sqlite3
from contextlib import closing
from typing import Optional

import pydantic
import pytest

from vectorops.runner import models, usage
from vectorops.runner.runner import execution_state, history_query
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.usage_history import contribution
from vectorops.runner.runner.usage_store import UsageStore
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


def generation(
    identifier: str,
    workflow_id: str = "root",
    *,
    incoming: int = 100,
    outgoing: int = 10,
    cost: str = "0.01",
    previous: str = "",
    streaming: bool = False,
    provider: bool = True,
) -> models.WorkflowStep:
    metrics: dict[str, pydantic.JsonValue] = {
        "input_tokens_estimate": incoming,
        "output_tokens_estimate": outgoing,
        "context_window": 1000,
        "context_remaining_estimate": 1000 - incoming,
        "cost_estimate_usd": cost,
        "cost_basis": "provider_usage" if provider else "token_estimate",
    }
    if provider:
        metrics["provider_usage"] = {
            "input_tokens": incoming,
            "output_tokens": outgoing,
            "completeness": "partial" if streaming else "final",
        }
    return models.WorkflowStep(
        id=identifier,
        workflow_id=workflow_id,
        previous_step_id=previous or None,
        operation=models.StepOperation(kind="llm.generate", name="generate"),
        outcome=models.StepStreaming() if streaming else models.StepCompleted(),
        metrics={"llm": metrics},
    )


async def seed(history: SQLiteHistory, identifier: str = "execution") -> None:
    await history.save_workflow(models.Workflow(id="root", definition_ref="test:main"))
    await history.save_execution(
        execution_state.ExecutionRecord(id=identifier, root_workflow_id="root")
    )


async def spawn(history: SQLiteHistory, parent: str, child: str) -> None:
    await history.save_workflow(models.Workflow(id=child, definition_ref="test:child"))
    await history.append_step(
        models.WorkflowStep(
            id=f"spawn-{child}",
            workflow_id=parent,
            operation=models.WorkflowSpawnOperation(child_workflow_id=child),
            outcome=models.StepCompleted(),
        )
    )


async def test_workflow_tree_persists_own_and_subtree_usage(tmp_path: Path) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await spawn(history, "root", "child")
    await spawn(history, "child", "grandchild")
    await history.append_step(generation("root-call", cost="0.1"))
    await history.append_step(
        generation("child-call", "child", incoming=200, cost="0.2")
    )
    await history.append_step(
        generation("grandchild-call", "grandchild", incoming=300, cost="0.3")
    )
    reopened = SQLiteHistory(history.path)
    root = await reopened.get_workflow("root")
    child = await reopened.get_workflow("child")
    grandchild = await reopened.get_workflow("grandchild")
    assert (
        root.usage is not None
        and child.usage is not None
        and grandchild.usage is not None
    )
    assert root.usage.own.input_tokens == 100
    assert child.usage.own.input_tokens == 200
    assert grandchild.usage.own.input_tokens == 300
    assert child.usage.subtree.cost_estimate_usd == Decimal("0.5")
    assert root.usage.subtree.cost_estimate_usd == Decimal("0.6")
    summary = await reopened.execution_summary()
    assert summary.usage is not None and summary.usage.root_context is not None
    assert summary.usage.totals is not None
    assert (
        summary.usage.totals.cost_estimate_usd == root.usage.subtree.cost_estimate_usd
    )
    assert summary.usage.totals.input_tokens == 600
    assert set(root.usage.subtree.model_dump()) == {
        "cost_estimate_usd",
        "estimated_calls",
        "incomplete_calls",
    }
    assert summary.usage.root_context.step_id == "root-call"
    await reopened.save_workflow(models.Workflow(id="root", definition_ref="test:main"))
    assert (await reopened.get_workflow("root")).usage == root.usage


async def test_summary_is_readonly_while_writer_has_uncommitted_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await history.append_step(generation("call"))

    def forbid_initialization(self: SQLiteHistory) -> None:
        raise AssertionError("summary must not initialize or migrate history")

    monkeypatch.setattr(SQLiteHistory, "_initialize", forbid_initialization)
    with closing(sqlite3.connect(history.path)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE workflows SET payload = json_set(payload, '$.name', 'uncommitted')"
        )
        try:
            summary = await asyncio.wait_for(
                SQLiteHistory(history.path).execution_summary(), 1
            )
        finally:
            writer.rollback()
    assert summary.name is None
    assert summary.usage is not None and summary.usage.root_context is not None
    assert summary.usage.root_context.step_id == "call"
    assert summary.usage.totals is not None
    assert summary.usage.totals.input_tokens == 100


async def test_summary_rejects_unknown_schema_without_mutation(tmp_path: Path) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    with closing(sqlite3.connect(history.path)) as connection:
        connection.execute("PRAGMA user_version = 99")
    with pytest.raises(ValueError, match="schema version"):
        await SQLiteHistory(history.path).execution_summary()
    with closing(sqlite3.connect(history.path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)


async def test_replacement_streaming_and_reset_baseline(tmp_path: Path) -> None:
    store = UsageStore(WorkspaceDatabase(tmp_path))
    history = SQLiteHistory(
        tmp_path / "executions/execution/history.sqlite3", usage_store=store
    )
    await seed(history)
    initial = generation("call", incoming=100, outgoing=1, cost="0.01", streaming=True)
    await history.append_step(initial)
    await history.update_step(initial)
    assert (await store.read()).totals.calls == 1
    reset = await store.read(reset=True)
    assert reset.totals == usage.Totals() and reset.generation == 1
    assert reset.reset_at is not None
    await history.update_step(initial)
    assert (await store.read()).totals == usage.Totals()
    final = generation("call", incoming=100, outgoing=10, cost="0.03")
    await history.update_step(final)
    totals = (await store.read()).totals
    assert totals.input_tokens == 100 and totals.output_tokens == 10
    assert totals.cost_estimate_usd == Decimal("0.03")
    assert totals.estimated_calls == 0
    assert totals.calls == 0
    workflow = await history.get_workflow("root")
    assert workflow.usage is not None
    assert workflow.usage.own.output_tokens == 10
    assert workflow.usage.own.cost_estimate_usd == Decimal("0.03")
    await store.record("execution", 0, usage.Totals())
    assert (await store.read()).totals == totals
    shutil.rmtree(history.path.parent)
    restarted = UsageStore(WorkspaceDatabase(tmp_path))
    assert (await restarted.read()).totals == totals
    assert (await restarted.read(reset=True)).totals == usage.Totals()


async def test_all_branches_cost_but_only_active_root_context(tmp_path: Path) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await history.append_step(generation("first", incoming=100))
    await history.append_step(generation("abandoned", incoming=500, previous="first"))
    await history.append_step(generation("branch", incoming=200, previous="first"))
    await spawn(history, "root", "child")
    await history.append_step(generation("child-call", "child", incoming=900))
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="execution",
            root_workflow_id="root",
            checkpoint=execution_state.ExecutionCheckpoint(heads={"root": "branch"}),
        ),
        replace_heads=True,
    )
    page = await history.query(
        history_query.HistoryQuery(
            execution_id="execution",
            include_children=False,
            limit=1,
        )
    )
    assert len(page.items) == 1
    summary = await history.execution_summary()
    assert summary.usage is not None and summary.usage.root_context is not None
    assert summary.usage.totals is not None
    assert summary.usage.totals.calls == 4
    assert summary.usage.totals.cost_estimate_usd == Decimal("0.04")
    assert summary.usage.root_context.step_id == "branch"
    assert summary.usage.root_context.input_tokens_estimate == 200


@pytest.mark.parametrize("estimate", [None, 123])
@pytest.mark.parametrize(
    "incoming,cached,written,expected",
    [(100, 500, 100, 700), (0, 0, 0, 0), (1200, 0, 0, 1200)],
)
async def test_root_context_prefers_final_cache_inclusive_usage(
    tmp_path: Path,
    estimate: Optional[int],
    incoming: int,
    cached: int,
    written: int,
    expected: int,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    step = generation("call")
    metrics = {
        "llm": {
            "input_tokens_estimate": estimate,
            "output_tokens_estimate": 5,
            "context_window": 1000,
            "output_budget": 200,
            "input_budget": 800,
            "provider_usage": {
                "input_tokens": incoming,
                "cache_read_tokens": cached,
                "cache_write_tokens": written,
                "output_tokens": 20,
                "completeness": "final",
            },
        }
    }
    step = step.model_copy(update={"metrics": metrics})
    await history.append_step(step)
    before = await history.usage_snapshot()
    for owner in (history, SQLiteHistory(history.path)):
        summary = await owner.execution_summary()
        assert summary.usage is not None and summary.usage.root_context is not None
        context = summary.usage.root_context
        assert context.input_tokens_estimate == expected
        assert context.output_tokens_estimate == 20
        assert context.context_window == 1000
        assert context.input_tokens_source == "provider_usage"
        assert await owner.get_step("call") == step
        assert await owner.usage_snapshot() == before


@pytest.mark.parametrize("completeness", ["none", "partial"])
@pytest.mark.parametrize("estimate", [None, 100])
async def test_root_context_partial_usage_is_not_a_measurement(
    tmp_path: Path, completeness: str, estimate: Optional[int]
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    step = generation("call", streaming=True)
    metrics = {
        "llm": {
            "input_tokens_estimate": estimate,
            "context_window": 1000,
            "provider_usage": {"input_tokens": 600, "completeness": completeness},
        }
    }
    step = step.model_copy(update={"metrics": metrics})
    await history.append_step(step)
    summary = await history.execution_summary()
    assert summary.usage is not None
    assert summary.usage.root_context is None
    assert summary.usage.totals.input_tokens == summary.usage.totals.output_tokens == 0


async def test_latest_unknown_context_keeps_last_measurement(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await history.append_step(generation("first"))
    latest = generation("latest", previous="first", streaming=True, provider=False)
    latest = latest.model_copy(
        update={
            "metrics": {"llm": {"input_tokens_estimate": None, "context_window": 1000}}
        }
    )
    await history.append_step(latest)
    summary = await history.execution_summary()
    assert summary.usage is not None and summary.usage.root_context is not None
    assert summary.usage.root_context.step_id == "first"
    assert summary.usage.root_context.input_tokens_estimate == 100


def test_unknown_and_estimated_usage() -> None:
    estimated = contribution(generation("call", provider=False))
    assert estimated == usage.Totals(calls=1, incomplete_calls=1)
    unknown = contribution(generation("call").model_copy(update={"metrics": None}))
    assert unknown.calls == unknown.incomplete_calls == 1
    assert unknown.input_tokens == 0
    assert (
        contribution(
            models.WorkflowStep(
                id="tool",
                workflow_id="root",
                operation=models.StepOperation(kind="llm.tool", name="tool"),
                outcome=models.StepCompleted(),
            )
        )
        == usage.Totals()
    )
    metrics = generation("call").metrics
    assert metrics is not None and isinstance(metrics["llm"], dict)
    metrics["llm"]["retries"] = 2
    assert (
        contribution(
            generation("call").model_copy(update={"metrics": metrics})
        ).incomplete_calls
        == 1
    )


async def test_concurrent_executions_count_once(tmp_path: Path) -> None:
    store = UsageStore(WorkspaceDatabase(tmp_path))
    histories = [
        SQLiteHistory(
            tmp_path / f"executions/{index}/history.sqlite3", usage_store=store
        )
        for index in range(5)
    ]
    await asyncio.gather(
        *(seed(history, str(index)) for index, history in enumerate(histories))
    )
    await asyncio.gather(
        *(history.append_step(generation("call")) for history in histories)
    )
    assert (await store.read()).totals.calls == 5
    assert (await store.read()).totals.cost_estimate_usd == Decimal("0.05")


async def test_existing_history_backfill_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "executions/execution/history.sqlite3"
    history = SQLiteHistory(path)
    await seed(history)
    await spawn(history, "root", "child")
    await history.append_step(generation("child-call", "child"))
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP TABLE usage_state")
        connection.execute("DROP TABLE workflow_parents")
        connection.commit()
    store = UsageStore(WorkspaceDatabase(tmp_path))
    reopened = SQLiteHistory(path, usage_store=store)
    await reopened.get_execution()
    await reopened.get_execution()
    revision, total = await reopened.usage_snapshot()
    await store.record("execution", revision, total)
    root = await reopened.get_workflow("root")
    assert root.usage is not None and root.usage.subtree.cost_estimate_usd == Decimal(
        "0.01"
    )
    assert (await store.read()).totals.calls == 1


@pytest.mark.parametrize(
    "field", ["input_tokens", "output_tokens", "cost_estimate_usd"]
)
def test_negative_usage_is_invalid(field: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        usage.Totals.model_validate({field: -1})


async def test_failed_batch_rolls_back_usage_with_steps(tmp_path: Path) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    initial = generation("stream", streaming=True)
    await history.append_step(initial)
    await history.append_step(generation("terminal"))
    before = (await history.execution_summary()).usage
    with pytest.raises(ValueError, match="terminal steps"):
        await history.update_steps(
            [
                generation("stream", cost="0.5"),
                generation("terminal", cost="0.5"),
            ]
        )
    assert await history.get_step("stream") == initial
    assert (await history.execution_summary()).usage == before


async def test_workspace_failure_recovers_from_committed_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = UsageStore(WorkspaceDatabase(tmp_path))
    history = SQLiteHistory(
        tmp_path / "executions/execution/history.sqlite3", usage_store=store
    )
    await seed(history)

    async def fail(execution_id: str, revision: int, total: usage.Totals) -> None:
        raise OSError("workspace write failed")

    with monkeypatch.context() as patch:
        patch.setattr(store, "record", fail)
        with pytest.raises(OSError, match="workspace write failed"):
            await history.append_step(generation("call"))
    assert (await history.get_step("call")).outcome.kind == "completed"
    revision, total = await history.usage_snapshot()
    await store.record("execution", revision, total)
    await store.record("execution", revision, total)
    assert (await store.read()).totals.calls == 1


async def test_linking_existing_child_rolls_up_once_and_rejects_cycles(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await history.save_workflow(
        models.Workflow(id="child", definition_ref="test:child")
    )
    await history.append_step(generation("call", "child"))
    await spawn(history, "root", "child")
    before = await history.get_workflow("root")
    assert (
        before.usage is not None
        and before.usage.subtree.cost_estimate_usd == Decimal("0.01")
    )
    with pytest.raises(ValueError, match="cycle"):
        await history.append_step(
            models.WorkflowStep(
                id="cycle",
                workflow_id="child",
                operation=models.WorkflowSpawnOperation(child_workflow_id="root"),
                outcome=models.StepCompleted(),
            )
        )
    assert await history.get_workflow("root") == before
    with pytest.raises(KeyError):
        await history.get_step("cycle")


async def test_coverage_replacement_clears_only_completed_contribution(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    await spawn(history, "root", "child")
    await history.append_step(generation("root-call", streaming=True, provider=False))
    await history.append_step(generation("child-call", "child", streaming=True))
    root = await history.get_workflow("root")
    assert root.usage is not None
    assert root.usage.subtree.estimated_calls == 0
    assert root.usage.subtree.incomplete_calls == 2
    await history.update_step(generation("root-call"))
    root = await history.get_workflow("root")
    assert root.usage is not None
    assert root.usage.own.estimated_calls == 0
    assert root.usage.subtree.estimated_calls == 0
    assert root.usage.subtree.incomplete_calls == 1
    await history.update_step(generation("child-call", "child"))
    root = await history.get_workflow("root")
    assert root.usage is not None and root.usage.subtree.incomplete_calls == 0


async def test_usage_reconciles_once_and_recovers_on_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "config.toml").write_text("")
    history = SQLiteHistory(tmp_path / "data/executions/execution/history.sqlite3")
    await seed(history)
    await history.append_step(generation("first"))
    runner = WorkflowRunner(tmp_path)
    assert (await runner.runner_usage()).totals.calls == 1
    index = await runner.execution_index()

    async def unexpected_scan() -> list[str]:
        raise AssertionError("usage rescanned execution history")

    monkeypatch.setattr(index, "discover", unexpected_scan)
    await runner.initialize_usage()
    assert (await runner.runner_usage()).totals.calls == 1
    await runner.runner_usage(reset=True)
    project = await runner.project()
    writer = SQLiteHistory(history.path, usage_store=project.usage)
    await writer.append_step(generation("second"))
    assert (await runner.runner_usage()).totals.calls == 1

    async def fail(execution_id: str, revision: int, total: usage.Totals) -> None:
        raise OSError("workspace write failed")

    with monkeypatch.context() as patch:
        patch.setattr(project.usage, "record", fail)
        with pytest.raises(OSError):
            await writer.append_step(generation("third"))
    restarted = WorkflowRunner(tmp_path)
    counters = await restarted.runner_usage()
    assert counters.generation == 1 and counters.totals.calls == 2
    assert (await restarted.runner_usage()).totals.calls == 2


async def test_runs_do_not_repeat_usage_reconciliation(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.initialize_usage()
    index = await runner.execution_index()

    async def unexpected_scan() -> list[str]:
        raise AssertionError("run rescanned execution history")

    monkeypatch.setattr(index, "discover", unexpected_scan)
    await runner.run("activities:workflow")
    await runner.run("activities:workflow")
    assert (await runner.runner_usage()).totals == usage.Totals()


def test_measured_zero_and_unknown_cost_use_existing_coverage() -> None:
    zero = contribution(generation("zero", incoming=0, outgoing=0, cost="0"))
    assert zero.calls == 1 and zero.incomplete_calls == 0
    assert zero.cost_estimate_usd == Decimal(0)
    assert zero.input_tokens == 0
    assert usage.Totals.model_validate_json(zero.model_dump_json()) == zero
    assert zero.since(zero) == usage.Totals()
    step = generation("unpriced").model_copy(
        update={
            "metrics": {
                "llm": {"provider_usage": {"input_tokens": 10, "completeness": "final"}}
            }
        }
    )
    unpriced = contribution(step)
    assert unpriced.calls == unpriced.incomplete_calls == 1
    assert unpriced.cost_estimate_usd == Decimal(0)


async def test_cached_finalization_counts_only_confirmed_usage_once(
    tmp_path: Path,
) -> None:
    store = UsageStore(WorkspaceDatabase(tmp_path / "workspace"))
    history = SQLiteHistory(tmp_path / "execution/history.sqlite3", usage_store=store)
    await seed(history)
    await history.append_step(generation("first", incoming=5000, cost="0.5"))
    baseline = (await store.read()).totals
    initial = generation(
        "cached",
        previous="first",
        incoming=511000,
        outgoing=9000,
        cost="20",
        streaming=True,
        provider=False,
    )
    await history.append_step(initial)
    for provider in (False, True):
        await history.update_step(
            generation(
                "cached",
                previous="first",
                incoming=511000,
                outgoing=9000,
                cost="20",
                streaming=True,
                provider=provider,
            )
        )
        pending = (await store.read()).totals
        assert pending.input_tokens == baseline.input_tokens
        assert pending.output_tokens == baseline.output_tokens
        assert pending.cost_estimate_usd == baseline.cost_estimate_usd
    final = generation(
        "cached", previous="first", incoming=2000, outgoing=20, cost="0.07"
    )
    assert final.metrics is not None and isinstance(final.metrics["llm"], dict)
    final.metrics["llm"]["provider_usage"] = {
        "input_tokens": 2000,
        "cache_read_tokens": 700000,
        "cache_write_tokens": 5000,
        "output_tokens": 20,
        "reasoning_tokens": 5,
        "completeness": "final",
    }
    await history.update_step(final)
    await history.update_step(final)
    revision, totals = await history.usage_snapshot()
    await store.record("execution", revision, totals)
    await store.record("execution", revision, totals)
    assert (await store.read()).totals == totals
    assert totals.input_tokens == 7000
    assert totals.cache_read_tokens == 700000 and totals.cache_write_tokens == 5000
    assert (
        totals.input_tokens + totals.cache_read_tokens + totals.cache_write_tokens
        == 712000
    )
    assert totals.output_tokens == 30 and totals.reasoning_tokens == 5
    assert totals.cost_estimate_usd == Decimal("0.57")
    assert totals.calls == 2 and totals.estimated_calls == 0


async def test_measured_context_stays_stable_then_accepts_reduction_and_branch(
    tmp_path: Path,
) -> None:
    summaries = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    history = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=summaries)
    await seed(history)
    await history.append_step(generation("first", incoming=704000))
    await history.append_step(
        generation(
            "next", previous="first", incoming=511000, streaming=True, provider=False
        )
    )
    for summary in (
        await history.execution_summary(),
        await summaries.get("execution"),
    ):
        assert summary.usage is not None and summary.usage.root_context is not None
        assert summary.usage.root_context.step_id == "first"
        assert summary.usage.root_context.input_tokens_estimate == 704000
    await history.update_step(generation("next", previous="first", incoming=707000))
    await history.append_step(generation("smaller", previous="next", incoming=100000))
    for owner in (history, SQLiteHistory(history.path)):
        summary = await owner.execution_summary()
        assert summary.usage is not None and summary.usage.root_context is not None
        assert summary.usage.root_context.step_id == "smaller"
        assert summary.usage.root_context.input_tokens_estimate == 100000
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="execution",
            root_workflow_id="root",
            checkpoint=execution_state.ExecutionCheckpoint(heads={"root": "first"}),
        ),
        replace_heads=True,
    )
    projected = await summaries.get("execution")
    assert projected.usage is not None and projected.usage.root_context is not None
    assert projected.usage.root_context.step_id == "first"
    assert projected.usage.root_context.input_tokens_estimate == 704000


async def test_existing_usage_is_not_rebuilt_or_schema_changed(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    first = generation("first", incoming=100, cost="0.1")
    pending = generation(
        "pending",
        previous="first",
        incoming=500000,
        cost="10",
        streaming=True,
        provider=False,
    )
    await history.append_step(first)
    await history.append_step(pending)
    before_revision, _ = await history.usage_snapshot()
    with closing(sqlite3.connect(history.path)) as connection, connection:
        connection.execute(
            "UPDATE usage_state SET payload = ?",
            (
                '{"input_tokens":500100,"output_tokens":20,"cost_estimate_usd":"10.1","calls":2,"estimated_calls":1,"incomplete_calls":0}',
            ),
        )
        connection.execute(
            "UPDATE workflows SET payload = json_set(payload, '$.usage', json(?))",
            (
                '{"own":{"input_tokens":500100,"estimated_calls":1},"subtree":{"estimated_calls":1}}',
            ),
        )
    reopened = SQLiteHistory(history.path)
    revision, totals = await reopened.usage_snapshot()
    assert revision == before_revision
    assert totals.input_tokens == 500100 and totals.estimated_calls == 1
    assert totals.cost_estimate_usd == Decimal("10.1")
    assert await reopened.get_step("first") == first
    assert await reopened.get_step("pending") == pending
    assert await SQLiteHistory(history.path).usage_snapshot() == (revision, totals)
    with closing(sqlite3.connect(history.path)) as connection:
        assert [
            row[1] for row in connection.execute("PRAGMA table_info(usage_state)")
        ] == ["id", "revision", "payload"]
    workflow = await reopened.get_workflow("root")
    assert workflow.usage is not None
    assert workflow.usage.own.input_tokens == 500100


async def test_existing_workspace_usage_and_reset_baseline_are_preserved(
    tmp_path: Path,
) -> None:
    workspace = WorkspaceDatabase(tmp_path)

    def legacy(connection: sqlite3.Connection) -> None:
        with connection:
            connection.execute(
                "CREATE TABLE usage_executions (execution_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO usage_executions VALUES ('execution', ?)",
                (
                    '{"revision":999,"total":{"input_tokens":500000,"estimated_calls":1},"baseline":{"estimated_calls":0}}',
                ),
            )
            WorkspaceDatabase.put_document(
                connection,
                "usage",
                '{"totals":{"input_tokens":500000,"estimated_calls":1}}',
            )

    await workspace.run(legacy)
    store = UsageStore(workspace)
    existing = (await store.read()).totals
    assert existing.input_tokens == 500000 and existing.estimated_calls == 1
    measured = contribution(generation("first"))
    await store.record("execution", 1, measured)
    assert (await store.read()).totals == existing
    restarted = UsageStore(workspace)
    assert (await restarted.read()).totals == existing
    await restarted.record("new-execution", 1, measured)
    assert (await restarted.read()).totals.input_tokens == 500100
    assert (await restarted.read(reset=True)).totals == usage.Totals()
    assert (await restarted.read()).totals == usage.Totals()
    names = await workspace.run(
        lambda connection: [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
    )
    assert "usage_executions" in names
    assert "usage_confirmed_executions" not in names


def test_legacy_usage_preserves_sums_and_reset_baselines() -> None:
    legacy = {
        "input_tokens": 100,
        "output_tokens": 10,
        "calls": 1,
        "cost_estimate_usd": "0.01",
        "estimated_token_calls": 1,
        "estimated_cost_calls": 1,
        "unknown_input_calls": 0,
        "unknown_output_calls": 0,
        "unknown_cost_calls": 0,
        "partial_provider_calls": 1,
        "unrecorded_attempts": 0,
    }
    totals = usage.Totals.model_validate(legacy)
    workflow = usage.WorkflowUsage.model_validate({"own": legacy, "subtree": legacy})
    assert workflow.own == totals
    assert workflow.subtree.cost_estimate_usd == Decimal("0.01")
    assert totals.estimated_calls == 2 and totals.incomplete_calls == 1
    assert totals.since(usage.Totals.model_validate(legacy)) == usage.Totals()
