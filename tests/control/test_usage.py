from decimal import Decimal
from pathlib import Path

import pytest

from vectorops.runner import usage
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import execution_state
from vectorops.runner.runner.sqlite_history import SQLiteHistory

from tests.runner.test_usage import generation, seed, spawn


async def test_usage_api_restart_reset_and_workflow_state(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text("")
    path = tmp_path / "data/executions/execution/history.sqlite3"
    history = SQLiteHistory(path)
    await seed(history)
    await spawn(history, "root", "child")
    await history.append_step(generation("root-call", cost="0.1"))
    await history.append_step(generation("child-call", "child", cost="0.2"))
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="execution",
            root_workflow_id="root",
            status="completed",
        )
    )
    async with client.embedded(tmp_path) as connection:
        counters = await connection.runner_usage()
        assert counters.totals.calls == 2
        assert counters.totals.cost_estimate_usd == Decimal("0.3")
        workflow = await connection.workflow_state("execution", "root")
        assert workflow.usage is not None
        assert workflow.usage.own.input_tokens == 100
        assert workflow.usage.subtree.cost_estimate_usd == Decimal("0.3")
        summary = await connection.execution_summary("execution")
        assert summary.usage is not None and summary.usage.root_context is not None
        assert summary.usage.root_context.step_id == "root-call"
        assert summary.usage.totals == counters.totals
        summaries = await connection.execution_summaries()
        assert summaries.executions[0].usage == summary.usage
        reset = await connection.reset_runner_usage()
        assert reset.generation == 1 and reset.totals == usage.Totals()
        assert (await connection.execution_summary("execution")).usage == summary.usage
        assert (
            await connection.workflow_state("execution", "root")
        ).usage == workflow.usage
        with pytest.raises(protocol.RemoteError):
            await connection.request("runner.usage.reset", {"unexpected": True})
    async with client.embedded(tmp_path) as connection:
        counters = await connection.runner_usage()
        assert counters.generation == 1 and counters.totals == usage.Totals()


async def test_post_reset_execution_is_counted_without_old_history(
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text("")
    async with client.embedded(tmp_path) as connection:
        await connection.reset_runner_usage()
    history = SQLiteHistory(tmp_path / "data/executions/new/history.sqlite3")
    await seed(history, "new")
    await history.append_step(generation("call"))
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="new",
            root_workflow_id="root",
            status="completed",
        )
    )
    async with client.embedded(tmp_path) as connection:
        counters = await connection.runner_usage()
        assert counters.generation == 1 and counters.totals.calls == 1


async def test_context_api_reports_measured_image_usage_after_restart(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "data/executions/execution/history.sqlite3")
    await seed(history)
    step = generation("image-call")
    metrics = {
        "llm": {
            "input_tokens_estimate": None,
            "context_window": 1050000,
            "provider_usage": {
                "input_tokens": 2438,
                "cache_read_tokens": 303616,
                "completeness": "final",
            },
        }
    }
    step = step.model_copy(update={"metrics": metrics})
    await history.append_step(step)
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="execution",
            root_workflow_id="root",
            status="completed",
        )
    )
    async with client.embedded(tmp_path) as connection:
        summary = await connection.execution_summary("execution")
        assert summary.usage is not None and summary.usage.root_context is not None
        assert summary.usage.root_context.input_tokens == 306054
        assert summary.usage.root_context.context_window == 1050000
        assert (await connection.execution_summaries()).executions[
            0
        ].usage == summary.usage
    assert (await history.get_step("image-call")).metrics == step.metrics


async def test_usage_api_has_null_measurements_until_provider_reports(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "data/executions/execution/history.sqlite3")
    await seed(history)
    await history.append_step(generation("pending", streaming=True, provider=False))
    await history.save_execution(
        execution_state.ExecutionRecord(
            id="execution", root_workflow_id="root", status="paused"
        )
    )
    async with client.embedded(tmp_path) as connection:
        summary = await connection.execution_summary("execution")
        assert summary.usage is not None
        assert summary.usage.totals is None and summary.usage.root_context is None
        assert (await connection.runner_usage()).totals.cost_estimate_usd is None
    await history.update_step(generation("pending", incoming=0, outgoing=0, cost="0"))
    async with client.embedded(tmp_path) as connection:
        summary = await connection.execution_summary("execution")
        assert summary.usage is not None and summary.usage.totals is not None
        assert summary.usage.totals.input_tokens == 0
        assert summary.usage.totals.cost_estimate_usd == Decimal(0)
        assert summary.usage.root_context is not None
        assert summary.usage.root_context.input_tokens == 0
