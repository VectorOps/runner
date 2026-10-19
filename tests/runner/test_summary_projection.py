import asyncio
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed, spawn
from vectorops.runner import models
from vectorops.runner.runner import execution_state, mailbox, summary_history
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_committed_names_usage_inputs_and_heads_update_projection(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await runner.initialize_usage()
    index = await runner.execution_index()
    with patch.object(index, "discover", side_effect=AssertionError("archive scan")):
        await history.set_workflow_name("root", "Projected title")
        await history.append_step(generation("first"))
        await history.append_step(generation("second", previous="first", incoming=300))
        await spawn(history, "root", "child")
        await history.append_step(generation("child-call", "child"))
        record = await history.get_execution()
        await history.save_execution(
            record.model_copy(
                update={
                    "checkpoint": execution_state.ExecutionCheckpoint(
                        heads={"root": "first"}
                    )
                }
            ),
            replace_heads=True,
        )
        await history.append_step(
            models.WorkflowStep(
                id="input",
                workflow_id="root",
                previous_step_id="first",
                operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
                outcome=models.StepPending(),
                deferred_delivery=True,
            )
        )
        await history.register_input("input", mailbox.InputSpec(channel="approval"))
        projected = await project.summaries.get("execution")
        assert projected == await history.execution_summary()
        assert projected.name == "Projected title"
        assert projected.pending_inputs == 1
        assert projected.usage is not None and projected.usage.root_context is not None
        assert projected.usage.totals.input_tokens == 500
        assert projected.usage.root_context.step_id == "first"
        await history.cancel_inputs()
        assert (await project.summaries.get("execution")).pending_inputs == 0
        with patch.object(
            runner, "_selected_database", side_effect=AssertionError("history read")
        ):
            assert await runner.execution_ids() == ["execution"]
            assert (
                await runner.execution_summary("execution")
            ).name == "Projected title"


async def test_stale_revisions_and_deleted_sources_cannot_restore_old_stats(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    old = await history.summary_snapshot()
    await history.set_workflow_name("root", "New title")
    new = await history.summary_snapshot()
    assert new.revision > old.revision
    await project.summaries.record(old)
    assert (await project.summaries.get("execution")).name == "New title"
    await project.summaries.remove(["execution"])
    await project.summaries.record(new)
    with pytest.raises(KeyError):
        await project.summaries.get("execution")
    assert await project.database.index_page(None, None) == []
    shutil.rmtree(history.path.parent)
    replacement = SQLiteHistory(history.path, summary_store=project.summaries)
    await seed(replacement)
    assert (await replacement.summary_snapshot()).source_id != old.source_id
    await project.summaries.record(new)
    assert (await project.summaries.get("execution")).name is None


async def test_projection_failure_preserves_commit_and_restart_repairs_it(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await runner.initialize_usage()
    with patch.object(
        project.summaries, "record", side_effect=RuntimeError("unavailable")
    ):
        await history.set_workflow_name("root", "Committed despite failure")
    assert (await history.execution_summary()).name == "Committed despite failure"
    assert (await project.summaries.get("execution")).name is None
    restarted = WorkflowRunner(tmp_path)
    await restarted.initialize_usage()
    assert (
        await restarted.execution_summary("execution")
    ).name == "Committed despite failure"


async def test_rollback_and_noop_do_not_advance_projected_revision(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await history.set_workflow_name("root", "Original")
    await history.append_step(generation("call", streaming=True))
    before = await history.summary_snapshot()
    with patch.object(project.summaries, "record", new=AsyncMock()) as publish:
        await history.set_workflow_name("root", "Ignored")
        with pytest.raises(KeyError):
            await history.update_steps((generation("call"), generation("missing")))
        publish.assert_not_awaited()
    assert await history.summary_snapshot() == before
    assert await project.summaries.get("execution") == before.summary


async def test_projection_and_notification_survive_repeated_cancellation(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    changes: list[tuple[str, ...]] = []
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
        change_sink=changes.append,
    )
    await seed(history)
    changes.clear()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = project.summaries.record

    async def publish(snapshot: summary_history.Snapshot) -> None:
        entered.set()
        await release.wait()
        await original(snapshot)

    with patch.object(project.summaries, "record", side_effect=publish):
        task = asyncio.create_task(history.set_workflow_name("root", "Committed"))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert changes == [("root",)]
    assert (await project.summaries.get("execution")).name == "Committed"


async def test_startup_reconciles_deleted_and_external_histories(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    path = project.data_path / "executions/old/history.sqlite3"
    await seed(SQLiteHistory(path), "old")
    await runner.initialize_usage()
    shutil.rmtree(path.parent)
    await seed(
        SQLiteHistory(project.data_path / "executions/new/history.sqlite3"), "new"
    )
    restarted = WorkflowRunner(tmp_path)
    await restarted.initialize_usage()
    assert await restarted.execution_ids() == ["new"]
    with pytest.raises(KeyError):
        await (await restarted.project()).summaries.get("old")


async def test_summary_reconciliation_rejects_concurrent_publication(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    generation_id, _ = await project.database.index_status()
    old = await history.summary_snapshot()
    await history.set_workflow_name("root", "Concurrent")
    assert not await project.summaries.reconcile(generation_id, [old])
    assert (await project.summaries.get("execution")).name == "Concurrent"


async def test_legacy_history_and_workspace_are_backfilled(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(project.data_path / "executions/execution/history.sqlite3")
    await seed(history)
    await history.append_step(generation("call"))
    with closing(sqlite3.connect(history.path)) as connection, connection:
        triggers = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'summary_%'"
        ).fetchall()
        for (name,) in triggers:
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute("DROP TABLE summary_state")
    with closing(sqlite3.connect(project.database.path)) as connection, connection:
        connection.execute("DROP TABLE execution_summaries")
        connection.execute("DROP TABLE summary_tombstones")
        connection.execute("PRAGMA user_version = 1")
    restarted = WorkflowRunner(tmp_path)
    await restarted.initialize_usage()
    result = await restarted.execution_summary("execution")
    assert result.usage is not None and result.usage.totals.calls == 1
    assert await restarted.execution_ids() == ["execution"]
    with closing(sqlite3.connect(project.database.path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)


async def test_startup_rebuilds_corrupt_projection_and_restored_source(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await history.set_workflow_name("root", "Authoritative")
    await runner.initialize_usage()
    with closing(sqlite3.connect(project.database.path)) as connection, connection:
        connection.execute("UPDATE execution_summaries SET payload = 'corrupt'")
        connection.execute("DELETE FROM execution_index")
    restarted = WorkflowRunner(tmp_path)
    await restarted.initialize_usage()
    assert (await restarted.execution_summary("execution")).name == "Authoritative"
    assert await restarted.execution_ids() == ["execution"]
    await project.summaries.remove(["execution"])
    await (await restarted.execution_index()).reconcile()
    assert (await restarted.execution_summary("execution")).name == "Authoritative"
    assert await restarted.execution_ids() == ["execution"]


@pytest.mark.parametrize("has_generation", [False, True])
async def test_long_root_context_uses_materialized_generation_link(
    tmp_path: Path,
    has_generation: bool,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    previous = None
    if has_generation:
        await history.append_step(generation("call"))
        previous = "call"
    for index in range(300):
        step_id = f"step-{index}"
        await history.append_step(
            models.WorkflowStep(
                id=step_id,
                workflow_id="root",
                previous_step_id=previous,
                operation=models.StepOperation(kind="activity", name="echo"),
                outcome=models.StepCompleted(),
            )
        )
        previous = step_id
    with patch.object(
        models.WorkflowStep,
        "model_validate_json",
        wraps=models.WorkflowStep.model_validate_json,
    ) as parse:
        result = await history.execution_summary()
        assert parse.call_count == int(has_generation)
    assert result.usage is not None
    if has_generation:
        assert result.usage.root_context is not None
        assert result.usage.root_context.step_id == "call"
    else:
        assert result.usage.root_context is None
    with closing(sqlite3.connect(history.path)) as connection, connection:
        connection.execute("DROP TABLE generation_ancestors")
    legacy = SQLiteHistory(history.path)
    assert await legacy.execution_summary() == result
    await legacy.get_execution()
    assert await legacy.execution_summary() == result
