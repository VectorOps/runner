import asyncio
from pathlib import Path
import sqlite3
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed
from vectorops.runner import models
from vectorops.runner.runner import execution_state, summary_history
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


@pytest.mark.parametrize("heads", [8, 256, 2304])
async def test_periodic_activity_does_not_read_retained_heads(
    tmp_path: Path, heads: int
) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    async with SQLiteHistory(
        tmp_path / "history.sqlite3", summary_store=store
    ) as database:
        await seed(database)
        for index in range(heads):
            identifier = f"child-{index}"
            await database.save_workflow(
                models.Workflow(id=identifier, definition_ref="test")
            )
            await database.append_step(
                models.WorkflowStep(
                    id=f"done-{index}",
                    workflow_id=identifier,
                    operation=models.WorkflowCompleteOperation(),
                    outcome=models.StepCompleted(result="x" * 16384),
                )
            )
        await database.append_step(generation("stream", streaming=True))
        record = await database.get_execution()
        statements: list[str] = []
        await database._invoke(
            lambda connection: connection.set_trace_callback(statements.append)
        )
        async with CachedHistory(database, flush_interval=60) as cache:
            checkpoint = ExecutionCheckpointer(database, cache, record, interval=0.01)
            committed = asyncio.Event()
            original = database.checkpoint_execution

            async def persist(
                identifier: str,
                *,
                status: Optional[execution_state.ExecutionStatus] = None,
            ) -> None:
                await original(identifier, status=status)
                committed.set()

            with (
                patch.object(database, "checkpoint_execution", side_effect=persist),
                patch.object(
                    database, "_step", side_effect=AssertionError("head step read")
                ),
                patch.object(
                    cache, "get_step", side_effect=AssertionError("cache scan")
                ),
                patch.object(
                    summary_history.SummaryHistory,
                    "snapshot",
                    side_effect=AssertionError("summary reconstruction"),
                ),
            ):
                async with checkpoint.tracking():
                    for _ in range(100):
                        checkpoint.record("root", "stream")
                    await asyncio.wait_for(committed.wait(), 2)
                    committed.clear()
                    checkpoint.record("root", "stream")
                    await asyncio.wait_for(committed.wait(), 2)
                assert checkpoint._generation == checkpoint._persisted_generation
        assert not any(
            "workflow_heads" in statement or "FROM steps" in statement
            for statement in statements
        )
        current = await database.get_execution()
        assert current.checkpoint.heads == record.checkpoint.heads
        assert current.checkpoint.updated_at > record.checkpoint.updated_at
        assert current.updated_at > record.updated_at
        assert database._summary_runtime is not None
        assert (
            database._summary_runtime.working.snapshot
            == await database.summary_snapshot()
        )


async def test_activity_during_checkpoint_and_failed_write_are_not_acknowledged(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    await database.append_step(generation("stream", streaming=True))
    async with CachedHistory(database, flush_interval=60) as cache:
        checkpoint = ExecutionCheckpointer(
            database, cache, await database.get_execution()
        )
        entered, release = asyncio.Event(), asyncio.Event()
        original = database.checkpoint_execution

        async def delayed(
            identifier: str, *, status: Optional[execution_state.ExecutionStatus] = None
        ) -> None:
            entered.set()
            await release.wait()
            await original(identifier, status=status)

        checkpoint.record("root", "stream")
        with patch.object(database, "checkpoint_execution", side_effect=delayed):
            task = asyncio.create_task(checkpoint.persist())
            await entered.wait()
            await cache.update_step(generation("stream", streaming=True, outgoing=90))
            checkpoint.record("root", "stream")
            release.set()
            await task
        assert checkpoint._generation > checkpoint._persisted_generation
        before = await database.get_execution()
        with patch.object(
            database,
            "checkpoint_execution",
            new=AsyncMock(side_effect=RuntimeError("failed")),
        ):
            with pytest.raises(RuntimeError, match="failed"):
                await checkpoint.persist()
        assert await database.get_execution() == before
        assert checkpoint._generation > checkpoint._persisted_generation
        await checkpoint.persist()
        assert checkpoint._generation == checkpoint._persisted_generation
        assert (await database.get_step("stream")).metrics == generation(
            "stream", streaming=True, outgoing=90
        ).metrics


async def test_checkpoint_cancellation_settles_commit_and_activity(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    async with CachedHistory(database) as cache:
        checkpoint = ExecutionCheckpointer(
            database, cache, await database.get_execution()
        )
        entered, release = asyncio.Event(), asyncio.Event()
        original = database.checkpoint_execution

        async def delayed(
            identifier: str, *, status: Optional[execution_state.ExecutionStatus] = None
        ) -> None:
            await original(identifier, status=status)
            entered.set()
            await release.wait()

        checkpoint.record("root", "activity")
        with patch.object(database, "checkpoint_execution", side_effect=delayed):
            task = asyncio.create_task(checkpoint.persist("cancelled"))
            await entered.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert checkpoint._generation == checkpoint._persisted_generation
        assert (await database.get_execution()).status == "cancelled"


async def test_metadata_preserves_control_status_heads_and_terminal_envelope(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    await database.append_step(generation("first"))
    stale = await database.get_execution()
    await database.append_step(generation("second", previous="first"))
    await database.stop_execution()
    async with CachedHistory(database) as cache:
        checkpoint = ExecutionCheckpointer(database, cache, stale)
        await checkpoint.persist()
        current = await database.get_execution()
        assert current.status == "cancelled"
        assert current.checkpoint.heads == {"root": "second"}
        await database.save_execution(stale, replace_heads=True)
        await checkpoint.persist()
        assert (await database.get_execution()).checkpoint.heads == {"root": "first"}
        await database.append_step(
            models.WorkflowStep(
                id="complete",
                workflow_id="root",
                previous_step_id="first",
                operation=models.WorkflowCompleteOperation(),
                outcome=models.StepCompleted(),
            )
        )
        await checkpoint.persist("completed")
    reopened = SQLiteHistory(database.path)
    record = await reopened.get_execution()
    assert record.status == "completed"
    assert record.completion_step_id == "complete"
    with sqlite3.connect(database.path) as connection:
        raw = execution_state.ExecutionRecord.model_validate_json(
            connection.execute("SELECT payload FROM execution").fetchone()[0]
        )
    assert raw == record
    with pytest.raises(ValueError, match="identity"):
        await reopened.checkpoint_execution("wrong")


async def test_metadata_transaction_rollback_preserves_summary(tmp_path: Path) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    before = await database.summary_snapshot()

    def fail(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON summary_state BEGIN SELECT RAISE(ABORT, 'failed checkpoint'); END"
        )

    await database._invoke(fail)
    with pytest.raises(sqlite3.IntegrityError, match="failed checkpoint"):
        await database.checkpoint_execution("execution", status="failed")
    assert await database.summary_snapshot() == before
    assert database._summary_runtime is not None
    assert database._summary_runtime.working.snapshot == before
