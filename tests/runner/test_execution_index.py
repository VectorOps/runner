import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from vectorops.runner import models
from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def seed(data_path: Path, identifier: str, day: int) -> SQLiteHistory:
    database = SQLiteHistory(data_path / "executions" / identifier / "history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="builtin:chat")
    )
    await database.save_execution(
        ExecutionRecord(
            id=identifier,
            root_workflow_id="root",
            status="completed",
            updated_at=f"2025-01-0{day}T00:00:00+00:00",
        )
    )
    return database


async def test_projection_is_incremental_and_external_changes_reconcile_on_restart(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    await seed(project.data_path, "old", 1)
    await seed(project.data_path, "new", 2)
    index = await runner.execution_index()
    assert await runner.execution_ids() == ["new", "old"]
    snapshot = await index.database.index_status()
    history = await runner._database(project, "old")
    record = await history.get_execution()
    await history.save_execution(
        record.model_copy(update={"updated_at": "2025-01-03T00:00:00+00:00"})
    )
    await seed(project.data_path, "latest", 4)
    restarted = WorkflowRunner(tmp_path)
    with (
        patch.object(Path, "iterdir", side_effect=AssertionError("unexpected scan")),
        patch.object(
            SQLiteHistory,
            "summary_snapshot",
            side_effect=AssertionError("unexpected read"),
        ),
    ):
        assert await runner.execution_ids() == ["old", "new"]
        assert (await index.database.index_status())[1] == snapshot[1]
        assert (await runner.execution_summary("old")).status == "completed"
    assert await restarted.execution_ids() == ["latest", "old", "new"]


@pytest.mark.parametrize("payload", ["not json", '{"schema_version": 2}', ""])
async def test_corrupt_cache_is_rebuilt(tmp_path: Path, payload: str) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    (index.data_path / "execution-index.json").write_text(payload)
    assert await runner.execution_ids() == ["execution"]
    assert await index.database.index_page(None, 100) == ["execution"]


async def test_concurrent_requests_share_refresh(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    with patch.object(index, "read_snapshot", wraps=index.read_snapshot) as read:
        results = await asyncio.gather(*(runner.execution_ids() for _ in range(10)))
    assert results == [["execution"]] * 10
    read.assert_awaited_once_with("execution")


async def test_removal_updates_projection_without_invalidating_index(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    assert await runner.execution_ids() == ["execution"]
    await runner.initialize_usage()
    with patch.object(index, "discover", side_effect=AssertionError("unexpected scan")):
        await runner.remove_execution("execution")
        assert await runner.execution_ids() == []
    assert await WorkflowRunner(tmp_path).execution_ids() == []


async def test_recovery_bypasses_fresh_index(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    assert await runner.execution_ids() == []
    index = await runner.execution_index()
    database = await seed(index.data_path, "running", 1)
    record = await database.get_execution()
    await database.save_execution(record.model_copy(update={"status": "running"}))
    assert await runner.execution_ids() == []
    assert [record.id for record in await runner.recovery_candidates()] == ["running"]


async def test_empty_active_page_does_not_refresh_archive(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    with patch.object(index, "discover", side_effect=AssertionError("unexpected scan")):
        assert await runner.execution_ids(active_only=True) == []
        with pytest.raises(ValueError, match="cursor"):
            await runner.execution_ids(after="missing", active_only=True)
