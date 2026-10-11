import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from vectorops.runner import models
from vectorops.runner.runner.execution_index import ExecutionIndex, IndexState
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


async def test_cache_is_persisted_reused_and_refreshed_after_30_seconds(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    await seed(project.data_path, "old", 1)
    await seed(project.data_path, "new", 2)
    index = await runner.execution_index()
    with patch("vectorops.runner.runner.execution_index.time", return_value=100):
        assert await runner.execution_ids() == ["new", "old"]
    payload = index.path.read_bytes()
    snapshot = IndexState.model_validate_json(payload)
    assert snapshot.refreshed_at == 100
    assert snapshot.executions[0].updated_at == "2025-01-02T00:00:00+00:00"
    await seed(project.data_path, "old", 3)
    await seed(project.data_path, "latest", 4)
    restarted = WorkflowRunner(tmp_path)
    with (
        patch("vectorops.runner.runner.execution_index.time", return_value=129.9),
        patch.object(Path, "iterdir", side_effect=AssertionError("unexpected scan")),
        patch.object(
            SQLiteHistory,
            "get_execution",
            side_effect=AssertionError("unexpected read"),
        ),
    ):
        assert await runner.execution_ids() == ["new", "old"]
        assert await restarted.execution_ids() == ["new", "old"]
        assert index.path.read_bytes() == payload
    with patch("vectorops.runner.runner.execution_index.time", return_value=130):
        assert await restarted.execution_ids() == ["latest", "old", "new"]
    assert IndexState.model_validate_json(index.path.read_bytes()).refreshed_at == 130


@pytest.mark.parametrize("payload", ["not json", '{"schema_version": 2}', ""])
async def test_corrupt_cache_is_rebuilt(tmp_path: Path, payload: str) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    index.path.write_text(payload)
    assert await runner.execution_ids() == ["execution"]
    assert IndexState.model_validate_json(index.path.read_bytes()).executions


async def test_concurrent_requests_share_refresh(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    with patch.object(index, "read_record", wraps=index.read_record) as read:
        results = await asyncio.gather(*(runner.execution_ids() for _ in range(10)))
    assert results == [["execution"]] * 10
    read.assert_awaited_once_with("execution")


async def test_removal_invalidates_cache(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    index = await runner.execution_index()
    await seed(index.data_path, "execution", 1)
    assert await runner.execution_ids() == ["execution"]
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


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_invalid_interval(tmp_path: Path, interval: float) -> None:
    with pytest.raises(ValueError):
        ExecutionIndex(tmp_path, AsyncMock(), refresh_interval=interval)
