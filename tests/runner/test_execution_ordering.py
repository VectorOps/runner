from pathlib import Path
from unittest.mock import patch

import pytest

from vectorops.runner import models
from vectorops.runner.runner import execution_state
from vectorops.runner.runner.execution_index import IndexState
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_execution_order_uses_updated_time_and_id_cursors(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    for identifier, day in (("a", 1), ("z", 3), ("b", 3), ("c", 2)):
        database = SQLiteHistory(
            project.data_path / "executions" / identifier / "history.sqlite3"
        )
        await database.save_workflow(
            models.Workflow(id="root", definition_ref="builtin:chat")
        )
        await database.save_execution(
            execution_state.ExecutionRecord(
                id=identifier,
                root_workflow_id="root",
                status="completed",
                updated_at=f"2025-01-0{day}T00:00:00+00:00",
            )
        )

    assert await runner.execution_ids(limit=None) == ["b", "z", "c", "a"]
    assert await runner.execution_ids(limit=2) == ["b", "z"]
    assert await runner.execution_ids(after="z", limit=2) == ["c", "a"]
    assert await runner.execution_ids(after="a") == []
    with pytest.raises(ValueError):
        await runner.execution_ids(after="missing")

    database = SQLiteHistory(project.data_path / "executions/a/history.sqlite3")
    record = await database.get_execution()
    await database.save_execution(
        record.model_copy(update={"updated_at": "2025-01-04T00:00:00+00:00"})
    )
    restarted = WorkflowRunner(tmp_path)
    assert await restarted.execution_ids() == ["b", "z", "c", "a"]
    index = await restarted.execution_index()
    snapshot = IndexState.model_validate_json(index.path.read_bytes())
    with patch(
        "vectorops.runner.runner.execution_index.time",
        return_value=snapshot.refreshed_at + 30,
    ):
        assert await restarted.execution_ids() == ["a", "b", "z", "c"]


async def test_empty_execution_list(tmp_path: Path) -> None:
    assert await WorkflowRunner(tmp_path).execution_ids() == []
