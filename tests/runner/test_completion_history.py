from pathlib import Path

import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.sqlite_history import SQLiteHistory


@pytest.mark.parametrize("sqlite", [False, True])
async def test_completion_snapshot_uses_current_heads_and_commit_order(
    tmp_path: Path, sqlite: bool
) -> None:
    backend = SQLiteHistory(tmp_path / "history.sqlite3") if sqlite else MemoryHistory()
    async with CachedHistory(backend) as history:
        for identifier in ("a", "b", "c"):
            await history.save_workflow(
                models.Workflow(id=identifier, definition_ref="test:child")
            )
        assert await history.first_completion(["a", "b"]) is None
        for identifier in ("b", "a", "c"):
            await history.append_step(
                models.WorkflowStep(
                    id="done-" + identifier,
                    workflow_id=identifier,
                    operation=models.WorkflowCompleteOperation(),
                    outcome=models.StepCompleted(result=identifier),
                )
            )
        first = await history.first_completion(["a", "b"])
        assert first is not None and first.id == "done-b"
        assert await history.first_completion([]) is None
        await history.append_step(
            models.WorkflowStep(
                id="new-b",
                workflow_id="b",
                operation=models.StepOperation(kind="activity", name="branch"),
                outcome=models.StepPending(),
                deferred_delivery=True,
            )
        )
        first = await history.first_completion(["b", "a"])
        assert first is not None and first.id == "done-a"
    if sqlite:
        reopened = SQLiteHistory(tmp_path / "history.sqlite3")
        first = await reopened.first_completion(["c", "a", "b"])
        assert first is not None and first.id == "done-a"
