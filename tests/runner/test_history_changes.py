import asyncio
from collections.abc import Callable
from pathlib import Path
import sqlite3

import pytest

from tests.runner.test_usage import generation, seed
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def test_changes_only_follow_committed_writes(tmp_path: Path) -> None:
    changes: list[tuple[str, ...]] = []
    database = SQLiteHistory(tmp_path / "history.sqlite3", change_sink=changes.append)
    await seed(database)
    changes.clear()
    await database.set_workflow_name("root", "Committed")
    assert changes == [("root",)]
    assert (await SQLiteHistory(database.path).get_workflow("root")).name == "Committed"
    changes.clear()
    await database.set_workflow_name("root", "Ignored")
    await database.save_workflow(await database.get_workflow("root"))
    assert not changes
    step = generation("pending", streaming=True)
    await database.append_step(step)
    changes.clear()
    with pytest.raises(KeyError):
        await database.update_steps((generation("pending"), generation("missing")))
    assert not changes
    assert (await database.get_step("pending")).outcome.kind == "streaming"


async def test_commit_notification_survives_caller_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    changes: list[tuple[str, ...]] = []
    database = SQLiteHistory(tmp_path / "history.sqlite3", change_sink=changes.append)
    await seed(database)
    changes.clear()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = database._accounted

    async def hold[T](
        operation: Callable[[sqlite3.Connection], T], *, account: bool
    ) -> T:
        result = await original(operation, account=account)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(database, "_accounted", hold)
    task = asyncio.create_task(database.set_workflow_name("root", "Committed"))
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert changes == [("root",)]
