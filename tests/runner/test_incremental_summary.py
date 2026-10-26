import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3
from time import perf_counter
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed
from vectorops.runner import models
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import summary_history
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.summary_runtime import SummaryRuntime
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


def workspace(tmp_path: Path) -> SummaryStore:
    return SummaryStore(WorkspaceDatabase(tmp_path / "workspace.sqlite3"))


@pytest.mark.parametrize("retained", [10, 1000])
async def test_streaming_work_is_independent_of_retained_workflows(
    tmp_path: Path, retained: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    target = LocalExecutionTarget(root=str(tmp_path))
    await seed(database)
    for index in range(retained):
        await database.save_workflow(
            models.Workflow(id=f"child-{index}", definition_ref="test", target=target)
        )
    steps = [
        generation(f"stream-{index}", f"child-{index}", streaming=True)
        for index in range(4)
    ]
    for step in steps:
        await database.append_step(step)
    statements: list[str] = []
    original = database._connect

    def connect() -> sqlite3.Connection:
        connection = original()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(database, "_connect", connect)
    async with database:
        assert database._summary_runtime is not None
        database._summary_runtime.interval = 60
        await database.get_execution()
        statements.clear()
        started = perf_counter()
        with (
            patch.object(
                summary_history.SummaryHistory,
                "snapshot",
                side_effect=AssertionError("full summary reconstruction"),
            ),
            patch.object(
                summary_history.TargetReferences,
                "read",
                side_effect=AssertionError("target discovery"),
            ),
            patch.object(store, "record", wraps=store.record) as publish,
        ):
            for index in range(20):
                await database.update_steps(
                    [
                        generation(
                            step.id,
                            step.workflow_id,
                            streaming=True,
                            outgoing=20 + index,
                        )
                        for step in steps
                    ]
                )
            assert publish.call_count == 0
            await database._summary_runtime.flush()
            assert publish.call_count == 1
            database._summary_runtime.interval = 0
            if database._summary_runtime._timer is not None:
                database._summary_runtime._timer.cancel()
                await asyncio.gather(
                    database._summary_runtime._timer, return_exceptions=True
                )
                database._summary_runtime._timer = None
        elapsed = perf_counter() - started
        assert not any(
            'FROM "workflows"' in statement and "WHERE" not in statement
            for statement in statements
        )
        assert not any(
            "SELECT root, worktree_id FROM target_references" in statement
            for statement in statements
        )
        expected = await database.summary_snapshot()
        assert database._summary_runtime.working.snapshot == expected
        print(
            f"retained={retained}: 20 updates, old target rows examined={20 * retained}, new discovery queries=0, projection writes=1, seconds={elapsed:.4f}"
        )


async def test_reference_migration_retains_history_and_is_idempotent(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    targets = [
        LocalExecutionTarget(root=str(tmp_path / name))
        for name in ("root", "completed", "branch", "runtime")
    ]
    for index, target in enumerate(targets[:3]):
        await database.save_workflow(
            models.Workflow(id=f"old-{index}", definition_ref="test", target=target)
        )
    await database.reference_target(targets[3])
    with closing(sqlite3.connect(database.path)) as connection, connection:
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'references_%'"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute("DROP TABLE target_references")
    reopened = SQLiteHistory(database.path)
    assert set(await reopened.referenced_targets()) == set(targets)
    await reopened.save_workflow(
        models.Workflow(id="duplicate", definition_ref="test", target=targets[1])
    )
    assert set(await SQLiteHistory(database.path).referenced_targets()) == set(targets)
    with closing(sqlite3.connect(database.path)) as connection:
        assert (
            connection.execute("SELECT count(*) FROM target_references").fetchone()[0]
            == 4
        )


async def test_shared_owner_commits_controls_and_rolls_back_failed_batches(
    tmp_path: Path,
) -> None:
    store = workspace(tmp_path)
    owner = SummaryRuntime(store, None)
    path = tmp_path / "history.sqlite3"
    database = SQLiteHistory(path, summary_store=store, summary_runtime=owner)
    control = SQLiteHistory(path, summary_store=store, summary_runtime=owner)
    await seed(database)
    await database.append_step(generation("stream", streaming=True))
    async with database:
        await asyncio.gather(
            database.update_step(generation("stream", streaming=True, outgoing=50)),
            control.set_workflow_name("root", "Named"),
            control.reference_target(LocalExecutionTarget(root=str(tmp_path))),
        )
        await owner.flush()
        assert owner.working.snapshot == await database.summary_snapshot()
        previous = owner.working.snapshot.model_copy(deep=True)
        with pytest.raises(KeyError):
            await database.update_steps(
                [
                    generation("stream", streaming=True, outgoing=100),
                    generation("missing", streaming=True),
                ]
            )
        assert owner.working.snapshot == previous
        await control.stop_execution()
        assert (await store.get("execution")).status == "cancelled"
        assert owner.working.snapshot == await database.summary_snapshot()


async def test_shutdown_failure_recovery_and_late_publication(tmp_path: Path) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    await database.append_step(generation("stream", streaming=True))
    async with database:
        with patch.object(
            store, "record", new=AsyncMock(side_effect=RuntimeError("unavailable"))
        ):
            await database.update_step(
                generation("stream", streaming=True, outgoing=77)
            )
            assert database._summary_runtime is not None
            assert not await database._summary_runtime.flush()
    snapshot = await SQLiteHistory(database.path).summary_snapshot()
    assert (await store.get("execution")) == snapshot.summary
    await store.remove(["execution"])
    await store.record(snapshot)
    with pytest.raises(KeyError):
        await store.get("execution")


async def test_unchanged_target_projection_does_not_rewrite_rows(
    tmp_path: Path,
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    target = LocalExecutionTarget(root=str(tmp_path))
    await database.reference_target(target)

    def protect(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TRIGGER no_target_delete BEFORE DELETE ON execution_targets BEGIN SELECT RAISE(ABORT, 'unchanged references'); END"
        )
        connection.execute(
            "CREATE TRIGGER no_target_insert BEFORE INSERT ON execution_targets BEGIN SELECT RAISE(ABORT, 'unchanged references'); END"
        )

    await store.database.run(protect)
    await database.append_step(generation("stream", streaming=True))
    await database.update_step(generation("stream", streaming=True, outgoing=55))
    assert (await store.get("execution")) == (await database.summary_snapshot()).summary


async def test_shutdown_wakes_long_timer_and_external_changes_invalidate(
    tmp_path: Path,
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    await database.append_step(generation("stream", streaming=True))
    async with asyncio.timeout(2):
        async with database:
            assert database._summary_runtime is not None
            database._summary_runtime.interval = 60
            await SQLiteHistory(database.path).set_workflow_name("root", "External")
            await database.update_step(
                generation("stream", streaming=True, outgoing=88)
            )
            assert (
                database._summary_runtime.working.snapshot
                == await database.summary_snapshot()
            )
    assert (await store.get("execution")).name == "External"
    assert (await store.get("execution")) == (
        await SQLiteHistory(database.path).summary_snapshot()
    ).summary


@pytest.mark.parametrize("retained", [10, 1000])
async def test_target_query_vm_work_before_and_after(
    tmp_path: Path, retained: int
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    target = LocalExecutionTarget(root=str(tmp_path))
    with closing(sqlite3.connect(database.path)) as connection, connection:
        connection.executemany(
            "INSERT INTO workflows VALUES (?, ?)",
            [
                (
                    f"retained-{index}",
                    models.Workflow(
                        id=f"retained-{index}", definition_ref="test", target=target
                    ).model_dump_json(),
                )
                for index in range(retained)
            ],
        )
        operations = 0

        def progress() -> int:
            nonlocal operations
            operations += 1
            return 0

        connection.set_progress_handler(progress, 1)
        rows = connection.execute(
            "SELECT json_extract(payload, '$.target') FROM workflows WHERE json_type(payload, '$.target') = 'object'"
        ).fetchall()
        before = operations
        operations = 0
        references = summary_history.TargetReferences.read(connection)
        after = operations
        connection.set_progress_handler(None, 0)
    assert len(rows) == retained
    assert references == [target]
    assert before > retained * 4
    assert after < 30
    print(
        f"retained={retained}: target discovery SQLite VM instructions before={before}, durable reference read after={after}; active streaming reference reads=0"
    )
