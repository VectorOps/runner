import asyncio
from contextlib import aclosing, closing
from pathlib import Path
import sqlite3
import threading
import typing

import pytest

from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models as m
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture
def database(tmp_path: Path) -> SQLiteHistory:
    return SQLiteHistory(tmp_path / "state" / "history.sqlite3", batch_size=2)


def step(
    identifier: str,
    *,
    workflow_id: str = "w",
    previous: typing.Optional[str] = None,
    outcome: typing.Optional[m.StepOutcome] = None,
) -> m.WorkflowStep:
    return m.WorkflowStep(
        id=identifier,
        workflow_id=workflow_id,
        previous_step_id=previous,
        operation=m.StepOperation(kind="activity", name="work"),
        outcome=m.StepCompleted(result=identifier) if outcome is None else outcome,
    )


async def seed(database: SQLiteHistory) -> None:
    await database.save_workflow(
        m.Workflow(id="w", definition_ref="activities:workflow")
    )


async def test_configuration_and_steps_survive_new_provider(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    await database.append_step(step("s"))
    reopened = SQLiteHistory(database.path)
    assert await reopened.get_workflow("w") == m.Workflow(
        id="w", definition_ref="activities:workflow"
    )
    assert await reopened.get_step("s") == step("s")
    assert database.path.is_file()


async def test_workflow_save_is_idempotent_and_rejects_conflicts(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    await seed(database)
    with pytest.raises(ValueError, match="conflict"):
        await database.save_workflow(
            m.Workflow(id="w", definition_ref="other:workflow")
        )
    assert (await database.get_workflow("w")).definition_ref == "activities:workflow"


async def test_missing_records_and_duplicate_steps(database: SQLiteHistory) -> None:
    with pytest.raises(KeyError):
        await database.get_workflow("missing")
    with pytest.raises(KeyError):
        await database.get_step("missing")
    with pytest.raises(KeyError):
        await database.append_step(step("s"))
    await seed(database)
    await database.append_step(step("s"))
    with pytest.raises(ValueError, match="duplicate"):
        await database.append_step(step("s"))
    with pytest.raises(KeyError):
        await database.update_step(step("missing"))


async def test_predecessor_validation(database: SQLiteHistory) -> None:
    await seed(database)
    await database.save_workflow(
        m.Workflow(id="other", definition_ref="activities:workflow")
    )
    await database.append_step(step("foreign", workflow_id="other"))
    with pytest.raises(KeyError):
        await database.append_step(step("missing", previous="unknown"))
    with pytest.raises(ValueError, match="another workflow"):
        await database.append_step(step("s", previous="foreign"))
    await database.append_step(step("active", outcome=m.StepStreaming()))
    with pytest.raises(ValueError, match="unfinished"):
        await database.append_step(step("s", previous="active"))


async def test_stream_updates_rewrite_single_row(database: SQLiteHistory) -> None:
    await seed(database)
    active = step("stream", outcome=m.StepStreaming(state={"version": 0}))
    await database.append_step(active)
    for version in range(20):
        await database.update_step(
            active.model_copy(
                update={"outcome": m.StepStreaming(state={"version": version})}
            )
        )
    assert (
        await SQLiteHistory(database.path).get_step("stream")
    ).outcome == m.StepStreaming(state={"version": 19})
    completed = active.model_copy(update={"outcome": m.StepCompleted(result="done")})
    await database.update_step(completed)
    assert await database.get_step("stream") == completed
    with pytest.raises(ValueError, match="terminal"):
        await database.update_step(active)

    def count() -> int:
        with closing(sqlite3.connect(database.path)) as connection:
            return typing.cast(
                int, connection.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
            )

    assert await asyncio.to_thread(count) == 1


@pytest.mark.parametrize("change", ["workflow", "predecessor", "operation"])
async def test_stream_identity_and_links_are_immutable(
    database: SQLiteHistory, change: str
) -> None:
    await seed(database)
    active = step("stream", outcome=m.StepStreaming())
    await database.append_step(active)
    updates: dict[str, object] = {
        "workflow": {"workflow_id": "other"},
        "predecessor": {"previous_step_id": "other"},
        "operation": {"operation": m.StepOperation(kind="activity", name="different")},
    }
    with pytest.raises(ValueError, match="identity"):
        await database.update_step(
            active.model_copy(
                update=typing.cast(dict[str, typing.Any], updates[change])
            )
        )
    assert await database.get_step(active.id) == active


async def test_failed_stream_becomes_immutable(database: SQLiteHistory) -> None:
    await seed(database)
    active = step("s", outcome=m.StepStreaming())
    await database.append_step(active)
    failed = active.model_copy(
        update={
            "outcome": m.StepFailed(error=m.StepError(type="Error", message="failed"))
        }
    )
    await database.update_step(failed)
    with pytest.raises(ValueError, match="terminal"):
        await database.update_step(active)


async def test_replay_only_selected_branch_in_root_order(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    for record in (
        step("a"),
        step("b", previous="a"),
        step("old", previous="b"),
        step("fork", previous="b"),
    ):
        await database.append_step(record)
    assert [record.id async for record in database.replay_path("w", "fork")] == [
        "a",
        "b",
        "fork",
    ]
    assert [record.id async for record in database.replay_path("w", "old")] == [
        "a",
        "b",
        "old",
    ]
    with pytest.raises(KeyError):
        _ = [record async for record in database.replay_path("w", "missing")]
    await database.save_workflow(m.Workflow(id="other", definition_ref="file:workflow"))
    with pytest.raises(ValueError, match="another workflow"):
        _ = [record async for record in database.replay_path("other", "fork")]


async def test_replay_snapshot_allows_updates_during_iteration(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    await database.append_step(step("a"))
    active = step("b", previous="a", outcome=m.StepStreaming(state="old"))
    await database.append_step(active)
    async with aclosing(database.replay_path("w", "b")) as path:
        assert (await anext(path)).id == "a"
        await SQLiteHistory(database.path).update_step(
            active.model_copy(update={"outcome": m.StepCompleted(result="new")})
        )
        assert (await anext(path)).outcome == m.StepStreaming(state="old")
    assert (await database.get_step("b")).outcome == m.StepCompleted(result="new")


async def test_early_close_releases_replay_connection(database: SQLiteHistory) -> None:
    await seed(database)
    await database.append_step(step("a"))
    async with aclosing(database.replay_path("w", "a")) as path:
        await anext(path)
    await database.append_step(step("b", previous="a"))
    assert (await database.get_step("b")).id == "b"


async def test_concurrent_providers_write_distinct_branches(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    await database.append_step(step("root"))
    providers = [database, SQLiteHistory(database.path)]
    await asyncio.gather(
        *(
            providers[index % 2].append_step(step(f"child-{index}", previous="root"))
            for index in range(20)
        )
    )
    assert [record.id async for record in database.replay_path("w", "child-19")] == [
        "root",
        "child-19",
    ]


async def test_competing_terminal_updates_are_serialized(
    database: SQLiteHistory,
) -> None:
    await seed(database)
    active = step("s", outcome=m.StepStreaming())
    await database.append_step(active)
    results = await asyncio.gather(
        database.update_step(
            active.model_copy(update={"outcome": m.StepCompleted(result=1)})
        ),
        SQLiteHistory(database.path).update_step(
            active.model_copy(update={"outcome": m.StepCompleted(result=2)})
        ),
        return_exceptions=True,
    )
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1


async def test_database_operations_run_off_event_loop(
    database: SQLiteHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    main_thread = threading.get_ident()
    original = database._connect
    threads: list[int] = []

    def connect() -> sqlite3.Connection:
        threads.append(threading.get_ident())
        return original()

    monkeypatch.setattr(database, "_connect", connect)
    await seed(database)
    await database.append_step(step("a"))
    _ = [record async for record in database.replay_path("w", "a")]
    assert threads and all(thread != main_thread for thread in threads)


async def test_large_history_replays_across_batches(database: SQLiteHistory) -> None:
    await seed(database)
    previous = None
    for index in range(150):
        await database.append_step(step(str(index), previous=previous))
        previous = str(index)
    assert [record.id async for record in database.replay_path("w", "149")] == [
        str(index) for index in range(150)
    ]


def test_invalid_batch_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SQLiteHistory(tmp_path / "history.sqlite3", batch_size=0)


async def test_concurrent_initialization_is_safe(tmp_path: Path) -> None:
    path = tmp_path / "shared.sqlite3"
    providers = [SQLiteHistory(path) for _ in range(4)]
    workflow = m.Workflow(id="w", definition_ref="file:workflow")
    await asyncio.gather(*(provider.save_workflow(workflow) for provider in providers))
    assert await providers[0].get_workflow("w") == workflow


async def test_unknown_schema_version_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite3"

    def create() -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("PRAGMA user_version = 99")

    await asyncio.to_thread(create)
    with pytest.raises(ValueError, match="schema version"):
        await SQLiteHistory(path).get_workflow("w")


async def test_cancelled_read_waits_for_database_work_before_cleanup(
    database: SQLiteHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    await seed(database)
    started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = database._workflow

    def blocked(connection: sqlite3.Connection, workflow_id: str) -> m.Workflow:
        loop.call_soon_threadsafe(started.set)
        if not release.wait(timeout=2):
            raise TimeoutError("test database worker was not released")
        return original(connection, workflow_id)

    monkeypatch.setattr(database, "_workflow", blocked)
    task = asyncio.create_task(database.get_workflow("w"))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    try:
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert await database.get_workflow("w") == m.Workflow(
        id="w", definition_ref="activities:workflow"
    )


async def test_default_database_follows_configured_data_directory(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    (workspace / "config.toml").write_text('data_directory = "state"\n')
    runner = WorkflowRunner(workspace)
    assert await runner.run("activities") == 2
    manifest = await runner.project_state()
    assert (
        workspace
        / "state/executions"
        / str(manifest.last_execution_id)
        / "history.sqlite3"
    ).is_file()
    assert not (workspace / "data/history.sqlite3").exists()


async def test_default_runner_persists_and_replays_after_restart(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run("activities", workflow_id="root") == 2
    record = await runner.execution_state(str(runner.last_execution_id))
    database = SQLiteHistory(
        workspace / "data/executions" / record.id / "history.sqlite3"
    )

    def head() -> str:
        with closing(sqlite3.connect(database.path)) as connection:
            return typing.cast(
                str,
                connection.execute(
                    "SELECT id FROM steps WHERE workflow_id = ? AND json_extract(payload, '$.operation.kind') = 'workflow.complete'",
                    ("root",),
                ).fetchone()[0],
            )

    completion_id = await asyncio.to_thread(head)
    assert (
        await WorkflowRunner(workspace).run(
            "activities", continue_from_step_id=completion_id, execution_id=record.id
        )
        == 2
    )
    assert execution_probe.calls == [0, 1]


async def test_stream_cancellation_and_restart_persist_latest_snapshot(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("streaming", input={"mode": "blocked"}, workflow_id="root")
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    record = await runner.execution_state(str(runner.last_execution_id))
    database = SQLiteHistory(
        workspace / "data/executions" / record.id / "history.sqlite3"
    )

    def head() -> str:
        with closing(sqlite3.connect(database.path)) as connection:
            return typing.cast(
                str,
                connection.execute(
                    "SELECT id FROM steps WHERE workflow_id = ?", ("root",)
                ).fetchone()[0],
            )

    stream_id = await asyncio.to_thread(head)
    assert (await database.get_step(stream_id)).outcome == m.StepStreaming(
        state={"index": 0}
    )
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).run(
        "streaming", continue_from_step_id=stream_id, execution_id=record.id
    ) == {"count": 2}
    assert (await database.get_step(stream_id)).outcome == m.StepCompleted(
        result={"count": 2}
    )


async def test_nested_children_persist_and_replay(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run("children", "deep_parent", workflow_id="root") == 4
    record = await runner.execution_state(str(runner.last_execution_id))
    database = SQLiteHistory(
        workspace / "data/executions" / record.id / "history.sqlite3"
    )

    def completion() -> str:
        with closing(sqlite3.connect(database.path)) as connection:
            return typing.cast(
                str,
                connection.execute(
                    "SELECT id FROM steps WHERE workflow_id = ? AND json_extract(payload, '$.operation.kind') = 'workflow.complete'",
                    ("root",),
                ).fetchone()[0],
            )

    assert (
        await WorkflowRunner(workspace).run(
            "children",
            "deep_parent",
            continue_from_step_id=await asyncio.to_thread(completion),
            execution_id=record.id,
        )
        == 4
    )
    assert execution_probe.calls == [1, 2]
