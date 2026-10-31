import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3
from time import perf_counter, process_time
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed
from vectorops.runner import models
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import summary_history
from vectorops.runner.runner import branching, execution_state, mailbox
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
        cpu_started = process_time()
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
            assert publish.call_count == 0
            database._summary_runtime.interval = 0
            if database._summary_runtime._timer is not None:
                database._summary_runtime._timer.cancel()
                await asyncio.gather(
                    database._summary_runtime._timer, return_exceptions=True
                )
                database._summary_runtime._timer = None
        elapsed = perf_counter() - started
        cpu_elapsed = process_time() - cpu_started
        assert not any(
            "TRIGGER" in statement.upper() or "summary_capture" in statement
            for statement in statements
        )
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
            f"retained={retained}: 20 updates, old target rows examined={20 * retained}, new discovery queries=0, projection writes=0, seconds={elapsed:.4f}, cpu_seconds={cpu_elapsed:.4f}, cpu_ms_per_persisted_step={cpu_elapsed * 1000 / 80:.4f}"
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
            await database.update_step(generation("stream", outgoing=77))
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
    for index in range(retained):
        await database.save_workflow(
            models.Workflow(
                id=f"retained-{index}", definition_ref="test", target=target
            )
        )
    with closing(sqlite3.connect(database.path)) as connection, connection:
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


async def test_reads_and_noops_do_not_process_summary_or_write_derived_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    step = generation("stream", streaming=True)
    await database.append_step(step)
    await database.set_workflow_name("root", "Named")
    record = await database.get_execution()
    await database.save_execution(record)
    before = await database.summary_snapshot()
    statements: list[str] = []
    original = database._connect

    def connect() -> sqlite3.Connection:
        connection = original()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(database, "_connect", connect)
    with patch.object(
        summary_history.WorkingSummary,
        "base",
        side_effect=AssertionError("read processed summary"),
    ):
        await database.get_execution()
        await database.get_workflow("root")
        await database.get_step("stream")
        await database.referenced_targets()
        await database.usage_snapshot()
        await database.list_inputs(mailbox.RequestQuery(execution_id="execution"))
    assert not any(
        statement.lstrip()
        .upper()
        .startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP"))
        for statement in statements
    )
    statements.clear()
    with patch.object(store, "record", new=AsyncMock()) as publish:
        await database.update_step(step)
        await database.save_execution(record)
        await database.save_workflow(await database.get_workflow("root"))
        await database.set_workflow_name("root", "Ignored")
        await database.cancel_inputs("missing")
        publish.assert_not_awaited()
    assert await database.summary_snapshot() == before
    assert not any(
        statement.lstrip()
        .upper()
        .startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP"))
        and any(
            table in statement
            for table in ("summary_state", "target_references", "usage_state")
        )
        for statement in statements
    )


async def test_migration_removes_triggers_preserves_identity_and_reference_union(
    tmp_path: Path,
) -> None:
    path = tmp_path / "history.sqlite3"
    database = SQLiteHistory(path)
    await seed(database)
    historical = LocalExecutionTarget(root=str(tmp_path / "historical"))
    await database.reference_target(historical)
    before = await database.summary_snapshot()
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            "CREATE TRIGGER summary_execution_update AFTER UPDATE ON execution BEGIN UPDATE summary_state SET revision = revision + 1; END"
        )
        connection.execute(
            "CREATE TRIGGER references_workflows_insert AFTER INSERT ON workflows BEGIN SELECT 1; END"
        )
        connection.execute(
            "UPDATE execution SET payload = json_set(payload, '$.referenced_targets', json('[]'))"
        )
        revision = connection.execute("SELECT revision FROM summary_state").fetchone()[
            0
        ]
    reopened = SQLiteHistory(path)
    migrated = await reopened.summary_snapshot()
    assert migrated.source_id == before.source_id
    assert migrated.revision == revision
    assert migrated.targets == [historical]
    assert await SQLiteHistory(path).summary_snapshot() == migrated
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
            == []
        )
    await reopened.append_step(generation("one", streaming=True))
    first = await reopened.summary_snapshot()
    assert first.revision == migrated.revision + 1
    await reopened.update_step(generation("one", streaming=True, outgoing=99))
    assert (await reopened.summary_snapshot()).revision == first.revision + 1


async def test_branch_and_input_deltas_match_durable_state(tmp_path: Path) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    await database.append_step(generation("first"))
    await database.append_step(generation("second", previous="first"))
    await database.append_step(
        models.WorkflowStep(
            id="input",
            workflow_id="root",
            previous_step_id="second",
            operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    async with database:
        assert database._summary_runtime is not None
        owner = database._summary_runtime
        await database.register_input("input", mailbox.InputSpec(channel="approval"))
        assert (await store.get("execution")).pending_inputs == 1
        await database.set_paused({"input"})
        assert (await store.get("execution")).status == "paused"
        await database.answer_input(
            mailbox.Answer(
                execution_id="execution",
                request_id="input",
                response="yes",
                idempotency_key="answer",
            )
        )
        assert owner.working.snapshot == await database.summary_snapshot()
        await database.accept_branch(
            branching.Branch(
                execution_id="execution",
                step_id="input",
                idempotency_key="branch",
                action=branching.InsertInput(response="edited"),
            )
        )
        await database.apply_branch()
        assert owner.working.snapshot == await database.summary_snapshot()
        record = await database.get_execution()
        await database.save_execution(
            record.model_copy(
                update={
                    "checkpoint": execution_state.ExecutionCheckpoint(
                        heads={"root": "first"}
                    )
                }
            ),
            replace_heads=True,
        )
        assert owner.working.snapshot == await database.summary_snapshot()


async def test_normal_updates_do_not_starve_and_urgent_status_bypasses_interval(
    tmp_path: Path,
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    await database.append_step(generation("stream", streaming=True))
    async with database:
        assert database._summary_runtime is not None
        owner = database._summary_runtime
        owner.interval = 0.04
        owner._next_publication = 0
        times: list[float] = []
        original = store.record

        async def record(snapshot: summary_history.Snapshot) -> None:
            times.append(asyncio.get_running_loop().time())
            await original(snapshot)

        with patch.object(store, "record", side_effect=record):
            await database.set_workflow_name("root", "Normal metadata")
            assert not times
            for index in range(25):
                await database.append_step(generation(f"measured-{index}"))
                await asyncio.sleep(0.008)
            assert len(times) >= 2
            assert all(
                right - left >= owner.interval * 0.9
                for left, right in zip(times, times[1:])
            )
            owner.interval = 60
            await owner.flush()
            owner._next_publication = asyncio.get_running_loop().time() + 60
            before = len(times)
            await database.update_step(generation("stream", outgoing=100))
            assert len(times) == before
            await database.stop_execution()
            assert len(times) == before + 1
            assert (await store.get("execution")).status == "cancelled"


async def test_flush_retains_newer_changes_and_failure_does_not_spin(
    tmp_path: Path,
) -> None:
    store = workspace(tmp_path)
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    snapshot = await database.summary_snapshot()
    owner = SummaryRuntime(store, None)
    owner.active = True
    owner.interval = 0.02
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.record
    published: list[int] = []

    async def hold(value: summary_history.Snapshot) -> None:
        entered.set()
        await release.wait()
        await original(value)
        published.append(value.revision)

    newer = snapshot.model_copy(
        update={
            "revision": snapshot.revision + 1,
            "summary": snapshot.summary.model_copy(update={"name": "New"}),
        }
    )
    with patch.object(store, "record", side_effect=hold):
        await owner.changed(snapshot, (), urgency=summary_history.Urgency.NORMAL)
        await asyncio.wait_for(entered.wait(), 2)
        await owner.changed(newer, ("root",), urgency=summary_history.Urgency.NORMAL)
        release.set()
        await owner.close()
    assert published == [snapshot.revision, newer.revision]
    owner.active = True
    with patch.object(
        store, "record", new=AsyncMock(side_effect=RuntimeError("offline"))
    ) as publish:
        await owner.changed(
            snapshot.model_copy(update={"revision": newer.revision + 1}),
            (),
            urgency=summary_history.Urgency.NORMAL,
        )
        await asyncio.sleep(0.12)
        assert publish.await_count == 1
    await owner.close()
