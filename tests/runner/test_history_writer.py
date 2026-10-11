import asyncio
from pathlib import Path
import sqlite3
import threading

import pytest

from vectorops.runner import models as m
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.sqlite_writer import SQLiteWriter


def step(identifier: str) -> m.WorkflowStep:
    return m.WorkflowStep(
        id=identifier,
        workflow_id="root",
        operation=m.StepOperation(kind="activity", name=identifier),
        outcome=m.StepStreaming(),
        deferred_delivery=True,
    )


async def seed(database: SQLiteHistory) -> None:
    await database.save_workflow(m.Workflow(id="root", definition_ref="test:main"))
    for identifier in ("first", "anchor", "other"):
        await database.append_step(step(identifier))


async def test_persistent_connection_and_atomic_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    connections: list[sqlite3.Connection] = []
    threads: list[int] = []
    statements: list[str] = []
    original = database._connect

    def connect() -> sqlite3.Connection:
        threads.append(threading.get_ident())
        connection = original()
        connection.set_trace_callback(statements.append)
        connections.append(connection)
        return connection

    monkeypatch.setattr(database, "_connect", connect)
    async with database:
        await seed(database)
        statements.clear()
        updates = [
            step(identifier).model_copy(update={"outcome": m.StepStreaming(state=1)})
            for identifier in ("first", "anchor", "other")
        ]
        await database.update_steps(updates)
        assert statements.count("BEGIN IMMEDIATE") == 1
        assert statements.count("COMMIT") == 1
        assert (
            await asyncio.gather(*(database.get_step(item.id) for item in updates))
            == updates
        )
        invalid = updates[-1].model_copy(update={"previous_step_id": "first"})
        with pytest.raises(ValueError, match="identity"):
            await database.update_steps(
                [updates[0].model_copy(update={"outcome": m.StepCompleted()}), invalid]
            )
        assert await database.get_step("first") == updates[0]
    assert len(connections) == 2
    assert all(thread != threading.get_ident() for thread in threads)
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")
    with pytest.raises(RuntimeError, match="closed"):
        await database.get_step("first")


async def test_delivery_does_not_flush_unrelated_streams(tmp_path: Path) -> None:
    async with SQLiteHistory(tmp_path / "history.sqlite3") as database:
        await seed(database)
        await database.update_step(
            step("first").model_copy(update={"outcome": m.StepCompleted(result=1)})
        )
        async with CachedHistory(database, flush_interval=60) as history:
            for identifier in ("anchor", "other"):
                await history.update_step(
                    step(identifier).model_copy(
                        update={"outcome": m.StepStreaming(state=7)}
                    )
                )
            delivered = await history.record_delivery("anchor", "first")
            assert delivered.outcome == m.StepStreaming(state=7)
            assert delivered.deliveries == ["first"]
            assert (await database.get_step("other")).outcome == m.StepStreaming()
            assert (await history.get_step("other")).outcome == m.StepStreaming(state=7)
            await history.update_step(
                delivered.model_copy(update={"outcome": m.StepStreaming(state=8)})
            )
            await history.flush()
            restored = await database.get_step("anchor")
            assert restored.deliveries == ["first"]
            assert restored.outcome == m.StepStreaming(state=8)
            assert (await database.get_step("other")).outcome == m.StepStreaming(
                state=7
            )


async def test_failed_flush_retains_all_snapshots_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with SQLiteHistory(tmp_path / "history.sqlite3") as database:
        await seed(database)
        async with CachedHistory(database, flush_interval=60) as history:
            for identifier in ("first", "anchor"):
                await history.update_step(
                    step(identifier).model_copy(
                        update={"outcome": m.StepStreaming(state=9)}
                    )
                )
            original = database._step

            def fail(connection: sqlite3.Connection, identifier: str) -> m.WorkflowStep:
                if identifier == "anchor":
                    raise OSError("batch failed")
                return original(connection, identifier)

            with monkeypatch.context() as patcher:
                patcher.setattr(database, "_step", fail)
                with pytest.raises(OSError, match="batch failed"):
                    await history.flush()
            assert (await database.get_step("first")).outcome == m.StepStreaming()
            assert (await history.get_step("first")).outcome == m.StepStreaming(state=9)
            await history.flush()
            for identifier in ("first", "anchor"):
                assert (await database.get_step(identifier)).outcome == m.StepStreaming(
                    state=9
                )


async def test_queue_cancellation_and_close_drain_accepted_work(tmp_path: Path) -> None:
    started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    calls: list[int] = []
    threads: list[int] = []

    def connect() -> sqlite3.Connection:
        threads.append(threading.get_ident())
        return sqlite3.connect(tmp_path / "queue.sqlite3")

    def first(connection: sqlite3.Connection) -> None:
        threads.append(threading.get_ident())
        loop.call_soon_threadsafe(started.set)
        if not release.wait(5):
            raise TimeoutError("test did not release writer")
        connection.execute("CREATE TABLE records (value INTEGER)")
        calls.append(1)

    def second(connection: sqlite3.Connection) -> None:
        threads.append(threading.get_ident())
        with connection:
            connection.execute("INSERT INTO records VALUES (2)")
        calls.append(2)

    writer = SQLiteWriter(connect)
    first_task = asyncio.create_task(writer.run(first))
    try:
        await asyncio.wait_for(started.wait(), 5)
        second_task = asyncio.create_task(writer.run(second))
        await asyncio.sleep(0)
        second_task.cancel()
        await asyncio.sleep(0)
        second_task.cancel()
        closing = asyncio.create_task(writer.aclose())
        await asyncio.sleep(0)
        assert not first_task.done()
        assert not second_task.done()
        assert not closing.done()
        release.set()
        await first_task
        with pytest.raises(asyncio.CancelledError):
            await second_task
        await closing
        assert calls == [1, 2]
        assert len(set(threads)) == 1
        assert threads[0] != threading.get_ident()
        with pytest.raises(RuntimeError, match="closed"):
            await writer.run(second)
    finally:
        release.set()
        await writer.aclose()
