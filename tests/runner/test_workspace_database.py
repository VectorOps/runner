import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3
import threading
from unittest.mock import Mock, call, patch

import pytest

from vectorops.runner.control import protocol
from vectorops.runner.control.request_store import RequestStore
from vectorops.runner.runner import state
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


async def test_workspace_preserves_receipts_and_pointer(tmp_path: Path) -> None:
    accepted = protocol.Accepted(execution_id="execution")
    async with RunnerStateStore(tmp_path) as store:
        await store.activate("execution")
        requests = RequestStore(store.database)
        await requests.save("request", "fingerprint", accepted)
        assert await requests.lookup("request", "fingerprint") == accepted
        with pytest.raises(protocol.RemoteError, match="different arguments"):
            await requests.lookup("request", "other")
    assert (await RunnerStateStore(tmp_path).load()).last_execution_id == "execution"
    assert (
        await RequestStore(WorkspaceDatabase(tmp_path)).lookup("request", "fingerprint")
        == accepted
    )


async def test_shared_writer_is_serialized_and_off_event_loop(tmp_path: Path) -> None:
    threads: set[int] = set()
    connections: set[int] = set()
    main = threading.get_ident()

    def inspect(connection: sqlite3.Connection) -> None:
        threads.add(threading.get_ident())
        connections.add(id(connection))

    async with WorkspaceDatabase(tmp_path) as database:
        await asyncio.gather(*(database.run(inspect) for _ in range(100)))
        await database.save_document("counter", "0")
        await asyncio.gather(
            *(
                database.update_document(
                    "counter", lambda: "0", lambda value: str(int(value) + 1)
                )
                for _ in range(100)
            )
        )
        assert await database.load_document("counter", lambda: "") == "100"
    assert len(threads) == len(connections) == 1
    assert main not in threads


async def test_independent_connections_update_atomically(tmp_path: Path) -> None:
    first, second = WorkspaceDatabase(tmp_path), WorkspaceDatabase(tmp_path)
    async with first, second:
        await first.save_document("counter", "0")
        await asyncio.gather(
            *(
                database.update_document(
                    "counter", lambda: "0", lambda value: str(int(value) + 1)
                )
                for _ in range(30)
                for database in (first, second)
            )
        )
        assert await second.load_document("counter", lambda: "") == "60"


async def test_cancelled_write_settles_before_close(tmp_path: Path) -> None:
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()

    def write(connection: sqlite3.Connection) -> None:
        with connection:
            loop.call_soon_threadsafe(started.set)
            if not release.wait(5):
                raise TimeoutError("worker not released")
            WorkspaceDatabase.put_document(connection, "value", "committed")

    async with WorkspaceDatabase(tmp_path) as database:
        task = asyncio.create_task(database.run(write))
        try:
            await asyncio.wait_for(started.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert (
        await WorkspaceDatabase(tmp_path).load_document("value", lambda: "")
        == "committed"
    )


async def test_failed_update_rolls_back_and_writer_remains_usable(
    tmp_path: Path,
) -> None:
    def fail(value: str) -> str:
        raise ValueError("failed")

    async with WorkspaceDatabase(tmp_path) as database:
        await database.save_document("value", "old")
        with pytest.raises(ValueError, match="failed"):
            await database.update_document("value", lambda: "", fail)
        assert await database.load_document("value", lambda: "") == "old"
        await database.save_document("value", "new")
        assert await database.load_document("value", lambda: "") == "new"


async def test_index_generation_and_sql_pagination(tmp_path: Path) -> None:
    async with WorkspaceDatabase(tmp_path) as database:
        generation, refreshed = await database.index_status()
        assert refreshed is None
        await database.invalidate_index()
        assert not await database.replace_index(generation, 100, [("stale", "1")])
        generation, _ = await database.index_status()
        assert await database.replace_index(
            generation, 101, [("b", "2"), ("a", "2"), ("c", "1")]
        )
        assert await database.index_page(None, 1) == ["a"]
        assert await database.index_page("a", 1) == ["b"]
        assert await database.index_page("b", None) == ["c"]
        assert await database.index_page("a", -1) == ["b", "c"]
        assert await database.index_page("a", 0) == []
        with pytest.raises(ValueError, match="cursor"):
            await database.index_page("missing", 1)
        with pytest.raises(ValueError, match="cursor"):
            await database.index_page("missing", 0)


async def test_index_pages_merge_active_without_duplicates(tmp_path: Path) -> None:
    async with WorkspaceDatabase(tmp_path) as database:
        generation, _ = await database.index_status()
        assert await database.replace_index(
            generation, 100, [("b", "2"), ("a", "2"), ("c", "1")]
        )
        active = ("a", "starting-old", "c", "starting-new")
        expected = ["starting-new", "starting-old", "a", "b", "c"]
        assert await database.index_page(None, None, active=active) == expected
        for index, identifier in enumerate(expected):
            after = None if index == 0 else expected[index - 1]
            assert await database.index_page(after, 1, active=active) == [identifier]
        assert await database.index_page("c", 1, active=active) == []
        assert await database.index_page(None, 4, active=active) == expected[:4]
        assert (
            await database.index_page("starting-new", 3, active=active) == expected[1:4]
        )
        assert await database.index_page(
            None, None, active=active, active_only=True
        ) == ["starting-new", "starting-old", "a", "c"]
        assert await database.index_page(
            "starting-old", 1, active=active, active_only=True
        ) == ["a"]
        assert await database.index_page("a", 2, active=active, active_only=True) == [
            "c"
        ]
        assert await database.index_page(None, 1, active_only=True) == []
        for after in ("missing", "b"):
            with pytest.raises(ValueError, match="cursor"):
                await database.index_page(after, 1, active=active, active_only=True)


@pytest.mark.parametrize("same_timestamp", [False, True])
async def test_deep_index_page_seeks_without_scanning_archive(
    tmp_path: Path, same_timestamp: bool
) -> None:
    operations = 0

    def progress() -> int:
        nonlocal operations
        operations += 1
        return 0

    async with WorkspaceDatabase(tmp_path) as database:
        generation, _ = await database.index_status()
        assert await database.replace_index(
            generation,
            100,
            [
                (f"e{index:05d}", "1" if same_timestamp else f"{10000 - index:05d}")
                for index in range(10000)
            ],
        )
        await database.run(
            lambda connection: connection.set_progress_handler(progress, 1)
        )
        try:
            assert await database.index_page("e09000", 2) == ["e09001", "e09002"]
            assert operations < 3000
            operations = 0
            assert await database.index_page(
                "e09000",
                2,
                active=("e09002", "e09001", "e09000"),
                active_only=True,
            ) == ["e09001", "e09002"]
        finally:
            await database.run(
                lambda connection: connection.set_progress_handler(None, 0)
            )
        assert operations < 3000


async def test_unknown_workspace_schema_is_rejected(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / "workspace.sqlite3")) as connection:
        connection.execute("PRAGMA user_version = 99")
    with pytest.raises(ValueError, match="schema version"):
        await WorkspaceDatabase(tmp_path).index_status()


async def test_worktree_schema_migrates_existing_workspace(tmp_path: Path) -> None:
    database = WorkspaceDatabase(tmp_path)
    await database.save_document("existing", "preserved")
    with closing(sqlite3.connect(database.path)) as connection:
        connection.execute("DROP TABLE worktrees")
        connection.execute("DROP TABLE worktree_uses")
        connection.execute("PRAGMA user_version = 2")
    reopened = WorkspaceDatabase(tmp_path)
    assert await reopened.load_document("existing", lambda: "lost") == "preserved"
    assert (
        await reopened.run(
            lambda connection: connection.execute("PRAGMA user_version").fetchone()[0]
        )
        == 3
    )
    assert (
        await reopened.run(
            lambda connection: connection.execute(
                "SELECT count(*) FROM worktrees"
            ).fetchone()[0]
        )
        == 0
    )


async def test_concurrent_first_open(tmp_path: Path) -> None:
    databases = [WorkspaceDatabase(tmp_path) for _ in range(8)]
    assert (
        await asyncio.gather(*(database.index_status() for database in databases))
        == [(0, None)] * 8
    )


@pytest.mark.parametrize("code", [sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED])
def test_journal_mode_retries_contention(tmp_path: Path, code: int) -> None:
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = code
    connection = Mock(spec=sqlite3.Connection)
    cursor = Mock(spec=sqlite3.Cursor)
    cursor.fetchone.return_value = (3,)
    connection.execute.side_effect = [cursor, error, cursor, cursor, cursor]
    with (
        patch(
            "vectorops.runner.runner.workspace_database.sqlite3.connect",
            return_value=connection,
        ),
        patch("vectorops.runner.runner.workspace_database.time.sleep") as sleep,
    ):
        assert WorkspaceDatabase(tmp_path)._connect() is connection
    assert (
        connection.execute.call_args_list[1:3]
        == [call("PRAGMA journal_mode = WAL")] * 2
    )
    sleep.assert_called_once_with(0.01)
    connection.close.assert_not_called()


@pytest.mark.parametrize(
    "code, clock",
    [
        (sqlite3.SQLITE_BUSY, [0, 31]),
        (sqlite3.SQLITE_LOCKED, [0, 31]),
        (sqlite3.SQLITE_IOERR, [0]),
    ],
)
def test_journal_mode_failure_closes_connection(
    tmp_path: Path, code: int, clock: list[int]
) -> None:
    error = sqlite3.OperationalError("journal mode failed")
    error.sqlite_errorcode = code
    connection = Mock(spec=sqlite3.Connection)
    connection.execute.side_effect = [None, error]
    with (
        patch(
            "vectorops.runner.runner.workspace_database.sqlite3.connect",
            return_value=connection,
        ),
        patch(
            "vectorops.runner.runner.workspace_database.time.monotonic",
            side_effect=clock,
        ),
        patch("vectorops.runner.runner.workspace_database.time.sleep") as sleep,
        pytest.raises(sqlite3.OperationalError) as failure,
    ):
        WorkspaceDatabase(tmp_path)._connect()
    assert failure.value is error
    sleep.assert_not_called()
    connection.close.assert_called_once_with()


async def test_old_workspace_files_are_ignored(tmp_path: Path) -> None:
    for name in ("runner-state.json", "control.sqlite3"):
        (tmp_path / name).write_text("not valid state")
    assert await RunnerStateStore(tmp_path).load() == state.RunnerState()
    assert (
        await RequestStore(WorkspaceDatabase(tmp_path)).lookup("request", "fingerprint")
        is None
    )


async def test_no_small_state_files_created(tmp_path: Path) -> None:
    async with RunnerStateStore(tmp_path) as store:
        await store.activate("execution")
        await RequestStore(store.database).save(
            "key", "fingerprint", protocol.Accepted(execution_id="execution")
        )
    assert {path.name for path in tmp_path.iterdir()} == {
        "runner.lock",
        "workspace.sqlite3",
    }
