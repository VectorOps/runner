import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3

import pytest

from tests.runner.test_usage import seed
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def test_migration_refuses_an_open_legacy_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "history.sqlite3"
    await seed(SQLiteHistory(path))
    with closing(sqlite3.connect(path)) as legacy:
        legacy.execute("PRAGMA user_version = 1")
        legacy.execute("SELECT * FROM summary_state").fetchall()
        migrated = SQLiteHistory(path)

        def connect() -> sqlite3.Connection:
            return sqlite3.connect(path, timeout=0.01)

        monkeypatch.setattr(migrated, "_connect", connect)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            await asyncio.to_thread(migrated._initialize)
        assert legacy.execute("PRAGMA user_version").fetchone() == (1,)
    await migrated.get_execution()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
