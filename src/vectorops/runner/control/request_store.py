"""Durable mutation receipts prevent duplicate launches across reconnections."""

import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3
import typing

from vectorops.runner.control import protocol as p


class RequestStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def lookup(self, key: str, fingerprint: str) -> typing.Optional[p.Accepted]:
        def read() -> typing.Optional[p.Accepted]:
            with closing(sqlite3.connect(self.path)) as connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS requests (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL)"
                )
                row = connection.execute(
                    "SELECT fingerprint, result FROM requests WHERE key = ?", (key,)
                ).fetchone()
                if row is None:
                    return None
                if row[0] != fingerprint:
                    raise p.RemoteError(
                        "idempotency_conflict",
                        "key already used for different arguments",
                    )
                return p.Accepted.model_validate_json(row[1])

        return await asyncio.to_thread(read)

    async def save(self, key: str, fingerprint: str, result: p.Accepted) -> None:
        def write() -> None:
            with closing(sqlite3.connect(self.path)) as connection, connection:
                connection.execute(
                    "INSERT INTO requests VALUES (?, ?, ?)",
                    (key, fingerprint, result.model_dump_json()),
                )

        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
