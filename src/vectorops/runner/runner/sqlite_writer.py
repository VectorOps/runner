"""Bounded, execution-scoped ownership of a SQLite connection and worker thread."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import sqlite3
from typing import Optional

from vectorops.runner.runner.resources import finish_cleanup


class SQLiteWriter:
    def __init__(self, connect: Callable[[], sqlite3.Connection]) -> None:
        self._connect = connect
        self._connection: Optional[sqlite3.Connection] = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="history")
        self._capacity = asyncio.Semaphore(64)
        self._closing: Optional[asyncio.Task[None]] = None

    def _invoke[T](self, operation: Callable[[sqlite3.Connection], T]) -> T:
        if self._connection is None:
            self._connection = self._connect()
        return operation(self._connection)

    async def run[T](self, operation: Callable[[sqlite3.Connection], T]) -> T:
        """Queue bounded work and settle accepted operations before cancellation."""
        async with self._capacity:
            if self._closing is not None:
                raise RuntimeError("history writer is closed")
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, copy_context().run, self._invoke, operation
            )

            async def settle() -> T:
                return await future

            return await finish_cleanup(asyncio.create_task(settle()))

    async def aclose(self) -> None:
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await finish_cleanup(self._closing)

    async def _close(self) -> None:
        """Close after accepted work on its owning thread, then join the worker."""

        def close() -> None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

        try:
            await asyncio.get_running_loop().run_in_executor(self._executor, close)
        finally:
            await asyncio.to_thread(self._executor.shutdown, wait=True)
