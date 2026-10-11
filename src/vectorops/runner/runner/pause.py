"""Execution-wide mailbox blocking, independent of task residency."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Optional

from vectorops.runner.runner.sqlite_history import SQLiteHistory


class ExecutionPause:
    def __init__(
        self,
        store: SQLiteHistory,
        flush: Callable[[], Awaitable[None]],
        changed: Callable[[str], None],
    ) -> None:
        self.store = store
        self.flush = flush
        self.changed = changed
        self.sessions: dict[str, Callable[[], Optional[set[str]]]] = {}
        self.requests: set[str] = set()
        self.frozen = False
        self._paused = False
        self._lock = asyncio.Lock()

    def _blocked(self) -> set[str]:
        requests: set[str] = set()
        if not self.sessions:
            return requests
        for inspect in self.sessions.values():
            blocked = inspect()
            if blocked is None:
                return set()
            requests.update(blocked)
        return requests

    async def refresh(self) -> None:
        async with self._lock:
            if self.frozen or self._paused or not self._blocked():
                return
            await self.flush()
            requests = self._blocked()
            if requests and await self.store.set_paused(requests):
                self._paused = True
                self.changed("paused")

    async def running(self) -> None:
        async with self._lock:
            if not self.frozen and self._paused:
                await self.store.wake_execution()
                self._paused = False
                self.changed("resumed")

    async def wait(self, request_id: str) -> None:
        self.requests.add(request_id)
        await self.refresh()

    async def release(self, request_id: str) -> None:
        self.requests.discard(request_id)
        await self.running()
