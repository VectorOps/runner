"""Absolute-time waits multiplexed through one earliest-deadline timer."""

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
import heapq
import itertools
import time
from typing import Optional


def normalize_deadline(deadline: datetime) -> datetime:
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise ValueError("sleep deadline must be timezone-aware")
    return deadline.astimezone(timezone.utc)


class Scheduler:
    """Own a priority queue of cancellable waits, ordered by UTC deadline."""

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._queue: list[tuple[float, int, asyncio.Future[None]]] = []
        self._sequence = itertools.count()
        self._timer: Optional[asyncio.TimerHandle] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def wait_until(self, deadline: datetime) -> None:
        when = normalize_deadline(deadline).timestamp()
        loop = asyncio.get_running_loop()
        if self._queue and self._loop is not loop:
            raise RuntimeError("scheduler has waits on another event loop")
        self._loop = loop
        future: asyncio.Future[None] = loop.create_future()
        entry = (when, next(self._sequence), future)
        heapq.heappush(self._queue, entry)
        self._arm()
        try:
            await future
        finally:
            if future.cancelled() and entry in self._queue:
                self._queue.remove(entry)
                heapq.heapify(self._queue)
                self._arm()

    def _arm(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._queue:
            assert self._loop is not None
            delay = max(0.0, self._queue[0][0] - self._clock())
            self._timer = self._loop.call_later(min(delay, 60.0), self._wake)

    def _wake(self) -> None:
        """Recheck wall time so clock adjustments never complete a wait early."""
        now = self._clock()
        while self._queue and self._queue[0][0] <= now:
            _, _, future = heapq.heappop(self._queue)
            if not future.done():
                future.set_result(None)
        self._arm()
