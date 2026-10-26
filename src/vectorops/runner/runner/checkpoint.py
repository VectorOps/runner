"""Flush activity snapshots and checkpoint metadata without reconstructing heads."""

from __future__ import annotations

import asyncio
import math
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
import typing

from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner import execution_state as state
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.resources import finish_cleanup


class CheckpointReporter(typing.Protocol):
    def record(self, workflow_id: str, step_id: str) -> None: ...


class ExecutionCheckpointer:
    def __init__(
        self,
        store: SQLiteHistory,
        history: CachedHistory,
        record: state.ExecutionRecord,
        *,
        interval: float = 1.0,
    ) -> None:
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("checkpoint interval must be finite and positive")
        self.store = store
        self.history = history
        self.execution_id = record.id
        self.heads = dict(record.checkpoint.heads)
        self.interval = interval
        self._generation = 0
        self._persisted_generation = 0
        self._worker: typing.Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()

    def record(self, workflow_id: str, step_id: str) -> None:
        if self._worker is not None and self._worker.done():
            self._worker.result()
        if self.heads.get(workflow_id) != step_id:
            self.heads[workflow_id] = step_id
        self._generation += 1

    async def persist(
        self,
        status: typing.Optional[state.ExecutionStatus] = None,
    ) -> None:
        """Settle accepted writes before cancellation and acknowledge only flushed activity."""

        async def persist() -> None:
            async with self._lock:
                generation = self._generation
                await self.history.flush()
                await self.store.checkpoint_execution(self.execution_id, status=status)
                self._persisted_generation = generation

        await finish_cleanup(asyncio.create_task(persist()))

    async def _periodic(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            if self._generation != self._persisted_generation:
                await self.persist()

    @asynccontextmanager
    async def tracking(self) -> AsyncIterator[None]:
        self._worker = asyncio.create_task(self._periodic())
        try:
            yield
        finally:
            self._worker.cancel()
            results = await asyncio.gather(self._worker, return_exceptions=True)
            self._worker = None
            result = results[0]
            if isinstance(result, Exception):
                raise result
