"""Collect executor heads and flush history before publishing runner checkpoints."""

from __future__ import annotations

import asyncio
import math
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
import typing

from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner import state
from vectorops.runner.runner.state_store import RunnerStateStore


class CheckpointReporter(typing.Protocol):
    def record(self, workflow_id: str, step_id: str) -> None: ...


class ExecutionCheckpointer:
    def __init__(
        self,
        store: RunnerStateStore,
        history: CachedHistory,
        record: state.ExecutionRecord,
        *,
        interval: float = 1.0,
    ) -> None:
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("checkpoint interval must be finite and positive")
        self.store = store
        self.history = history
        self.execution = record
        self.heads = dict(record.checkpoint.heads)
        self.interval = interval
        self._dirty = False
        self._worker: typing.Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()

    def record(self, workflow_id: str, step_id: str) -> None:
        if self._worker is not None and self._worker.done():
            self._worker.result()
        self.heads[workflow_id] = step_id
        self._dirty = True

    async def persist(
        self,
        status: typing.Literal[
            "running", "completed", "failed", "cancelled"
        ] = "running",
    ) -> None:
        async with self._lock:
            await self.history.flush()
            heads = dict(self.heads)
            for workflow_id, step_id in heads.items():
                step = await self.history.get_step(step_id)
                if step.workflow_id != workflow_id:
                    raise ValueError("checkpoint head belongs to another workflow")
            completion_id = None
            root_head = heads.get(self.execution.root_workflow_id)
            if root_head is not None:
                root = await self.history.get_step(root_head)
                if root.operation.kind == "workflow.complete":
                    completion_id = root.id
            self.execution = self.execution.model_copy(
                update={
                    "status": status,
                    "updated_at": state.timestamp(),
                    "checkpoint": state.ExecutionCheckpoint(heads=heads),
                    "completion_step_id": completion_id,
                }
            )
            manifest = await self.store.load()
            records = dict(manifest.executions)
            records[self.execution.id] = self.execution
            await self.store.save(state.RunnerState(executions=records))
            self._dirty = self.heads != heads

    async def _periodic(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            if self._dirty:
                await self.persist()

    @asynccontextmanager
    async def tracking(self) -> AsyncIterator[None]:
        self._worker = asyncio.create_task(self._periodic())
        try:
            yield
        finally:
            self._worker.cancel()
            results = await asyncio.gather(self._worker, return_exceptions=True)
            result = results[0]
            if isinstance(result, Exception):
                raise result
