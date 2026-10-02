"""Bounded read caching and write-behind coalescing for streaming snapshots."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator
from contextlib import aclosing
import math
import typing

from vectorops.runner import models
from vectorops.runner.runner.history import WorkflowHistory


class CachedHistory:
    """Own a cache for one writer; external changes require a fresh cache instance."""

    def __init__(
        self,
        backend: WorkflowHistory,
        *,
        cache_size: int = 256,
        max_pending: int = 64,
        flush_interval: float = 0.25,
    ) -> None:
        if cache_size < 1 or max_pending < 1:
            raise ValueError("cache and pending limits must be positive")
        if not math.isfinite(flush_interval) or flush_interval <= 0:
            raise ValueError("flush interval must be finite and positive")
        self.backend = backend
        self.cache_size = cache_size
        self.max_pending = max_pending
        self.flush_interval = flush_interval
        self._workflows: OrderedDict[str, models.Workflow] = OrderedDict()
        self._steps: OrderedDict[str, models.WorkflowStep] = OrderedDict()
        self._pending: dict[str, models.WorkflowStep] = {}
        self._lock = asyncio.Lock()
        self._worker: typing.Optional[asyncio.Task[None]] = None
        self._error: typing.Optional[Exception] = None
        self._closed = False

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError("history cache is closed")
        if self._error is not None:
            raise self._error

    def _cache_step(self, step: models.WorkflowStep) -> None:
        self._steps[step.id] = step
        self._steps.move_to_end(step.id)
        if len(self._steps) > self.cache_size:
            self._steps.popitem(last=False)

    def _cache_workflow(self, workflow: models.Workflow) -> None:
        self._workflows[workflow.id] = workflow
        self._workflows.move_to_end(workflow.id)
        if len(self._workflows) > self.cache_size:
            self._workflows.popitem(last=False)

    async def save_workflow(self, workflow: models.Workflow) -> None:
        async with self._lock:
            self._check()
            cached = self._workflows.get(workflow.id)
            if cached is not None:
                if cached != workflow:
                    raise ValueError("workflow configuration conflict")
                return
            await self.backend.save_workflow(workflow)
            self._cache_workflow(workflow.model_copy(deep=True))

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        async with self._lock:
            self._check()
            workflow = self._workflows.get(workflow_id)
            if workflow is None:
                workflow = await self.backend.get_workflow(workflow_id)
            self._cache_workflow(workflow)
            return workflow.model_copy(deep=True)

    async def _get_step(self, step_id: str) -> models.WorkflowStep:
        step = self._pending.get(step_id) or self._steps.get(step_id)
        if step is None:
            step = await self.backend.get_step(step_id)
        self._cache_step(step)
        return step

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        async with self._lock:
            self._check()
            return (await self._get_step(step_id)).model_copy(deep=True)

    async def append_step(self, step: models.WorkflowStep) -> None:
        async with self._lock:
            self._check()
            await self.backend.append_step(step)
            self._cache_step(step.model_copy(deep=True))

    async def update_step(self, step: models.WorkflowStep) -> None:
        async with self._lock:
            self._check()
            previous = await self._get_step(step.id)
            if previous.outcome.kind not in ("streaming", "pending", "waiting"):
                raise ValueError("terminal steps cannot be updated")
            if (
                previous.workflow_id,
                previous.previous_step_id,
                previous.operation,
                previous.deferred_delivery,
            ) != (
                step.workflow_id,
                step.previous_step_id,
                step.operation,
                step.deferred_delivery,
            ):
                raise ValueError("step identity and links cannot change")
            snapshot = step.model_copy(deep=True)
            if snapshot.outcome.kind == "streaming":
                self._pending[step.id] = snapshot
                self._cache_step(snapshot)
                if len(self._pending) >= self.max_pending:
                    await self._flush()
                elif self._worker is None:
                    self._worker = asyncio.create_task(self._periodic_flush())
            else:
                await self.backend.update_step(snapshot)
                self._pending.pop(step.id, None)
                self._cache_step(snapshot)

    async def _flush(self) -> None:
        for step_id in tuple(self._pending):
            await self.backend.update_step(self._pending[step_id])
            del self._pending[step_id]

    async def flush(self) -> None:
        async with self._lock:
            self._check()
            await self._flush()

    async def _periodic_flush(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.flush_interval)
                async with self._lock:
                    if self._closed:
                        return
                    await self._flush()
                    if not self._pending:
                        return
        except Exception as exc:
            self._error = exc
        finally:
            self._worker = None

    async def aclose(self) -> None:
        """Stop the timer and persist latest snapshots before releasing ownership."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            worker = self._worker
            if worker is not None:
                worker.cancel()
            if self._error is not None:
                error = self._error
            else:
                error = None
                try:
                    await self._flush()
                except Exception as exc:
                    error = exc
            self._workflows.clear()
            self._steps.clear()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        if error is not None:
            raise error

    async def __aenter__(self) -> typing.Self:
        self._check()
        return self

    async def __aexit__(self, *args: object) -> None:
        task = asyncio.create_task(self.aclose())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        await self.flush()
        async with aclosing(
            self.backend.replay_path(workflow_id, head_step_id)
        ) as path:
            async for step in path:
                self._check()
                yield step
