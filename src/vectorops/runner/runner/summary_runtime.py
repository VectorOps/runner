"""Committed summary ownership and bounded, execution-local publication."""

import asyncio
from typing import Optional

from vectorops.runner import diagnostics
from vectorops.runner.runner import events, summary_history
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.resources import finish_cleanup


class SummaryRuntime:
    """Serialize adapters and coalesce projections without weakening history commits."""

    def __init__(
        self, store: SummaryStore, sink: Optional[events.HistoryChangeSink]
    ) -> None:
        self.store = store
        self.sink = sink
        self.lock = asyncio.Lock()
        self.working = summary_history.WorkingSummary()
        self.active = False
        self.interval = 0.25
        self._pending: Optional[summary_history.Snapshot] = None
        self._published: Optional[summary_history.Snapshot] = None
        self._workflows: set[str] = set()
        self._notify = False
        self._timer: Optional[asyncio.Task[None]] = None
        self._wake = asyncio.Event()
        self._publication_lock = asyncio.Lock()

    async def changed(
        self,
        snapshot: summary_history.Snapshot,
        workflows: tuple[str, ...],
        *,
        prompt: bool,
        notify: bool = False,
    ) -> None:
        self._pending = snapshot
        self._workflows.update(workflows)
        self._notify = self._notify or notify
        if prompt or not self.active:
            await self.flush()
        elif self._timer is None:
            self._timer = asyncio.create_task(self._later())

    async def _later(self) -> None:
        try:
            while self._pending is not None:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.interval)
                except TimeoutError:
                    pass
                self._wake.clear()
                if not await self.flush():
                    break
        finally:
            self._timer = None

    async def flush(self) -> bool:
        """Keep failed work pending for a later mutation or close; recovery repairs crashes."""
        async with self._publication_lock:
            snapshot = self._pending
            if snapshot is None:
                return True
            previous = self._published
            notify = self._notify
            workflows = tuple(sorted(self._workflows))
            self._notify = False
            self._workflows.clear()
            relevant = previous is None or (
                snapshot.summary,
                snapshot.targets,
                snapshot.updated_at,
            ) != (previous.summary, previous.targets, previous.updated_at)
            try:
                if relevant:
                    await self.store.record(snapshot)
            except Exception as exc:
                self._notify = self._notify or notify
                self._workflows.update(workflows)
                try:
                    await self.store.database.invalidate_index()
                except Exception as invalidation_error:
                    diagnostics.log(
                        "summary.invalidate_failed",
                        level=diagnostics.Level.ERROR,
                        error=invalidation_error,
                    )
                diagnostics.log(
                    "summary.publish_failed", level=diagnostics.Level.ERROR, error=exc
                )
                return False
            self._published = snapshot
            if self._pending is snapshot:
                self._pending = None
            if self.sink is not None and (relevant or workflows or notify):
                try:
                    self.sink(workflows)
                except Exception as exc:
                    diagnostics.log(
                        "history.sink_failed", level=diagnostics.Level.ERROR, error=exc
                    )
            return True

    async def close(self) -> None:
        async def drain() -> None:
            self.active = False
            self._wake.set()
            if self._timer is not None:
                await self._timer
            await self.flush()

        await finish_cleanup(asyncio.create_task(drain()))
