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
        self,
        store: SummaryStore,
        sink: Optional[events.HistoryChangeSink],
        *,
        publication_sink: Optional[events.PublicationSink] = None,
    ) -> None:
        self.store = store
        self.sink = sink
        self.publication_sink = publication_sink
        self.lock = asyncio.Lock()
        self.working = summary_history.WorkingSummary()
        self.active = False
        self.interval = 0.25
        self._pending: Optional[summary_history.Snapshot] = None
        self._published: Optional[summary_history.Snapshot] = None
        self._workflows: set[str] = set()
        self._notify = False
        self._execution_changed = False
        self._recorded: Optional[summary_history.Snapshot] = None
        self._timer: Optional[asyncio.Task[None]] = None
        self._wake = asyncio.Event()
        self._publication_lock = asyncio.Lock()
        self._next_publication = 0.0

    async def changed(
        self,
        snapshot: summary_history.Snapshot,
        workflows: tuple[str, ...],
        *,
        urgency: summary_history.Urgency,
        notify: bool = False,
        execution_changed: bool = False,
    ) -> None:
        self._pending = snapshot
        self._workflows.update(workflows)
        self._notify = self._notify or notify
        self._execution_changed = self._execution_changed or execution_changed
        references_changed = (
            bool(snapshot.targets)
            if self._published is None
            else (
                self._published.targets is not snapshot.targets
                and self._published.targets != snapshot.targets
            )
        )
        if (
            urgency == summary_history.Urgency.IMMEDIATE
            or references_changed
            or not self.active
        ):
            await self.flush()
        elif self._timer is None:
            self._next_publication = max(
                self._next_publication,
                asyncio.get_running_loop().time() + self.interval,
            )
            self._timer = asyncio.create_task(self._later())

    async def _later(self) -> None:
        try:
            while self._pending is not None:
                delay = max(
                    0.0, self._next_publication - asyncio.get_running_loop().time()
                )
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=delay)
                except TimeoutError:
                    if asyncio.get_running_loop().time() < self._next_publication:
                        continue
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
            self._next_publication = asyncio.get_running_loop().time() + self.interval
            notify = self._notify
            execution_changed = self._execution_changed
            workflows = tuple(sorted(self._workflows))
            self._notify = False
            self._execution_changed = False
            self._workflows.clear()
            relevant = previous is None or (
                snapshot.summary,
                snapshot.targets,
                snapshot.updated_at,
            ) != (previous.summary, previous.targets, previous.updated_at)
            try:
                if relevant:
                    recorded = self._recorded
                    projection = snapshot
                    if (
                        recorded is not None
                        and recorded.source_id == snapshot.source_id
                        and recorded.targets is snapshot.targets
                    ):
                        projection = snapshot.model_copy(
                            update={"targets_base_revision": recorded.revision}
                        )
                    await self.store.record(projection)
                    self._recorded = snapshot
            except Exception as exc:
                self._notify = self._notify or notify
                self._execution_changed = self._execution_changed or execution_changed
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
            if self.publication_sink is not None:
                publication = events.Publication(
                    workflows=workflows,
                    summary=previous is None or previous.summary != snapshot.summary,
                    execution=execution_changed,
                )
                if (
                    publication.workflows
                    or publication.summary
                    or publication.execution
                ):
                    try:
                        self.publication_sink(publication)
                    except Exception as exc:
                        diagnostics.log(
                            "history.sink_failed",
                            level=diagnostics.Level.ERROR,
                            error=exc,
                        )
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
