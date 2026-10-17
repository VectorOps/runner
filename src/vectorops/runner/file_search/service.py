"""Async lifecycle and latest-only delivery, independent of UI protocols."""

import asyncio
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Optional

from . import index, matcher, models


@dataclass(eq=False)
class _Subscription:
    event: asyncio.Event = field(default_factory=asyncio.Event)
    queue: asyncio.Queue[Optional[models.SearchUpdate]] = field(
        default_factory=lambda: asyncio.Queue(maxsize=1)
    )
    cancel: threading.Event = field(default_factory=threading.Event)
    task: Optional[asyncio.Task[None]] = None

    def deliver(self, update: Optional[models.SearchUpdate]) -> None:
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(update)


class FileSearchService:
    def __init__(self, root: Path, config: models.SearchConfig) -> None:
        self._root = root.absolute()
        self._config = config
        self._discovery = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="file-index"
        )
        self._matching = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="file-match"
        )
        self._matcher = matcher.Matcher(config.max_cache_candidates)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._view = index.IndexView()
        self._revision = 0
        self._refresh_id = models.RefreshId(0)
        self._refreshing = False
        self._incomplete = True
        self._limited = False
        self._error: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task[None]] = None
        self._stop = threading.Event()
        self._subscriptions: set[_Subscription] = set()
        self._closed = False
        self._close_task: Optional[asyncio.Task[None]] = None

    def _bind(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif loop is not self._loop:
            raise RuntimeError("FileSearchService must be used on one event loop")
        return loop

    def _notify(self) -> None:
        for subscription in self._subscriptions:
            subscription.event.set()

    def _publish(self, view: index.IndexView) -> None:
        if not self._closed and self._refreshing:
            self._view = view
            self._revision += 1
            self._notify()

    async def refresh(self) -> models.RefreshId:
        """Schedule a background refresh, reusing the active refresh if present."""
        self._bind()
        if self._closed:
            raise RuntimeError("FileSearchService is closed")
        if not self._refreshing:
            self._refresh_id = models.RefreshId(self._refresh_id + 1)
            self._refreshing = True
            self._stop.clear()
            self._refresh_task = asyncio.create_task(self._run_refresh())
            self._notify()
        return self._refresh_id

    async def _run_refresh(self) -> None:
        """Bridge worker publications through a bounded latest-view mailbox."""
        loop = self._bind()
        lock = threading.Lock()
        pending: Optional[index.IndexView] = None
        scheduled = False

        def drain() -> None:
            nonlocal pending, scheduled
            with lock:
                view, pending = pending, None
                scheduled = False
            if view is not None:
                self._publish(view)

        def publish(view: index.IndexView) -> None:
            nonlocal pending, scheduled
            with lock:
                pending = view
                if not scheduled:
                    scheduled = True
                    loop.call_soon_threadsafe(drain)

        outcome = await loop.run_in_executor(
            self._discovery,
            index.build_refresh,
            self._root,
            self._config,
            self._view,
            self._stop,
            publish,
        )
        self._view = outcome.view
        self._revision += 1
        self._refreshing = False
        self._incomplete = outcome.error is not None
        self._limited = outcome.limited
        self._error = outcome.error
        self._notify()

    def search(
        self, query: str, *, limit: int = 20
    ) -> AsyncGenerator[models.SearchUpdate, None]:
        """Stream complete top-K snapshots until cancelled or the service closes."""
        if not 1 <= limit <= self._config.max_results:
            raise ValueError(f"limit must be between 1 and {self._config.max_results}")
        if len(query) > self._config.max_query_length:
            raise ValueError("Query length limit exceeded")
        return self._search(query, limit)

    async def _search(
        self, query: str, limit: int
    ) -> AsyncGenerator[models.SearchUpdate, None]:
        self._bind()
        if self._closed:
            raise RuntimeError("FileSearchService is closed")
        subscription = _Subscription()
        self._subscriptions.add(subscription)
        subscription.event.set()
        subscription.task = asyncio.create_task(
            self._evaluate(subscription, query, limit)
        )
        try:
            while (update := await subscription.queue.get()) is not None:
                yield update
        finally:
            subscription.cancel.set()
            subscription.task.cancel()
            await asyncio.gather(subscription.task, return_exceptions=True)
            self._subscriptions.discard(subscription)

    async def _evaluate(
        self, subscription: _Subscription, query: str, limit: int
    ) -> None:
        loop = self._bind()
        while True:
            await subscription.event.wait()
            subscription.event.clear()
            revision = self._revision
            refreshing, incomplete = self._refreshing, self._incomplete
            refresh_id, error, limited = self._refresh_id, self._error, self._limited
            results, truncated = await loop.run_in_executor(
                self._matching,
                self._matcher.evaluate,
                self._view,
                query,
                limit,
                subscription.cancel,
            )
            subscription.deliver(
                models.SearchUpdate(
                    index_revision=revision,
                    results=results,
                    refreshing=refreshing,
                    incomplete=incomplete,
                    truncated=truncated or limited,
                    refresh_id=refresh_id if refresh_id else None,
                    refresh_error=error,
                )
            )

    async def close(self) -> None:
        """Stop discovery and matching workers and terminate all subscriptions."""
        self._bind()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._shutdown())
        await asyncio.shield(self._close_task)

    async def _shutdown(self) -> None:
        self._stop.set()
        tasks: list[asyncio.Task[None]] = []
        for subscription in self._subscriptions:
            subscription.cancel.set()
            if subscription.task is not None:
                subscription.task.cancel()
                tasks.append(subscription.task)
            subscription.deliver(None)
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._refresh_task is not None:
            await self._refresh_task
        self._subscriptions.clear()
        await asyncio.to_thread(
            self._discovery.shutdown, wait=True, cancel_futures=True
        )
        await asyncio.to_thread(self._matching.shutdown, wait=True, cancel_futures=True)
