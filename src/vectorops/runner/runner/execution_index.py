"""Startup reconciliation and incremental execution ordering, separate from recovery."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
import typing

from vectorops.runner.runner.summary_history import Snapshot
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class ExecutionIndex:
    def __init__(
        self,
        data_path: Path,
        read_snapshot: Callable[[str], Awaitable[Snapshot]],
        *,
        database: typing.Optional[WorkspaceDatabase] = None,
    ) -> None:
        self.data_path = data_path
        self.database = (
            database if database is not None else WorkspaceDatabase(data_path)
        )
        self.path = self.database.path
        self.read_snapshot = read_snapshot
        self.summaries = SummaryStore(self.database)
        self._initialized = False
        self._lock = asyncio.Lock()

    async def discover(self) -> list[str]:
        """Discover histories without consulting the cache, including during recovery."""

        def listing() -> list[str]:
            directory = self.data_path / "executions"
            if not directory.exists():
                return []
            return sorted(
                path.name
                for path in directory.iterdir()
                if (path / "history.sqlite3").is_file()
            )

        return await asyncio.to_thread(listing)

    async def invalidate(self) -> None:
        async with self._lock:
            await self.database.invalidate_index()
            self._initialized = False

    async def reconcile(self) -> None:
        """Repair missed publications and external additions/removals once at startup."""
        async with self._lock:
            await self._reconcile()

    async def _reconcile(self) -> None:
        while True:
            generation, _ = await self.database.index_status()
            snapshots = []
            for identifier in await self.discover():
                try:
                    snapshots.append(await self.read_snapshot(identifier))
                except (FileNotFoundError, KeyError):
                    continue
            if await self.summaries.reconcile(generation, snapshots):
                self._initialized = True
                return

    async def identifiers(
        self,
        *,
        after: typing.Optional[str] = None,
        limit: typing.Optional[int] = 100,
        active: Sequence[str] = (),
        active_only: bool = False,
    ) -> list[str]:
        """Page committed projections without timed archive discovery."""
        if active_only and not active:
            if after is not None:
                raise ValueError("execution index cursor does not exist")
            return []
        async with self._lock:
            if not self._initialized:
                await self._reconcile()
            return await self.database.index_page(
                after, limit, active=active, active_only=active_only
            )
