"""Rebuildable, time-bounded execution discovery cache, separate from recovery state."""

import asyncio
from collections.abc import Awaitable, Callable
import math
from pathlib import Path
from time import time
import typing

import pydantic

from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.workspace_database import WorkspaceDatabase

REFRESH_INTERVAL: typing.Final[float] = 30.0


class IndexEntry(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    execution_id: str = pydantic.Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    updated_at: str


class IndexState(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    schema_version: typing.Literal[1] = 1
    refreshed_at: float = pydantic.Field(ge=0, allow_inf_nan=False)
    executions: list[IndexEntry] = pydantic.Field(default_factory=list)


class ExecutionIndex:
    def __init__(
        self,
        data_path: Path,
        read_record: Callable[[str], Awaitable[ExecutionRecord]],
        *,
        refresh_interval: float = REFRESH_INTERVAL,
        database: typing.Optional[WorkspaceDatabase] = None,
    ) -> None:
        if not math.isfinite(refresh_interval) or refresh_interval <= 0:
            raise ValueError("execution index refresh interval must be positive")
        self.data_path = data_path
        self.database = (
            database if database is not None else WorkspaceDatabase(data_path)
        )
        self.path = self.database.path
        self.read_record = read_record
        self.refresh_interval = refresh_interval
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

    async def identifiers(
        self, *, after: typing.Optional[str] = None, limit: typing.Optional[int] = 100
    ) -> list[str]:
        """Refresh expired snapshots on demand, never on individual message writes."""
        async with self._lock:
            while True:
                generation, refreshed_at = await self.database.index_status()
                now = time()
                if (
                    refreshed_at is not None
                    and 0 <= now - refreshed_at < self.refresh_interval
                ):
                    break
                entries = []
                for identifier in await self.discover():
                    try:
                        record = await self.read_record(identifier)
                        updated_at = record.updated_at
                    except FileNotFoundError:
                        continue
                    except KeyError:
                        updated_at = ""
                    entries.append(
                        IndexEntry(execution_id=identifier, updated_at=updated_at)
                    )
                if await self.database.replace_index(
                    generation,
                    now,
                    [(entry.execution_id, entry.updated_at) for entry in entries],
                ):
                    break
            return await self.database.index_page(after, limit)
