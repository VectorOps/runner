"""Rebuildable, time-bounded execution discovery cache, separate from recovery state."""

import asyncio
from collections.abc import Awaitable, Callable
import math
import os
from pathlib import Path
import tempfile
from time import time
import typing

import pydantic

from vectorops.runner.runner.execution_state import ExecutionRecord

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
    ) -> None:
        if not math.isfinite(refresh_interval) or refresh_interval <= 0:
            raise ValueError("execution index refresh interval must be positive")
        self.data_path = data_path
        self.path = data_path / "execution-index.json"
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

    def _load(self) -> typing.Optional[IndexState]:
        try:
            return IndexState.model_validate_json(self.path.read_bytes())
        except (FileNotFoundError, pydantic.ValidationError):
            return None

    def _save(self, snapshot: IndexState) -> None:
        self.data_path.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(
            prefix=".execution-index-", dir=self.data_path
        )
        try:
            with os.fdopen(descriptor, "w") as stream:
                stream.write(snapshot.model_dump_json(indent=2))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    async def invalidate(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self.path.unlink, missing_ok=True)

    async def identifiers(
        self, *, after: typing.Optional[str] = None, limit: typing.Optional[int] = 100
    ) -> list[str]:
        """Refresh expired snapshots on demand, never on individual message writes."""
        async with self._lock:
            snapshot = await asyncio.to_thread(self._load)
            now = time()
            if (
                snapshot is None
                or not 0 <= now - snapshot.refreshed_at < self.refresh_interval
            ):
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
                entries.sort(key=lambda entry: entry.updated_at, reverse=True)
                snapshot = IndexState(refreshed_at=now, executions=entries)
                task = asyncio.create_task(asyncio.to_thread(self._save, snapshot))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
            identifiers = [entry.execution_id for entry in snapshot.executions]
            start = identifiers.index(after) + 1 if after is not None else 0
            return identifiers[start : None if limit is None else start + limit]
