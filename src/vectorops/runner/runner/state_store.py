"""Atomic JSON manifest persistence and exclusive project writer ownership."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import os
import tempfile
import typing

from vectorops.runner.runner import state
from vectorops.runner.runner.locking import FileLease


class RunnerStateStore:
    def __init__(self, data_path: Path) -> None:
        self.data_path = data_path
        self.path = data_path / "runner-state.json"
        self._descriptor: typing.Optional[int] = None
        self._lease = FileLease(data_path / "runner.lock")
        self.coordination = asyncio.Lock()
        self.reserved: set[str] = set()
        self.scheduled: set[str] = set()

    @asynccontextmanager
    async def reserve(self, execution_id: str) -> AsyncIterator[None]:
        async with self.coordination:
            if execution_id in self.reserved:
                raise ValueError("execution is already active")
            self.reserved.add(execution_id)
        try:
            yield
        finally:
            self.reserved.discard(execution_id)

    async def activate(self, execution_id: str) -> None:
        async with self.coordination:
            current = await self.load()
            identifiers = sorted(set(current.active_execution_ids) | {execution_id})
            await self.save(
                state.RunnerState(
                    last_execution_id=execution_id, active_execution_ids=identifiers
                )
            )

    async def reconcile(self, execution_ids: list[str]) -> None:
        async with self.coordination:
            current = await self.load()
            await self.save(
                state.RunnerState(
                    last_execution_id=current.last_execution_id,
                    active_execution_ids=sorted(execution_ids),
                )
            )

    async def deactivate(self, execution_id: str) -> None:
        async with self.coordination:
            current = await self.load()
            await self.save(
                current.model_copy(
                    update={
                        "active_execution_ids": [
                            identifier
                            for identifier in current.active_execution_ids
                            if identifier != execution_id
                        ]
                    }
                )
            )

    async def load(self) -> state.RunnerState:
        def read() -> state.RunnerState:
            if not self.path.exists():
                return state.RunnerState()
            return state.RunnerState.model_validate_json(self.path.read_bytes())

        return await asyncio.to_thread(read)

    async def save(self, manifest: state.RunnerState) -> None:
        if self._descriptor is None:
            raise RuntimeError("project writer ownership is required")
        payload = manifest.model_dump_json(indent=2)

        def write() -> None:
            descriptor, name = tempfile.mkstemp(
                prefix=".runner-state-", dir=self.data_path
            )
            try:
                with os.fdopen(descriptor, "w") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, self.path)
                if os.name != "nt":
                    directory = os.open(self.data_path, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            finally:
                if os.path.exists(name):
                    os.unlink(name)

        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def __aenter__(self) -> typing.Self:
        def acquire() -> None:
            self.data_path.mkdir(parents=True, exist_ok=True)
            self._lease.acquire()
            self._descriptor = self._lease.descriptor

        task = asyncio.create_task(asyncio.to_thread(acquire))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            await self.__aexit__()
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._descriptor is not None:
            self._descriptor = None
            await asyncio.to_thread(self._lease.release)
