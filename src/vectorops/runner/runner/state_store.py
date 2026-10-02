"""Atomic JSON manifest persistence and exclusive project writer ownership."""

from __future__ import annotations

import asyncio
import fcntl
from pathlib import Path
import os
import shutil
import tempfile
import typing

from vectorops.runner.runner import state


class RunnerStateStore:
    def __init__(self, data_path: Path) -> None:
        self.data_path = data_path
        self.path = data_path / "runner-state.json"
        self._descriptor: typing.Optional[int] = None

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
            descriptor = os.open(
                self.data_path / "runner.lock", os.O_CREAT | os.O_RDWR, 0o600
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                os.close(descriptor)
                raise RuntimeError("another runner owns this project") from exc
            self._descriptor = descriptor

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
            descriptor = self._descriptor
            self._descriptor = None
            await asyncio.to_thread(os.close, descriptor)

    async def remove(self, execution_id: str) -> None:
        """Delete only terminal execution history, then remove its registry entry."""
        if self._descriptor is None:
            raise RuntimeError("project writer ownership is required")
        manifest = await self.load()
        record = manifest.executions[execution_id]
        if record.status == "running":
            raise ValueError("active execution history cannot be removed")
        directory = self.data_path / "executions" / record.id
        if directory.exists():
            if not directory.resolve().is_relative_to(self.data_path.resolve()):
                raise ValueError("execution directory escapes project data")
            await asyncio.to_thread(shutil.rmtree, directory)
        records = dict(manifest.executions)
        del records[execution_id]
        await self.save(state.RunnerState(executions=records))
