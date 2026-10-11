"""Transactional workspace state and exclusive project writer ownership."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
import typing

from vectorops.runner.runner import state
from vectorops.runner.runner.locking import FileLease
from vectorops.runner.runner.resources import finish_cleanup
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class RunnerStateStore:
    def __init__(
        self, data_path: Path, database: typing.Optional[WorkspaceDatabase] = None
    ) -> None:
        self.data_path = data_path
        self.database = (
            database if database is not None else WorkspaceDatabase(data_path)
        )
        self.path = self.database.path
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
            await self._update(
                lambda current: state.RunnerState(
                    last_execution_id=execution_id,
                    active_execution_ids=sorted(
                        set(current.active_execution_ids) | {execution_id}
                    ),
                )
            )

    async def reconcile(self, execution_ids: list[str]) -> None:
        async with self.coordination:
            await self._update(
                lambda current: state.RunnerState(
                    last_execution_id=current.last_execution_id,
                    active_execution_ids=sorted(execution_ids),
                )
            )

    async def deactivate(self, execution_id: str) -> None:
        async with self.coordination:
            await self._update(
                lambda current: current.model_copy(
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
        return state.RunnerState.model_validate_json(
            await self.database.load_document(
                "runner", lambda: state.RunnerState().model_dump_json()
            )
        )

    async def save(self, manifest: state.RunnerState) -> None:
        if self._descriptor is None:
            raise RuntimeError("project writer ownership is required")
        await self.database.save_document("runner", manifest.model_dump_json())

    async def _update(
        self, update: Callable[[state.RunnerState], state.RunnerState]
    ) -> None:
        if self._descriptor is None:
            raise RuntimeError("project writer ownership is required")
        await self.database.update_document(
            "runner",
            lambda: state.RunnerState().model_dump_json(),
            lambda payload: update(
                state.RunnerState.model_validate_json(payload)
            ).model_dump_json(),
        )

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
        try:
            await self.database.__aenter__()
        except BaseException:
            self._descriptor = None
            await asyncio.to_thread(self._lease.release)
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._descriptor is not None:

            async def close() -> None:
                try:
                    await self.database.__aexit__()
                finally:
                    self._descriptor = None
                    await asyncio.to_thread(self._lease.release)

            await finish_cleanup(asyncio.create_task(close()))
