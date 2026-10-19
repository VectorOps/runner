"""Execution-owned target attachments and cross-target process routing."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional
from uuid import UUID

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.lifecycle import close_processes
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.environment.processes.manager import (
    ProcessManager,
    SessionCapacity,
)
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner.resources import finish_cleanup

from . import models as m
from .manager import Manager


class ExecutionTargets:
    def __init__(
        self,
        manager: Manager,
        config: LocalEnvironmentConfig,
        execution_id: Optional[str],
    ) -> None:
        self.manager = manager
        self.config = config
        self.execution_id = execution_id
        self._capacity = SessionCapacity(config.processes.max_sessions)
        self._stacks: list[AsyncExitStack] = []
        self._processes: dict[str, ProcessManager] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    async def _open(
        self, target: LocalExecutionTarget
    ) -> tuple[LocalEnvironment, AsyncExitStack, AsyncExitStack]:
        stack = AsyncExitStack()
        lease = AsyncExitStack()
        try:
            worktree_id = await self.manager.owning_worktree(target)
            if worktree_id is not None:
                selected = await lease.enter_async_context(
                    self.manager.use(worktree_id, self.execution_id)
                )
                if target.worktree_id is not None and selected != target:
                    raise m.WorktreeError("recorded worktree target changed")
            environment = await LocalEnvironment.open(
                self.config, target=target, capacity=self._capacity
            )

            async def cleanup() -> None:
                try:
                    await close_processes(environment.processes)
                except BaseException as exc:
                    await lease.__aexit__(type(exc), exc, exc.__traceback__)
                    raise
                else:
                    await lease.aclose()

            stack.push_async_callback(cleanup)
            self._stacks.append(stack)
            return environment, stack, lease
        except BaseException:
            await finish_cleanup(asyncio.create_task(lease.aclose()))
            raise

    async def open(self, target: LocalExecutionTarget) -> LocalEnvironment:
        environment, _, _ = await self._open(target)
        return environment

    @asynccontextmanager
    async def attach(
        self, target: LocalExecutionTarget
    ) -> AsyncIterator[LocalEnvironment]:
        environment, stack, _ = await self._open(target)
        try:
            yield environment
        finally:
            await finish_cleanup(asyncio.create_task(stack.aclose()))
            self._stacks.remove(stack)

    def process_manager(
        self, identifier: str, fallback: LocalEnvironment
    ) -> ProcessManager:
        return self._processes.get(identifier, fallback.processes)

    async def list_processes(
        self, fallback: LocalEnvironment
    ) -> list[processes.ProcessResult]:
        managers = {fallback.processes, *self._processes.values()}
        return [
            result for manager in managers for result in await manager.list_processes()
        ]

    async def exec(
        self, request: processes.ExecRequest, identifier: UUID
    ) -> processes.ProcessResult:
        """Retain the attachment until process cleanup, even if the calling activity is cancelled."""
        return await finish_cleanup(
            asyncio.create_task(self._exec(request, identifier))
        )

    async def _exec(
        self, request: processes.ExecRequest, identifier: UUID
    ) -> processes.ProcessResult:
        target = await self.manager.resolve(identifier)
        environment, stack, lease = await self._open(target)
        try:
            result = await environment.processes.exec(request)
        except BaseException:
            await finish_cleanup(asyncio.create_task(stack.aclose()))
            self._stacks.remove(stack)
            raise
        if result.state in ("exited", "failed") and result.output_complete:
            await lease.aclose()
            if not result.retained:
                await stack.aclose()
                self._stacks.remove(stack)
                return result
        self._processes[result.process_id] = environment.processes

        async def release() -> None:
            try:
                while True:
                    sessions = await environment.processes.list_processes()
                    if any(item.state == "unknown" for item in sessions):
                        return
                    if not sessions or all(
                        item.state in ("exited", "failed") and item.output_complete
                        for item in sessions
                    ):
                        break
                    await asyncio.sleep(0.1)
                await lease.aclose()
                while await environment.processes.list_processes():
                    await asyncio.sleep(0.1)
                await stack.aclose()
                self._stacks.remove(stack)
                self._processes.pop(result.process_id, None)
            finally:
                current = asyncio.current_task()
                if current is not None:
                    self._tasks.discard(current)

        task = asyncio.create_task(release())
        self._tasks.add(task)
        task.add_done_callback(
            lambda finished: None if finished.cancelled() else finished.exception()
        )
        return result

    async def close(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        failures: list[BaseException] = []
        while self._stacks:
            try:
                await self._stacks.pop().aclose()
            except (Exception, asyncio.CancelledError) as exc:
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup("target cleanup failed", failures)
