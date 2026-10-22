"""Root-execution resource ownership, independent of history and event delivery."""

import asyncio
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Optional, Protocol, Self

from vectorops.runner import diagnostics
from vectorops.runner.runner import child_lifecycle


class CleanupRegistry(Protocol):
    def register(self, name: str, cleanup: Callable[[], Awaitable[None]]) -> None: ...


async def finish_cleanup[T](task: asyncio.Task[T]) -> T:
    """Finish owned teardown despite repeated cancellation, then propagate cancellation."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


class ExecutionResources:
    def __init__(self) -> None:
        self._callbacks: list[tuple[str, Callable[[], Awaitable[None]]]] = []
        self._closing: Optional[asyncio.Task[None]] = None

    def register(self, name: str, cleanup: Callable[[], Awaitable[None]]) -> None:
        if self._closing is not None:
            raise RuntimeError("root execution resources are closing")
        scope = child_lifecycle.current.get()
        if scope is not None:
            original = cleanup
            pending: Optional[asyncio.Task[None]] = None

            async def invoke() -> None:
                await original()

            async def once() -> None:
                nonlocal pending
                if pending is None:
                    pending = asyncio.create_task(invoke())
                await finish_cleanup(pending)

            cleanup = once
            scope.cleanups.append(cleanup)
        self._callbacks.append((name, cleanup))

    async def __aenter__(self) -> Self:
        if self._closing is not None:
            raise RuntimeError("root execution resources are closed")
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        try:
            await self.close()
        except BaseExceptionGroup:
            if exc is None:
                raise

    async def close(self) -> None:
        if self._closing is None:
            self._closing = asyncio.create_task(self._cleanup())
        await finish_cleanup(self._closing)

    async def _cleanup(self) -> None:
        failures: list[BaseException] = []
        while self._callbacks:
            name, callback = self._callbacks.pop()
            try:
                await callback()
            except (Exception, asyncio.CancelledError) as exc:
                failures.append(exc)
                diagnostics.log(
                    "execution.cleanup_failed",
                    level=diagnostics.Level.ERROR,
                    fields={"resource": name},
                    error=exc,
                )
        if failures:
            raise BaseExceptionGroup("root execution cleanup failed", failures)
