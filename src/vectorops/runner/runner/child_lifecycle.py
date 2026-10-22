"""Execution-local cancellation signals; durable intent belongs to workflow history."""

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Optional

import pydantic


@dataclass
class Cancellation:
    requested: bool = False
    reason: Optional[str] = None
    workflow_task: Optional[asyncio.Task[pydantic.JsonValue]] = None
    cleanups: list[Callable[[], Awaitable[None]]] = field(default_factory=list)

    def request(self, reason: Optional[str]) -> None:
        if self.requested:
            return
        self.requested = True
        self.reason = reason
        if self.workflow_task is not None:
            self.workflow_task.cancel()

    async def close(self) -> None:
        failures: list[BaseException] = []
        while self.cleanups:
            try:
                await self.cleanups.pop()()
            except (Exception, asyncio.CancelledError) as exc:
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup("child cancellation cleanup failed", failures)


current: ContextVar[Optional[Cancellation]] = ContextVar(
    "child_cancellation", default=None
)
