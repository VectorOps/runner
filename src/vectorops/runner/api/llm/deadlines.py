"""Per-attempt deadlines that count provider waits, not consumer persistence."""

import asyncio
from collections.abc import AsyncGenerator, Iterator
from contextlib import aclosing, contextmanager
from contextvars import ContextVar
from copy import deepcopy
from typing import Final, Optional

import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.api.llm import models
from vectorops.runner.runner import streaming


class StreamTimeoutError(TimeoutError):
    def __init__(self, phase: str, seconds: float) -> None:
        self.phase = phase
        self.seconds = seconds
        super().__init__(
            f"LLM {phase} deadline exceeded ({seconds:g} seconds without output)"
        )

    def log(self) -> None:
        diagnostics.log(
            "llm.request_timeout",
            level=diagnostics.Level.WARNING,
            fields={"phase": self.phase, "timeout_seconds": self.seconds},
        )


class _Wait:
    def __init__(self, seconds: float) -> None:
        self.remaining = seconds
        self.timer = asyncio.timeout(seconds)
        self.paused = False

    @contextmanager
    def suspend(self) -> Iterator[None]:
        if self.paused or self.timer.expired():
            yield
            return
        deadline = self.timer.when()
        assert deadline is not None
        self.remaining = max(0, deadline - asyncio.get_running_loop().time())
        self.paused = True
        self.timer.reschedule(None)
        try:
            yield
        finally:
            self.paused = False
            self.timer.reschedule(asyncio.get_running_loop().time() + self.remaining)


_WAIT: Final[ContextVar[Optional[_Wait]]] = ContextVar(
    "llm_provider_wait", default=None
)


@contextmanager
def suspend() -> Iterator[None]:
    """Exclude provider authentication, including host-assisted credential refresh."""
    wait = _WAIT.get()
    if wait is None:
        yield
    else:
        with wait.suspend():
            yield


class _Progress:
    def __init__(self) -> None:
        self.values: dict[tuple[int, str], pydantic.JsonValue] = {}

    def update(self, value: pydantic.JsonValue) -> bool:
        """Inspect output fields only; metadata and unchanged snapshots are not progress."""
        if not isinstance(value, dict):
            return False
        content = value.get("content")
        if not isinstance(content, list):
            return False
        changed = False
        for index, block in enumerate(content):
            if not isinstance(block, dict):
                continue
            kind = block.get("type")
            fields = (
                ("text",)
                if kind in ("text", "reasoning")
                else ("arguments_text", "arguments") if kind == "tool_call" else ()
            )
            for field in fields:
                output = block.get(field)
                if output is None or output == "":
                    continue
                key = (index, field)
                if key not in self.values or self.values[key] != output:
                    self.values[key] = deepcopy(output)
                    changed = True
        return changed


async def stream(
    items: AsyncGenerator[streaming.StreamItem, None], settings: models.Settings
) -> AsyncGenerator[streaming.StreamItem, None]:
    """Reset an idle budget on output, pausing it while downstream consumes snapshots."""
    progress = _Progress()
    started = False
    remaining = settings.first_token_timeout
    async with aclosing(items):
        while True:
            wait = _Wait(remaining)
            token = _WAIT.set(wait)
            try:
                async with wait.timer:
                    item = await anext(items)
                deadline = wait.timer.when()
                assert deadline is not None
                remaining = max(0, deadline - asyncio.get_running_loop().time())
            except StopAsyncIteration:
                return
            except TimeoutError:
                if not wait.timer.expired():
                    raise
                phase = "stream_stall" if started else "first_token"
                seconds = (
                    settings.stream_stall_timeout
                    if started
                    else settings.first_token_timeout
                )
                error = StreamTimeoutError(phase, seconds)
                error.log()
                raise error from None
            finally:
                _WAIT.reset(token)
            if remaining <= 0:
                error = StreamTimeoutError(
                    "stream_stall" if started else "first_token",
                    (
                        settings.stream_stall_timeout
                        if started
                        else settings.first_token_timeout
                    ),
                )
                error.log()
                raise error
            if isinstance(item, streaming.ActivityFinal) or progress.update(item.value):
                started = True
                remaining = settings.stream_stall_timeout
            yield item
