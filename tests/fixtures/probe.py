import asyncio
from collections.abc import Callable
from typing import Optional

from vectorops.runner.runner.activity_context import ActivityExecutionContext


class ExecutionProbe:
    def __init__(self) -> None:
        self.calls: list[int] = []
        self.contexts: list[ActivityExecutionContext] = []
        self.effects: dict[str, int] = {}
        self.active = 0
        self.peak_active = 0
        self.cancelled = 0
        self.gate = asyncio.Event()
        self.started: asyncio.Queue[int] = asyncio.Queue()
        self.before_call: Optional[Callable[[], None]] = None


current = ExecutionProbe()
