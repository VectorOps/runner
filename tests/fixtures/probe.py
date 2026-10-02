import asyncio
from collections.abc import Callable
from typing import Optional


class ExecutionProbe:
    def __init__(self) -> None:
        self.calls: list[int] = []
        self.active = 0
        self.peak_active = 0
        self.cancelled = 0
        self.gate = asyncio.Event()
        self.started: asyncio.Queue[int] = asyncio.Queue()
        self.before_call: Optional[Callable[[], None]] = None


current = ExecutionProbe()
