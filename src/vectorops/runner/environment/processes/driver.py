"""Platform-independent process driver and handle contracts."""

from collections.abc import AsyncIterator
from typing import Protocol

from . import models as m


class ProcessHandle(Protocol):
    def read_output(self) -> AsyncIterator[m.OutputChunk]: ...

    async def write_stdin(self, data: bytes) -> None: ...

    async def close_stdin(self) -> None: ...

    async def wait(self) -> m.ExitResult: ...

    async def terminate(self, *, force: bool) -> None: ...

    async def close(self) -> None: ...


class ProcessDriver(Protocol):
    @property
    def capabilities(self) -> m.DriverCapabilities: ...

    async def spawn(self, request: m.ExecRequest) -> ProcessHandle: ...
