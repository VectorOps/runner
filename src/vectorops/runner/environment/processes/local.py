"""POSIX pipe driver with fresh process groups and explicit process environments."""

import asyncio
from collections.abc import AsyncIterator, Callable
import os
from pathlib import Path
import signal

from . import models as m
from .driver import ProcessHandle
from .environment import build_environment


class LocalProcessHandle:
    def __init__(
        self,
        process: asyncio.subprocess.Process,
        transport: asyncio.SubprocessTransport,
        kill_tree: bool = True,
    ) -> None:
        self._process = process
        self._transport = transport
        self._kill_tree = kill_tree

    async def read_output(self) -> AsyncIterator[m.OutputChunk]:
        """Multiplex one pending read per pipe without an unbounded intermediate queue."""
        streams = {"stdout": self._process.stdout, "stderr": self._process.stderr}
        pending: dict[asyncio.Task[bytes], str] = {}
        for name, stream in streams.items():
            assert stream is not None
            pending[asyncio.create_task(stream.read(16384))] = name
        try:
            while pending:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    name = pending.pop(task)
                    data = task.result()
                    if data:
                        yield m.OutputChunk(
                            stream="stdout" if name == "stdout" else "stderr", data=data
                        )
                        stream = streams[name]
                        assert stream is not None
                        pending[asyncio.create_task(stream.read(16384))] = name
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def write_stdin(self, data: bytes) -> None:
        stream = self._process.stdin
        if stream is None or stream.is_closing():
            raise BrokenPipeError("stdin is closed")
        stream.write(data)
        await stream.drain()

    async def close_stdin(self) -> None:
        if self._process.stdin is not None:
            self._process.stdin.close()

    async def wait(self) -> m.ExitResult:
        while self._process.returncode is None:
            await asyncio.sleep(0.01)
        return m.ExitResult(returncode=self._process.returncode)

    async def terminate(self, *, force: bool) -> None:
        try:
            sig = signal.SIGKILL if force else signal.SIGTERM
            if self._kill_tree:
                os.killpg(self._process.pid, sig)
            elif self._process.returncode is None:
                self._process.send_signal(sig)
        except ProcessLookupError:
            pass

    async def close(self) -> None:
        await self.close_stdin()
        for descriptor in (0, 1, 2):
            pipe = self._transport.get_pipe_transport(descriptor)
            if pipe is not None:
                pipe.close()
        if self._process.returncode is not None:
            self._transport.close()


class LocalProcessDriver:
    def __init__(self, resolve_cwd: Callable[[str], Path], policy: m.EnvPolicy) -> None:
        self._resolve_cwd = resolve_cwd
        self._policy = policy.model_copy(deep=True)

    @property
    def capabilities(self) -> m.DriverCapabilities:
        return m.DriverCapabilities(termination_scope="group")

    async def spawn(self, request: m.ExecRequest) -> ProcessHandle:
        task = asyncio.create_task(self._spawn(request))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            handle = await task
            try:
                await handle.terminate(force=True)
                await handle.wait()
            finally:
                await handle.close()
            raise

    async def _spawn(self, request: m.ExecRequest) -> ProcessHandle:
        cwd = await asyncio.to_thread(self._resolve_cwd, request.cwd)
        argv = (
            request.command.argv
            if isinstance(request.command, m.ArgvCommand)
            else ["/bin/sh", "-c", request.command.script]
        )
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.subprocess_exec(
            lambda: asyncio.subprocess.SubprocessStreamProtocol(limit=65536, loop=loop),
            *argv,
            cwd=cwd,
            env=build_environment(self._policy, os.environ, request.env),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return LocalProcessHandle(
            asyncio.subprocess.Process(transport, protocol, loop),
            transport,
            request.kill_tree,
        )
