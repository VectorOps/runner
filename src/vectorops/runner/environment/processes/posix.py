"""POSIX pipe and PTY processes with independent sessions and asynchronous I/O."""

import asyncio
from collections.abc import AsyncIterator
import errno
import os
from pathlib import Path
import pty
import signal
import sys
import termios
from typing import Optional

from . import models as m
from .driver import ProcessHandle
from ..errors import EnvironmentError, ErrorCode
from ..paths import resolve_path
from .environment import build_environment


async def _wait_fd(fd: int, *, writing: bool = False) -> None:
    loop = asyncio.get_running_loop()
    ready: asyncio.Future[None] = loop.create_future()
    remove = loop.remove_writer if writing else loop.remove_reader

    def notify() -> None:
        remove(fd)
        if not ready.done():
            ready.set_result(None)

    if writing:
        loop.add_writer(fd, notify)
    else:
        loop.add_reader(fd, notify)
    try:
        await ready
    finally:
        remove(fd)


async def _read_fd(fd: int) -> bytes:
    while True:
        try:
            return os.read(fd, 16384)
        except BlockingIOError:
            await _wait_fd(fd)


class PosixProcessHandle:
    def __init__(
        self,
        process: asyncio.subprocess.Process,
        transport: asyncio.SubprocessTransport,
        kill_tree: bool = True,
        terminal: Optional[int] = None,
    ) -> None:
        self._process = process
        self._transport = transport
        self._kill_tree = kill_tree
        self._terminal = terminal

    async def read_output(self) -> AsyncIterator[m.OutputChunk]:
        """Drain a terminal or multiplex pipes without an unbounded intermediate queue."""
        if self._terminal is not None:
            while True:
                try:
                    data = await _read_fd(self._terminal)
                except OSError as exc:
                    if exc.errno != errno.EIO:
                        raise
                    return
                if not data:
                    return
                yield m.OutputChunk(stream="terminal", data=data)
                await asyncio.sleep(0)
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
        if self._terminal is not None:
            remaining = memoryview(data)
            while remaining:
                try:
                    written = os.write(self._terminal, remaining)
                except BlockingIOError:
                    await _wait_fd(self._terminal, writing=True)
                else:
                    remaining = remaining[written:]
            return
        stream = self._process.stdin
        if stream is None or stream.is_closing():
            raise BrokenPipeError("stdin is closed")
        stream.write(data)
        await stream.drain()

    async def close_stdin(self) -> None:
        if self._terminal is not None:
            raise EnvironmentError(
                ErrorCode.UNSUPPORTED, "PTY stdin cannot be half-closed"
            )
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
            if self._process.returncode is None:
                try:
                    self._process.send_signal(sig)
                except ProcessLookupError:
                    pass

    async def close(self) -> None:
        if self._terminal is not None:
            os.close(self._terminal)
            self._terminal = None
        else:
            await self.close_stdin()
        for descriptor in (0, 1, 2):
            pipe = self._transport.get_pipe_transport(descriptor)
            if pipe is not None:
                pipe.close()
        if self._process.returncode is not None:
            self._transport.close()


class PosixProcessDriver:
    def __init__(self, root: Path, policy: m.EnvPolicy) -> None:
        self._root = root
        self._policy = policy.model_copy(deep=True)

    @property
    def capabilities(self) -> m.DriverCapabilities:
        return m.DriverCapabilities(pty=True, termination_scope="group")

    async def spawn(self, request: m.ExecRequest) -> ProcessHandle:
        task = asyncio.create_task(self._spawn(request))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:

            async def cleanup() -> None:
                handle = await task
                try:
                    await handle.terminate(force=True)
                    await handle.wait()
                finally:
                    await handle.close()

            closing = asyncio.create_task(cleanup())
            while not closing.done():
                try:
                    await asyncio.shield(closing)
                except asyncio.CancelledError:
                    pass
            closing.result()
            raise

    async def _spawn(self, request: m.ExecRequest) -> ProcessHandle:
        cwd = await asyncio.to_thread(resolve_path, self._root, request.cwd)
        argv = (
            request.command.argv
            if isinstance(request.command, m.ArgvCommand)
            else ["/bin/sh", "-c", request.command.script]
        )
        env = build_environment(self._policy, os.environ, request.env)
        if request.mode == "pty":
            return await self._spawn_pty(request, argv, cwd, env)
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.subprocess_exec(
            lambda: asyncio.subprocess.SubprocessStreamProtocol(limit=65536, loop=loop),
            *argv,
            cwd=cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return PosixProcessHandle(
            asyncio.subprocess.Process(transport, protocol, loop),
            transport,
            request.kill_tree,
        )

    async def _spawn_pty(
        self, request: m.ExecRequest, argv: list[str], cwd: Path, env: dict[str, str]
    ) -> PosixProcessHandle:
        """Use an isolated exec launcher instead of a thread-unsafe preexec_fn."""
        master, slave = pty.openpty()
        error_read: Optional[int] = None
        error_write: Optional[int] = None
        handle: Optional[PosixProcessHandle] = None
        try:
            size = request.terminal_size or m.TerminalSize()
            termios.tcsetwinsize(slave, (size.rows, size.cols))
            os.set_blocking(master, False)
            error_read, error_write = os.pipe()
            os.set_blocking(error_read, False)
            loop = asyncio.get_running_loop()
            transport, protocol = await loop.subprocess_exec(
                lambda: asyncio.subprocess.SubprocessStreamProtocol(
                    limit=65536, loop=loop
                ),
                sys.executable,
                "-I",
                "-S",
                str(Path(__file__).with_name("_posix_exec.py")),
                str(error_write),
                *argv,
                cwd=cwd,
                env=env,
                stdin=slave,
                stdout=slave,
                stderr=slave,
                pass_fds=(error_write,),
            )
            handle = PosixProcessHandle(
                asyncio.subprocess.Process(transport, protocol, loop),
                transport,
                request.kill_tree,
                terminal=master,
            )
            os.close(error_write)
            error_write = None
            failure = bytearray()
            while data := await _read_fd(error_read):
                failure.extend(data)
            if failure:
                code, _, message = failure.partition(b"\n")
                raise OSError(int(code), message.decode("utf-8", errors="replace"))
            return handle
        except BaseException:
            if handle is None:
                os.close(master)
            else:
                try:
                    await handle.terminate(force=True)
                    await handle.wait()
                finally:
                    await handle.close()
            raise
        finally:
            os.close(slave)
            if error_read is not None:
                os.close(error_read)
            if error_write is not None:
                os.close(error_write)
