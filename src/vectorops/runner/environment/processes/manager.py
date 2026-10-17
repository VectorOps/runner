"""Environment-owned process sessions independent of request and workflow lifetimes."""

import asyncio
from dataclasses import dataclass, field
from typing import Literal, Optional
from uuid import uuid4

from ..errors import EnvironmentError, ErrorCode
from . import models as m
from .local import LocalProcessDriver, LocalProcessHandle
from .output import OutputBuffer


@dataclass
class Session:
    id: str
    buffer: OutputBuffer
    kill_tree: bool = True
    state: Literal["starting", "running", "exited", "failed", "unknown"] = "starting"
    handle: Optional[LocalProcessHandle] = None
    task: Optional[asyncio.Task[None]] = None
    stop_task: Optional[asyncio.Task[m.KillResult]] = None
    input_task: Optional[asyncio.Task[None]] = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    input_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    exit_result: Optional[m.ExitResult] = None
    error: Optional[str] = None
    timed_out: bool = False
    stdin_closed: bool = False
    input_pending: bool = False
    expiry: Optional[asyncio.TimerHandle] = None


class ProcessManager:
    def __init__(self, driver: LocalProcessDriver, config: m.ProcessConfig) -> None:
        self._driver = driver
        self._config = config.model_copy(deep=True)
        self._sessions: dict[str, Session] = {}
        self._operations: set[asyncio.Task[None]] = set()
        self._closed = False
        self._close_task: Optional[asyncio.Task[list[m.KillResult]]] = None

    def _get(self, identifier: str) -> Session:
        try:
            return self._sessions[identifier]
        except KeyError as exc:
            raise EnvironmentError(
                ErrorCode.MISSING,
                f"unknown or expired process ID {identifier!r}; "
                "process sessions do not survive environment restarts. "
                "This session cannot be continued. Start a new process with exec "
                "and use its new process ID.",
            ) from exc

    async def exec(self, request: m.ExecRequest) -> m.ProcessResult:
        if self._closed:
            raise EnvironmentError(ErrorCode.DENIED, "process manager is closed")
        if request.mode == "pty" and not self._driver.capabilities.pty:
            raise EnvironmentError(
                ErrorCode.UNSUPPORTED, "PTY execution is unsupported"
            )
        if isinstance(request.command, m.ShellCommand) and not self._config.allow_shell:
            raise EnvironmentError(ErrorCode.DENIED, "shell execution is disabled")
        if len(request.stdin) > self._config.max_stdin_bytes:
            raise EnvironmentError(
                ErrorCode.RESOURCE_LIMIT, "initial stdin exceeds limit"
            )
        if not request.keep_stdin_open and not self._driver.capabilities.stdin_eof:
            raise EnvironmentError(ErrorCode.UNSUPPORTED, "stdin EOF is unsupported")
        if (
            request.execution_timeout_ms is not None
            and self._driver.capabilities.termination_scope == "none"
        ):
            raise EnvironmentError(
                ErrorCode.UNSUPPORTED, "execution deadlines require termination support"
            )
        if len(self._sessions) >= self._config.max_sessions:
            raise EnvironmentError(
                ErrorCode.RESOURCE_LIMIT, "process session limit reached"
            )
        session = Session(uuid4().hex, OutputBuffer(self._config.max_output_bytes))
        session.kill_tree = request.kill_tree
        self._sessions[session.id] = session
        session.task = asyncio.create_task(
            self._run(session, request.model_copy(deep=True))
        )
        if request.yield_timeout_ms:
            try:
                await asyncio.wait_for(
                    session.done.wait(), request.yield_timeout_ms / 1000
                )
            except TimeoutError:
                pass
        return self._result(session, 0)

    async def _collect(self, session: Session, handle: LocalProcessHandle) -> None:
        async for chunk in handle.read_output():
            session.buffer.append(chunk)
            session.changed.set()

    async def _input(self, session: Session, data: bytes, eof: bool) -> None:
        async with session.input_lock:
            if session.handle is None or session.stdin_closed or session.done.is_set():
                raise EnvironmentError(
                    ErrorCode.CONFLICT, "process stdin is unavailable"
                )
            async with asyncio.timeout(self._config.io_timeout_seconds):
                if data:
                    await session.handle.write_stdin(data)
                if eof:
                    await session.handle.close_stdin()
                    session.stdin_closed = True

    async def _initial_input(self, session: Session, request: m.ExecRequest) -> None:
        try:
            await self._input(session, request.stdin, not request.keep_stdin_open)
        except (BrokenPipeError, ConnectionResetError):
            session.stdin_closed = True

    async def _deadline(self, session: Session, milliseconds: int) -> None:
        await asyncio.sleep(milliseconds / 1000)
        if not session.done.is_set():
            session.timed_out = True
            await self._stop(session)

    async def _run(self, session: Session, request: m.ExecRequest) -> None:
        """Own spawn, I/O, exit monitoring, and cleanup beyond any caller's await."""
        tasks: list[asyncio.Task[object]] = []
        timer: Optional[asyncio.Task[None]] = None
        try:
            async with asyncio.timeout(self._config.io_timeout_seconds):
                handle = await self._driver.spawn(request)
            session.handle = handle
            session.state = "running"
            session.ready.set()
            session.changed.set()
            output = asyncio.create_task(self._collect(session, handle))
            initial = asyncio.create_task(self._initial_input(session, request))
            waiter = asyncio.create_task(handle.wait())
            tasks.extend([output, initial, waiter])
            if request.execution_timeout_ms is not None:
                timer = asyncio.create_task(
                    self._deadline(session, request.execution_timeout_ms)
                )
            watched: set[asyncio.Task[object]] = set(tasks)
            while not waiter.done():
                finished, _ = await asyncio.wait(
                    watched, return_when=asyncio.FIRST_COMPLETED
                )
                for task in finished:
                    task.result()
                    watched.discard(task)
            session.exit_result = waiter.result()
            if (
                session.kill_tree
                and self._driver.capabilities.termination_scope == "group"
            ):
                async with asyncio.timeout(self._config.io_timeout_seconds):
                    await handle.terminate(force=True)
            initial.cancel()
            await asyncio.gather(initial, return_exceptions=True)
            try:
                await asyncio.wait_for(
                    asyncio.shield(output), self._config.io_timeout_seconds
                )
            except TimeoutError:
                session.error = "output streams did not close after process exit"
                if self._driver.capabilities.termination_scope != "none":
                    async with asyncio.timeout(self._config.io_timeout_seconds):
                        await handle.terminate(force=True)
            session.state = (
                "exited" if session.exit_result.outcome == "exited" else "unknown"
            )
        except asyncio.CancelledError:
            session.state = "unknown"
            session.error = "session closed before its outcome was confirmed"
            raise
        except Exception as exc:
            session.error = str(exc)
            uncertain_spawn = (
                isinstance(exc, EnvironmentError) and exc.may_have_occurred
            )
            session.state = (
                "failed"
                if session.handle is None and not uncertain_spawn
                else "unknown"
            )
            if (
                session.handle is not None
                and self._driver.capabilities.termination_scope != "none"
            ):
                try:
                    async with asyncio.timeout(self._config.io_timeout_seconds):
                        await session.handle.terminate(force=True)
                        await session.handle.wait()
                except Exception:
                    session.error += "; termination could not be confirmed"
        finally:
            if timer is not None:
                timer.cancel()
                await asyncio.gather(timer, return_exceptions=True)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if session.input_task is not None and not session.input_task.done():
                session.input_task.cancel()
                await asyncio.gather(session.input_task, return_exceptions=True)
            if session.handle is not None:
                try:
                    async with asyncio.timeout(self._config.io_timeout_seconds):
                        await session.handle.close()
                except Exception as exc:
                    session.error = f"{session.error or ''} cleanup failed: {exc}"
            session.ready.set()
            session.done.set()
            session.changed.set()
            session.expiry = asyncio.get_running_loop().call_later(
                self._config.retention_seconds, self._release, session
            )

    def _release(self, session: Session) -> None:
        if session.expiry is not None:
            session.expiry.cancel()
        self._sessions.pop(session.id, None)

    def _result(
        self, session: Session, cursor: int, *, release: bool = True
    ) -> m.ProcessResult:
        chunks, next_cursor, dropped = session.buffer.read(
            cursor, self._config.max_response_bytes
        )
        complete = session.done.is_set() and next_cursor == session.buffer.end
        retained = not (complete and release)
        result = m.ProcessResult(
            process_id=session.id,
            state=session.state,
            chunks=chunks,
            cursor=next_cursor,
            dropped_bytes=dropped,
            output_complete=complete,
            retained=retained,
            returncode=(
                session.exit_result.returncode
                if session.exit_result is not None
                else None
            ),
            error=session.error,
            timed_out=session.timed_out,
        )
        if not retained:
            self._release(session)
        return result

    async def poll(self, request: m.PollRequest) -> m.ProcessResult:
        session = self._get(request.process_id)
        if request.cursor > session.buffer.end:
            raise EnvironmentError(
                ErrorCode.CONFLICT, "cursor is beyond collected output"
            )
        if (
            request.cursor == session.buffer.end
            and not session.done.is_set()
            and request.wait_timeout_ms
        ):
            session.changed.clear()
            try:
                await asyncio.wait_for(
                    session.changed.wait(), request.wait_timeout_ms / 1000
                )
            except TimeoutError:
                pass
        return self._result(session, request.cursor)

    async def list_processes(self) -> list[m.ProcessResult]:
        return [
            self._result(session, session.buffer.end, release=False)
            for session in self._sessions.values()
        ]

    async def write_stdin(self, request: m.StdinRequest) -> None:
        if self._closed:
            raise EnvironmentError(ErrorCode.DENIED, "process manager is closed")
        session = self._get(request.process_id)
        if len(request.data) > self._config.max_stdin_bytes:
            raise EnvironmentError(ErrorCode.RESOURCE_LIMIT, "stdin exceeds limit")
        if request.eof and not self._driver.capabilities.stdin_eof:
            raise EnvironmentError(ErrorCode.UNSUPPORTED, "stdin EOF is unsupported")
        if session.input_pending:
            raise EnvironmentError(
                ErrorCode.CONFLICT, "stdin write already in progress"
            )
        session.input_pending = True

        async def send() -> None:
            try:
                async with asyncio.timeout(self._config.io_timeout_seconds):
                    await session.ready.wait()
                    await self._input(session, request.data, request.eof)
            except EnvironmentError:
                raise
            except asyncio.CancelledError as exc:
                raise EnvironmentError(
                    ErrorCode.IO,
                    "session ended during stdin delivery",
                    may_have_occurred=True,
                ) from exc
            except (OSError, TimeoutError) as exc:
                raise EnvironmentError(
                    ErrorCode.IO, str(exc), may_have_occurred=True
                ) from exc
            finally:
                session.input_pending = False

        task = asyncio.create_task(send())
        session.input_task = task
        self._operations.add(task)
        task.add_done_callback(self._operation_done)
        await asyncio.shield(task)

    def _operation_done(self, task: asyncio.Task[None]) -> None:
        self._operations.discard(task)
        if not task.cancelled():
            task.exception()

    async def _stop(self, session: Session) -> m.KillResult:
        scope = self._driver.capabilities.termination_scope
        if scope == "group" and not session.kill_tree:
            scope = "process"
        if scope == "none":
            raise EnvironmentError(ErrorCode.UNSUPPORTED, "termination is unsupported")
        try:
            await asyncio.wait_for(
                session.ready.wait(), self._config.io_timeout_seconds
            )
        except TimeoutError:
            return m.KillResult(
                process_id=session.id, termination_confirmed=False, scope=scope
            )
        if session.handle is not None and not session.done.is_set():
            try:
                for force in (False, True):
                    async with asyncio.timeout(self._config.io_timeout_seconds):
                        await session.handle.terminate(force=force)
                    try:
                        await asyncio.wait_for(
                            session.done.wait(), self._config.termination_grace_seconds
                        )
                        break
                    except TimeoutError:
                        pass
            except Exception as exc:
                session.error = f"termination failed: {exc}"
        return m.KillResult(
            process_id=session.id,
            scope=scope,
            termination_confirmed=(
                session.state == "failed"
                or (
                    session.exit_result is not None
                    and session.exit_result.outcome == "exited"
                )
            ),
        )

    async def kill(self, request: m.SessionRequest) -> m.KillResult:
        session = self._get(request.process_id)
        if session.stop_task is None or session.stop_task.done():
            session.stop_task = asyncio.create_task(self._stop(session))
        return await asyncio.shield(session.stop_task)

    async def close(self) -> list[m.KillResult]:
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        return await asyncio.shield(self._close_task)

    async def _close(self) -> list[m.KillResult]:
        results: list[m.KillResult] = []
        for session in list(self._sessions.values()):
            if self._driver.capabilities.termination_scope != "none":
                results.append(await self._stop(session))
            else:
                results.append(
                    m.KillResult(
                        process_id=session.id, termination_confirmed=False, scope="none"
                    )
                )
            if not session.done.is_set() and session.task is not None:
                session.task.cancel()
                await asyncio.gather(session.task, return_exceptions=True)
            self._release(session)
        if self._operations:
            await asyncio.gather(*self._operations, return_exceptions=True)
        return results
