"""Bounded, task-correlated diagnostic JSONL storage independent of saga history."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from enum import IntEnum
import json
import os
from pathlib import Path
import re
import stat
import typing
from uuid import uuid4

import pydantic


class Level(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class LogConfig(Model):
    level: typing.Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    max_bytes: int = pydantic.Field(default=8 * 1024 * 1024, ge=1024, strict=True)
    max_files: int = pydantic.Field(default=5, ge=1, le=1000, strict=True)
    queue_size: int = pydantic.Field(default=1024, ge=1, le=100000, strict=True)
    max_record_bytes: int = pydantic.Field(default=16384, ge=512, strict=True)


class Correlation(Model):
    execution_id: typing.Optional[str] = None
    workflow_id: typing.Optional[str] = None
    step_id: typing.Optional[str] = None


class LogRecord(Correlation):
    version: typing.Literal[1] = 1
    timestamp: datetime
    level: str
    event: str
    fields: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


class LogFile(Model):
    name: str
    size: int


class LogListing(Model):
    files: list[LogFile]
    dropped: int
    write_errors: int


class LogDownload(Model):
    file: str = pydantic.Field(pattern=r"^[0-9a-f]{32}\.jsonl$")
    offset: int = pydantic.Field(default=0, ge=0, strict=True)
    limit: int = pydantic.Field(default=65536, ge=1, le=262144, strict=True)
    end: typing.Optional[int] = pydantic.Field(default=None, ge=0, strict=True)


class LogChunk(Model):
    file: str
    content_base64: str
    next_offset: int
    end: int
    eof: bool


_OWNER: ContextVar[typing.Optional[DiagnosticLog]] = ContextVar(
    "diagnostic_log", default=None
)
_CORRELATION: ContextVar[Correlation] = ContextVar(
    "log_correlation", default=Correlation()
)
_SENSITIVE: typing.Final = re.compile(
    r"password|passwd|secret|token|authorization|cookie|api.?key|credential|private.?key",
    re.IGNORECASE,
)
_BEARER: typing.Final = re.compile(r"\bBearer\s+[^\s,;]+", re.IGNORECASE)
_ASSIGNMENT: typing.Final = re.compile(
    r"\b(password|passwd|secret|token|api[_-]?key)\s*[:=]\s*[^\s,;]+",
    re.IGNORECASE,
)


def _redact(value: pydantic.JsonValue) -> pydantic.JsonValue:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _SENSITIVE.search(key) else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return _ASSIGNMENT.sub(
            r"\1=[REDACTED]", _BEARER.sub("Bearer [REDACTED]", value)
        )
    return value


@contextmanager
def bind(
    *,
    execution_id: typing.Optional[str] = None,
    workflow_id: typing.Optional[str] = None,
    step_id: typing.Optional[str] = None,
) -> Iterator[None]:
    """Inherit execution identity while resetting narrower scopes explicitly."""
    previous = _CORRELATION.get()
    token = _CORRELATION.set(
        Correlation(
            execution_id=(
                execution_id if execution_id is not None else previous.execution_id
            ),
            workflow_id=(
                workflow_id if workflow_id is not None else previous.workflow_id
            ),
            step_id=step_id,
        )
    )
    try:
        yield
    finally:
        _CORRELATION.reset(token)


def log(
    event: str,
    *,
    level: Level = Level.INFO,
    fields: typing.Optional[dict[str, pydantic.JsonValue]] = None,
    error: typing.Optional[BaseException] = None,
) -> None:
    owner = _OWNER.get()
    if owner is not None:
        owner.emit(event, level=level, fields=fields, error=error)


class DiagnosticLog:
    def __init__(
        self, data_path: Path, config: typing.Optional[LogConfig] = None
    ) -> None:
        self.directory = data_path / "logs"
        self.config = config or LogConfig()
        self.dropped = 0
        self.write_errors = 0
        self._queue: asyncio.Queue[typing.Optional[bytes]] = asyncio.Queue(
            self.config.queue_size
        )
        self._worker: typing.Optional[asyncio.Task[None]] = None
        self._close_task: typing.Optional[asyncio.Task[None]] = None
        self._closing = False
        self._io_lock = asyncio.Lock()
        self._active: typing.Optional[Path] = None
        self._size = 0

    @contextmanager
    def activate(self) -> Iterator[None]:
        token = _OWNER.set(self)
        try:
            yield
        finally:
            _OWNER.reset(token)

    async def __aenter__(self) -> typing.Self:
        await asyncio.to_thread(self._prepare)
        self._worker = asyncio.create_task(self._write_loop())
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    def _prepare(self) -> None:
        if not self.directory.resolve().is_relative_to(self.directory.parent.resolve()):
            raise ValueError("log directory escapes project data")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._prune()

    def emit(
        self,
        event: str,
        *,
        level: Level = Level.INFO,
        fields: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        error: typing.Optional[BaseException] = None,
    ) -> None:
        """Snapshot and sanitize before enqueueing; never wait for disk or capacity."""
        if level < Level[self.config.level]:
            return
        if self._closing or self._worker is None or self._queue.full():
            self.dropped += 1
            return
        try:
            details = dict(fields or {})
            if error is not None:
                details["error_type"] = type(error).__name__
                frames: list[pydantic.JsonValue] = []
                traceback = error.__traceback__
                while traceback is not None and len(frames) < 32:
                    frames.append(
                        {
                            "file": Path(traceback.tb_frame.f_code.co_filename).name,
                            "function": traceback.tb_frame.f_code.co_name,
                            "line": traceback.tb_lineno,
                        }
                    )
                    traceback = traceback.tb_next
                details["frames"] = frames
            record = LogRecord(
                **_CORRELATION.get().model_dump(),
                timestamp=datetime.now(timezone.utc),
                level=level.name,
                event=event,
                fields=details,
            )
            encoded = (
                json.dumps(_redact(record.model_dump(mode="json")), allow_nan=False)
                + "\n"
            ).encode("utf-8")
            if len(encoded) > min(self.config.max_bytes, self.config.max_record_bytes):
                self.dropped += 1
                return
            self._queue.put_nowait(encoded)
        except (ValueError, TypeError, RecursionError, asyncio.QueueFull):
            self.dropped += 1

    async def _write_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                async with self._io_lock:
                    try:
                        await asyncio.to_thread(self._append, item)
                    except OSError:
                        self.write_errors += 1
                        self.dropped += 1
                        self._active = None
                        self._size = 0
            finally:
                self._queue.task_done()

    def _files(self) -> list[Path]:
        return sorted(
            (
                path
                for path in self.directory.glob("*.jsonl")
                if re.fullmatch(r"[0-9a-f]{32}\.jsonl", path.name)
                and not path.is_symlink()
                and path.is_file()
            ),
            key=lambda path: (path.stat().st_mtime_ns, path.name),
        )

    def _prune(self) -> None:
        files = self._files()
        removable = [path for path in files if path != self._active]
        for path in removable[: max(0, len(files) - self.config.max_files)]:
            path.unlink()

    def _append(self, content: bytes) -> None:
        if self._active is None or self._size + len(content) > self.config.max_bytes:
            self._active = self.directory / (uuid4().hex + ".jsonl")
            descriptor = os.open(
                self._active, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
            os.close(descriptor)
            self._size = 0
            self._prune()
        with self._active.open("ab") as stream:
            stream.write(content)
        self._size += len(content)

    async def flush(self) -> None:
        await self._queue.join()

    async def aclose(self) -> None:
        if self._worker is None:
            return
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._finish())
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            await asyncio.shield(self._close_task)
            raise

    async def _finish(self) -> None:
        await self._queue.put(None)
        assert self._worker is not None
        await self._worker

    async def listing(self) -> LogListing:
        await self.flush()
        async with self._io_lock:
            files = await asyncio.to_thread(
                lambda: [
                    LogFile(name=path.name, size=path.stat().st_size)
                    for path in self._files()
                ]
            )
        return LogListing(
            files=files, dropped=self.dropped, write_errors=self.write_errors
        )

    async def download(self, query: LogDownload) -> LogChunk:
        """Read bounded raw bytes; an explicit end pins the append-only file boundary."""

        def read() -> LogChunk:
            path = self.directory / query.file
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as stream:
                metadata = os.fstat(stream.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise ValueError("log download requires a regular file")
                size = metadata.st_size
                end = size if query.end is None else query.end
                if not query.offset <= end <= size:
                    raise ValueError("invalid log download range")
                stream.seek(query.offset)
                content = stream.read(min(query.limit, end - query.offset))
            offset = query.offset + len(content)
            return LogChunk(
                file=query.file,
                content_base64=base64.b64encode(content).decode("ascii"),
                next_offset=offset,
                end=end,
                eof=offset == end,
            )

        async with self._io_lock:
            return await asyncio.to_thread(read)
