"""POSIX filesystem I/O with basic resolved-path validation, not a sandbox."""

import asyncio
from collections.abc import Callable
import errno
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tempfile
import typing

from . import models as m, text
from .configuration import LocalEnvironmentConfig
from .errors import EnvironmentError, ErrorCode


class LocalEnvironment:
    def __init__(self, root: Path, config: LocalEnvironmentConfig) -> None:
        self._root = root
        self._config = config

    @classmethod
    async def open(cls, config: LocalEnvironmentConfig) -> typing.Self:
        def attach() -> Path:
            if os.name != "posix":
                raise EnvironmentError(
                    ErrorCode.UNSUPPORTED, "local backend requires POSIX"
                )
            root = Path(config.base_project_path).resolve(strict=True)
            if not root.is_dir():
                raise NotADirectoryError(
                    errno.ENOTDIR, "target is not a directory", str(root)
                )
            return root

        return cls(await cls._run(attach), config)

    @staticmethod
    async def _run[T](operation: Callable[[], T], *, mutation: bool = False) -> T:
        try:
            return await asyncio.to_thread(operation)
        except OSError as exc:
            code = ErrorCode.IO
            if exc.errno == errno.ENOENT:
                code = ErrorCode.MISSING
            elif exc.errno in (errno.EACCES, errno.EPERM):
                code = ErrorCode.DENIED
            elif exc.errno in (
                errno.EEXIST,
                errno.ENOTEMPTY,
                errno.EISDIR,
                errno.ENOTDIR,
            ):
                code = ErrorCode.CONFLICT
            elif exc.errno in (errno.ENOSPC, errno.EDQUOT, errno.EFBIG):
                code = ErrorCode.RESOURCE_LIMIT
            elif exc.errno in (errno.EXDEV, errno.ENOTSUP):
                code = ErrorCode.UNSUPPORTED
            raise EnvironmentError(code, str(exc), may_have_occurred=mutation) from exc

    def _path(self, value: str, *, follow_final: bool = True) -> Path:
        path = PurePosixPath(value)
        if not value or "\0" in value:
            raise EnvironmentError(
                ErrorCode.INVALID_PATH, "path must be nonempty without NULs"
            )
        if path.is_absolute() or ".." in path.parts:
            raise EnvironmentError(
                ErrorCode.OUTSIDE_ROOT, "path must be relative without '..'"
            )
        candidate = self._root.joinpath(*path.parts)
        resolved = (
            candidate.resolve()
            if follow_final or candidate == self._root
            else candidate.parent.resolve() / candidate.name
        )
        if not resolved.is_relative_to(self._root):
            raise EnvironmentError(
                ErrorCode.OUTSIDE_ROOT, "path resolves outside target root"
            )
        return resolved

    def _mutation_path(self, value: str) -> Path:
        path = self._path(value, follow_final=False)
        if path == self._root:
            raise EnvironmentError(ErrorCode.DENIED, "cannot mutate the target root")
        return path

    @staticmethod
    def _limit(value: int, maximum: int) -> None:
        if value > maximum:
            raise EnvironmentError(
                ErrorCode.RESOURCE_LIMIT, f"requested limit exceeds {maximum}"
            )

    @staticmethod
    def _info(path: str, metadata: os.stat_result) -> m.FileInfo:
        kind: typing.Literal["file", "directory", "symlink", "other"] = "other"
        if stat.S_ISREG(metadata.st_mode):
            kind = "file"
        elif stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
        elif stat.S_ISLNK(metadata.st_mode):
            kind = "symlink"
        return m.FileInfo(
            path=path,
            kind=kind,
            size=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
        )

    async def describe(self) -> m.EnvironmentDescription:
        return m.EnvironmentDescription(
            identity=self._root.as_uri(),
            platform=sys.platform,
            max_read_bytes=self._config.max_read_bytes,
            max_write_bytes=self._config.max_write_bytes,
            max_list_entries=self._config.max_list_entries,
        )

    async def stat(self, request: m.PathRequest) -> m.FileInfo:
        return await self._run(
            lambda: self._info(
                str(PurePosixPath(request.path)),
                self._path(request.path, follow_final=False).lstat(),
            )
        )

    async def list_dir(self, request: m.ListRequest) -> m.ListResult:
        self._limit(request.limit, self._config.max_list_entries)

        def listing() -> m.ListResult:
            entries: list[m.FileInfo] = []
            with os.scandir(self._path(request.path)) as iterator:
                for entry in iterator:
                    if len(entries) == request.limit:
                        return m.ListResult(entries=entries, truncated=True)
                    entries.append(
                        self._info(
                            str(PurePosixPath(request.path) / entry.name),
                            entry.stat(follow_symlinks=False),
                        )
                    )
            return m.ListResult(entries=entries, truncated=False)

        return await self._run(listing)

    async def read_file(self, request: m.ReadRequest) -> m.ReadResult:
        self._limit(request.limit, self._config.max_read_bytes)

        def read() -> m.ReadResult:
            path = self._path(request.path)
            descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise EnvironmentError(
                        ErrorCode.UNSUPPORTED, "only regular files can be read"
                    )
                stream.seek(request.offset)
                content = stream.read(request.limit + 1)
                return m.ReadResult(
                    content=content[: request.limit], eof=len(content) <= request.limit
                )

        return await self._run(read)

    async def read_lines(self, request: m.LineReadRequest) -> m.LineReadResult:
        self._limit(request.byte_limit, self._config.max_read_bytes)
        return await text.read_lines(
            self, request, chunk_size=min(65536, self._config.max_read_bytes)
        )

    async def write_file(self, request: m.WriteRequest) -> None:
        self._limit(len(request.content), self._config.max_write_bytes)

        def write() -> None:
            path = self._mutation_path(request.path)
            mode = 0o600
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                if request.mode == "replace":
                    raise
            else:
                if request.mode == "create":
                    raise FileExistsError(errno.EEXIST, "destination exists", str(path))
                if not stat.S_ISREG(metadata.st_mode):
                    raise EnvironmentError(
                        ErrorCode.CONFLICT, "only regular files can be replaced"
                    )
                mode = stat.S_IMODE(metadata.st_mode)
            descriptor, name = tempfile.mkstemp(prefix=".target-", dir=path.parent)
            temporary = Path(name)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(request.content)
                    os.fchmod(stream.fileno(), mode)
                if request.mode == "create":
                    os.link(temporary, path)
                else:
                    os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)

        await self._run(write, mutation=True)

    async def mkdir(self, request: m.MkdirRequest) -> None:
        def mkdir() -> None:
            self._mutation_path(request.path)
            path = self._path(request.path)
            if path == self._root:
                raise EnvironmentError(
                    ErrorCode.DENIED, "cannot mutate the target root"
                )
            path.mkdir(parents=request.parents, exist_ok=request.exist_ok)

        await self._run(mkdir, mutation=True)

    async def remove(self, request: m.RemoveRequest) -> None:
        def remove() -> None:
            path = self._mutation_path(request.path)
            metadata = path.lstat()
            if stat.S_ISDIR(metadata.st_mode):
                if request.recursive:
                    shutil.rmtree(path)
                else:
                    path.rmdir()
            else:
                path.unlink()

        await self._run(remove, mutation=True)

    async def rename(self, request: m.RenameRequest) -> None:
        def rename() -> None:
            source = self._mutation_path(request.path)
            destination = self._mutation_path(request.destination)
            if not request.overwrite and os.path.lexists(destination):
                raise FileExistsError(
                    errno.EEXIST, "destination exists", str(destination)
                )
            source.rename(destination)

        await self._run(rename, mutation=True)
