"""Async application of format-owned plans to local project files."""

import asyncio
import errno
import os
from pathlib import Path
import stat
import tempfile
from typing import Literal, Optional

from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment import paths

from . import common, models, v4a
from . import search_replace


def get_supported_formats() -> tuple[str, ...]:
    return ("v4a", "patch")


def get_system_instruction(format: str) -> str:
    normalized = format.lower()
    if normalized == "v4a":
        return v4a.INSTRUCTION
    if normalized == "patch":
        return search_replace.INSTRUCTION
    raise ValueError(f"Unsupported patch format: {format}")


class PatchService:
    def __init__(
        self,
        root: Path,
        *,
        max_patch_bytes: int = 1024 * 1024,
        max_write_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        if max_patch_bytes < 1 or max_write_bytes < 1:
            raise ValueError("patch and write limits must be positive")
        self._root = root
        self._max_write_bytes = max_write_bytes
        self._max_patch_bytes = max_patch_bytes
        self._lock = asyncio.Lock()

    def _read(self, path: str) -> Optional[bytes]:
        local = paths.resolve_path(
            self._root, path, follow_final=False, allow_external=True
        )
        try:
            info = local.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(info.st_mode):
            raise common.PatchFailure("Only regular files can be patched")
        if info.st_size > self._max_write_bytes:
            raise common.PatchFailure("File exceeds patch size limit")
        with paths.open_regular(local) as stream:
            content = stream.read(self._max_write_bytes + 1)
        if len(content) > self._max_write_bytes:
            raise common.PatchFailure("File exceeds patch size limit")
        return content

    def _write(self, destination: str, content: bytes, *, creating: bool) -> None:
        """Publish complete files atomically, without replacing symlinks or move targets."""
        path = paths.resolve_path(
            self._root,
            destination,
            follow_final=False,
            mutation=True,
            allow_external=True,
        )
        mode = 0o600
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            if not creating:
                raise
        else:
            if creating:
                raise FileExistsError(errno.EEXIST, "destination exists", str(path))
            if not stat.S_ISREG(metadata.st_mode):
                raise EnvironmentError(
                    ErrorCode.CONFLICT, "only regular files can be replaced"
                )
            mode = stat.S_IMODE(metadata.st_mode)
        if path.parent != self._root:
            parent = paths.resolve_path(
                self._root, str(Path(destination).parent), allow_external=True
            )
            parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=".target-", dir=path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                os.fchmod(stream.fileno(), mode)
            if creating:
                os.link(temporary, path)
            else:
                os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _remove(self, path: str) -> None:
        paths.resolve_path(
            self._root, path, follow_final=False, mutation=True, allow_external=True
        ).unlink()

    def _check_destination(self, path: str) -> None:
        destination = paths.resolve_path(
            self._root, path, follow_final=False, mutation=True, allow_external=True
        )
        if os.path.lexists(destination):
            raise common.PatchFailure("Move destination already exists")

    def _parse(
        self, text: str, format: str, reverse: bool
    ) -> tuple[list[common.FileOperation], list[models.PatchError]]:
        get_system_instruction(format)
        if len(text.encode("utf-8")) > self._max_patch_bytes:
            return [], [models.PatchError(msg="Patch exceeds size limit")]
        operations: list[common.FileOperation]
        if format.lower() == "v4a":
            parsed_v4a, errors = v4a.parse(text, reverse=reverse)
            operations = list(parsed_v4a)
        else:
            parsed_search, errors = search_replace.parse(text, reverse=reverse)
            operations = list(parsed_search)
        return operations, errors

    async def scope(
        self, text: str, *, format: str = "v4a", reverse: bool = False
    ) -> Literal["internal", "external"]:
        """Classify all executable source and destination paths without reading file contents."""

        def classify() -> Literal["internal", "external"]:
            operations, errors = self._parse(text, format, reverse)
            if any(
                paths.path_scope(self._root, path) == "external"
                for operation in operations
                for path in (operation.path, operation.destination)
                if path is not None
            ):
                return "external"
            return "internal"

        return await asyncio.to_thread(classify)

    async def apply(
        self, text: str, *, format: str = "v4a", reverse: bool = False
    ) -> models.PatchResult:
        """Serialize calls on this service, without claiming cross-process isolation."""
        operations, errors = self._parse(text, format, reverse)
        async with self._lock:
            return await self._apply(operations, errors)

    async def _apply(
        self, operations: list[common.FileOperation], errors: list[models.PatchError]
    ) -> models.PatchResult:
        """Validate each complete file before mutation; report only completed I/O."""
        changes: list[models.AppliedChange] = []
        if not operations:
            return models.PatchResult(errors=errors)
        counts: dict[str, int] = {}
        for operation in operations:
            for path in (operation.path, operation.destination):
                if path is not None:
                    counts[path] = counts.get(path, 0) + 1
        for operation in operations:
            mutation = False
            try:
                if any(
                    counts[path] > 1
                    for path in (operation.path, operation.destination)
                    if path is not None
                ):
                    raise common.PatchFailure(
                        "Conflicting file operations or move destinations"
                    )
                content = await asyncio.to_thread(self._read, operation.path)
                change = operation.plan(content)
                if change.destination is not None:
                    await asyncio.to_thread(self._check_destination, change.destination)
                if change.content is None:
                    mutation = True
                    await asyncio.to_thread(self._remove, change.path)
                    changes.append(
                        models.AppliedChange(path=change.path, kind="deleted")
                    )
                    continue
                encoded = (
                    change.content.encode("utf-8")
                    if isinstance(change.content, str)
                    else change.content
                )
                if len(encoded) > self._max_write_bytes:
                    raise common.PatchFailure("Replacement exceeds write size limit")
                if encoded == content and change.destination is None:
                    continue
                destination = change.destination or change.path
                creating = content is None or change.destination is not None
                mutation = True
                await asyncio.to_thread(
                    self._write, destination, encoded, creating=creating
                )
                changes.append(
                    models.AppliedChange(
                        path=destination, kind="created" if creating else "updated"
                    )
                )
                if change.destination is not None:
                    await asyncio.to_thread(self._remove, change.path)
                    changes.append(
                        models.AppliedChange(path=change.path, kind="deleted")
                    )
            except common.PatchFailure as exc:
                errors.append(
                    models.PatchError(
                        msg=str(exc),
                        filename=operation.path,
                        chunk=exc.chunk or operation.source,
                    )
                )
            except (EnvironmentError, OSError) as exc:
                failure = (
                    exc
                    if isinstance(exc, EnvironmentError)
                    else EnvironmentError.from_os_error(exc, mutation=mutation)
                )
                errors.append(
                    models.PatchError(
                        msg=f"{failure.code.value}: {failure}",
                        filename=operation.path,
                        chunk=operation.source,
                        may_have_occurred=failure.may_have_occurred,
                    )
                )
        return models.PatchResult(changes=changes, errors=errors)
