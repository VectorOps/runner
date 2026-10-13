"""Async application of format-owned plans through target environment contracts."""

import asyncio
from pathlib import PurePosixPath
from typing import Optional

from vectorops.runner.environment import models as filesystem
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.protocol import TargetEnvironment

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
        self, environment: TargetEnvironment, *, max_patch_bytes: int = 1024 * 1024
    ) -> None:
        if max_patch_bytes < 1:
            raise ValueError("max_patch_bytes must be positive")
        self._environment = environment
        self._max_patch_bytes = max_patch_bytes
        self._lock = asyncio.Lock()

    async def _read(
        self, path: str, description: filesystem.EnvironmentDescription
    ) -> Optional[bytes]:
        try:
            info = await self._environment.stat(filesystem.PathRequest(path=path))
        except EnvironmentError as exc:
            if exc.code == ErrorCode.MISSING:
                return None
            raise
        if info.kind != "file":
            raise common.PatchFailure("Only regular files can be patched")
        if info.size > description.max_write_bytes:
            raise common.PatchFailure("File exceeds patch size limit")
        content = bytearray()
        while True:
            limit = min(
                description.max_read_bytes,
                description.max_write_bytes - len(content) + 1,
            )
            part = await self._environment.read_file(
                filesystem.ReadRequest(path=path, offset=len(content), limit=limit)
            )
            content.extend(part.content)
            if len(content) > description.max_write_bytes:
                raise common.PatchFailure("File exceeds patch size limit")
            if part.eof:
                break
            if not part.content:
                raise common.PatchFailure("Incomplete file read made no progress")
        return bytes(content)

    async def apply(
        self, text: str, *, format: str = "v4a", reverse: bool = False
    ) -> models.PatchResult:
        """Serialize calls on this service, without claiming cross-process isolation."""
        get_system_instruction(format)
        if len(text.encode("utf-8")) > self._max_patch_bytes:
            return models.PatchResult(
                errors=[models.PatchError(msg="Patch exceeds size limit")]
            )
        operations: list[common.FileOperation]
        if format.lower() == "v4a":
            parsed_v4a, errors = v4a.parse(text, reverse=reverse)
            operations = list(parsed_v4a)
        else:
            parsed_search, errors = search_replace.parse(text, reverse=reverse)
            operations = list(parsed_search)
        async with self._lock:
            return await self._apply(operations, errors)

    async def _apply(
        self, operations: list[common.FileOperation], errors: list[models.PatchError]
    ) -> models.PatchResult:
        """Validate each complete file before mutation; report only completed I/O."""
        changes: list[models.AppliedChange] = []
        if not operations:
            return models.PatchResult(errors=errors)
        try:
            description = await self._environment.describe()
            if (
                not description.capabilities.filesystem
                or not description.capabilities.atomic_file_replacement
            ):
                raise common.PatchFailure("Patching requires atomic file replacement")
        except (EnvironmentError, common.PatchFailure) as exc:
            errors.append(models.PatchError(msg=str(exc)))
            return models.PatchResult(errors=errors)
        counts: dict[str, int] = {}
        for operation in operations:
            for path in (operation.path, operation.destination):
                if path is not None:
                    counts[path] = counts.get(path, 0) + 1
        for operation in operations:
            try:
                if any(
                    counts[path] > 1
                    for path in (operation.path, operation.destination)
                    if path is not None
                ):
                    raise common.PatchFailure(
                        "Conflicting file operations or move destinations"
                    )
                content = await self._read(operation.path, description)
                change = operation.plan(content)
                if change.destination is not None:
                    try:
                        await self._environment.stat(
                            filesystem.PathRequest(path=change.destination)
                        )
                    except EnvironmentError as exc:
                        if exc.code != ErrorCode.MISSING:
                            raise
                    else:
                        raise common.PatchFailure("Move destination already exists")
                if change.content is None:
                    await self._environment.remove(
                        filesystem.RemoveRequest(path=change.path)
                    )
                    changes.append(
                        models.AppliedChange(path=change.path, kind="deleted")
                    )
                    continue
                encoded = (
                    change.content.encode("utf-8")
                    if isinstance(change.content, str)
                    else change.content
                )
                if len(encoded) > description.max_write_bytes:
                    raise common.PatchFailure("Replacement exceeds write size limit")
                if encoded == content and change.destination is None:
                    continue
                destination = change.destination or change.path
                parent = str(PurePosixPath(destination).parent)
                if parent != ".":
                    await self._environment.mkdir(
                        filesystem.MkdirRequest(
                            path=parent, parents=True, exist_ok=True
                        )
                    )
                creating = content is None or change.destination is not None
                await self._environment.write_file(
                    filesystem.WriteRequest(
                        path=destination,
                        content=encoded,
                        mode="create" if creating else "replace",
                    )
                )
                changes.append(
                    models.AppliedChange(
                        path=destination, kind="created" if creating else "updated"
                    )
                )
                if change.destination is not None:
                    await self._environment.remove(
                        filesystem.RemoveRequest(path=change.path)
                    )
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
            except EnvironmentError as exc:
                errors.append(
                    models.PatchError(
                        msg=f"{exc.code.value}: {exc}",
                        filename=operation.path,
                        chunk=operation.source,
                        may_have_occurred=exc.may_have_occurred,
                    )
                )
        return models.PatchResult(changes=changes, errors=errors)
