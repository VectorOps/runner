"""Local path validation and regular-file safety shared by project operations."""

import os
from pathlib import Path, PurePosixPath
import stat
from typing import BinaryIO, Literal

from .errors import EnvironmentError, ErrorCode


def resolve_path(
    root: Path,
    value: str,
    *,
    follow_final: bool = True,
    mutation: bool = False,
    allow_external: bool = False,
) -> Path:
    path = PurePosixPath(value)
    if not value or "\0" in value:
        raise EnvironmentError(
            ErrorCode.INVALID_PATH, "path must be nonempty without NULs"
        )
    if not allow_external and (path.is_absolute() or ".." in path.parts):
        raise EnvironmentError(
            ErrorCode.OUTSIDE_ROOT, "path must be relative without '..'"
        )
    candidate = (
        root / Path(value).expanduser()
        if allow_external
        else root.joinpath(*path.parts)
    )
    resolved = (
        candidate.resolve()
        if follow_final or candidate == root
        else candidate.parent.resolve() / candidate.name
    )
    if not allow_external and not resolved.is_relative_to(root):
        raise EnvironmentError(
            ErrorCode.OUTSIDE_ROOT, "path resolves outside target root"
        )
    if mutation and resolved == root:
        raise EnvironmentError(ErrorCode.DENIED, "cannot mutate the target root")
    return resolved


def path_scope(root: Path, value: str) -> Literal["internal", "external"]:
    resolved = resolve_path(root, value, allow_external=True)
    return "internal" if resolved.is_relative_to(root.resolve()) else "external"


def check_limit(value: int, maximum: int) -> None:
    if value > maximum:
        raise EnvironmentError(
            ErrorCode.RESOURCE_LIMIT, f"requested limit exceeds {maximum}"
        )


def open_regular(path: Path) -> BinaryIO:
    """Reject special files without blocking on a FIFO during open."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise EnvironmentError(
                ErrorCode.UNSUPPORTED, "only regular files can be read"
            )
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise
