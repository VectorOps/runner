"""Shared text handling and the format-owned file operation interface."""

from pathlib import PurePosixPath
import re
import typing
from typing import Optional, Protocol

from .models import FileChange


class PatchFailure(ValueError):
    def __init__(self, message: str, *, chunk: Optional[str] = None) -> None:
        super().__init__(message)
        self.chunk = chunk


class FileOperation(Protocol):
    @property
    def path(self) -> str: ...

    @property
    def destination(self) -> Optional[str]: ...

    @property
    def source(self) -> str: ...

    def plan(self, content: Optional[typing.Union[str, bytes]]) -> FileChange: ...


def decode_text(content: typing.Union[str, bytes]) -> str:
    if isinstance(content, str):
        return content
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PatchFailure("File is not valid UTF-8") from exc


def validate_path(path: str) -> str:
    if (
        not path
        or "\0" in path
        or "\\" in path
        or re.match(r"^[A-Za-z]:", path)
        or str(PurePosixPath(path)) in (".", "/", "~")
    ):
        raise PatchFailure(f"Expected a file path: {path!r}")
    return str(PurePosixPath(path))


def split_text(content: str) -> tuple[list[str], str, bool]:
    newline = "\r\n" if "\r\n" in content else "\n"
    if "\r" in content.replace("\r\n", "") or (
        newline == "\r\n" and "\n" in content.replace("\r\n", "")
    ):
        raise PatchFailure("Mixed or unsupported line endings")
    normalized = content.replace("\r\n", "\n")
    if not normalized:
        return [], newline, False
    trailing = normalized.endswith("\n")
    lines = normalized.split("\n")
    if trailing:
        lines.pop()
    return lines, newline, trailing


def join_text(lines: list[str], newline: str, trailing: bool) -> str:
    return newline.join(lines) + (newline if trailing and lines else "")
