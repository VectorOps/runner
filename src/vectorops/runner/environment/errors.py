"""Backend-independent failure categories and mutation outcome uncertainty."""

from enum import Enum


class ErrorCode(str, Enum):
    MISSING = "missing"
    DENIED = "denied"
    OUTSIDE_ROOT = "outside_root"
    INVALID_PATH = "invalid_path"
    CONFLICT = "conflict"
    UNSUPPORTED = "unsupported"
    RESOURCE_LIMIT = "resource_limit"
    INVALID_TEXT = "invalid_text"
    IO = "io"


class EnvironmentError(RuntimeError):
    def __init__(
        self, code: ErrorCode, message: str, *, may_have_occurred: bool = False
    ) -> None:
        super().__init__(message)
        self.code = code
        self.may_have_occurred = may_have_occurred
