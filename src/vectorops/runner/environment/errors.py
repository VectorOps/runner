"""Backend-independent failure categories and mutation outcome uncertainty."""

from enum import Enum
import errno


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

    @classmethod
    def from_os_error(
        cls, error: OSError, *, mutation: bool = False
    ) -> "EnvironmentError":
        code = ErrorCode.IO
        if error.errno == errno.ENOENT:
            code = ErrorCode.MISSING
        elif error.errno in (errno.EACCES, errno.EPERM):
            code = ErrorCode.DENIED
        elif error.errno in (
            errno.EEXIST,
            errno.ENOTEMPTY,
            errno.EISDIR,
            errno.ENOTDIR,
        ):
            code = ErrorCode.CONFLICT
        elif error.errno in (errno.ENOSPC, errno.EDQUOT, errno.EFBIG):
            code = ErrorCode.RESOURCE_LIMIT
        elif error.errno in (errno.EXDEV, errno.ENOTSUP):
            code = ErrorCode.UNSUPPORTED
        return cls(code, str(error), may_have_occurred=mutation)
