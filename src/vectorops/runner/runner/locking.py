"""Nonblocking cross-platform filesystem writer leases."""

import os
from pathlib import Path
import sys
import typing

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


class FileLease:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: typing.Optional[int] = None

    def acquire(self) -> None:
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if sys.platform == "win32":
                if os.fstat(descriptor).st_size == 0:
                    os.write(descriptor, b"\0")
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            raise RuntimeError("another runner owns this project") from exc
        self.descriptor = descriptor

    def release(self) -> None:
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None
