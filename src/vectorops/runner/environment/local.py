"""Local execution settings and root-owned process sessions, not an I/O gateway."""

import asyncio
from dataclasses import dataclass
import errno
import os
from pathlib import Path
from typing import Self

from .configuration import LocalEnvironmentConfig
from .errors import EnvironmentError, ErrorCode
from .processes.local import LocalProcessDriver
from .processes.manager import ProcessManager


@dataclass(frozen=True)
class LocalEnvironment:
    root: Path
    config: LocalEnvironmentConfig
    processes: ProcessManager

    @classmethod
    async def open(cls, config: LocalEnvironmentConfig) -> Self:
        def attach() -> Path:
            if os.name != "posix":
                raise EnvironmentError(
                    ErrorCode.UNSUPPORTED, "local execution requires POSIX"
                )
            root = Path(config.base_project_path).resolve(strict=True)
            if not root.is_dir():
                raise NotADirectoryError(
                    errno.ENOTDIR, "target is not a directory", str(root)
                )
            return root

        try:
            root = await asyncio.to_thread(attach)
        except OSError as exc:
            raise EnvironmentError.from_os_error(exc) from exc
        settings = config.model_copy(deep=True)
        return cls(
            root,
            settings,
            ProcessManager(
                LocalProcessDriver(root, settings.processes.environment),
                settings.processes,
            ),
        )
