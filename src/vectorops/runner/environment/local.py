"""Local execution settings and root-owned process sessions, not an I/O gateway."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Self

from .configuration import LocalEnvironmentConfig
from .errors import EnvironmentError, ErrorCode
from .processes.manager import ProcessManager
from .target import LocalExecutionTarget


@dataclass(frozen=True)
class LocalEnvironment:
    root: Path
    config: LocalEnvironmentConfig
    processes: ProcessManager

    @classmethod
    async def resolve_target(
        cls,
        config: LocalEnvironmentConfig,
        base_project_path: Optional[str] = None,
    ) -> LocalExecutionTarget:
        return await LocalExecutionTarget.resolve(
            config.base_project_path if base_project_path is None else base_project_path
        )

    @classmethod
    async def open(
        cls,
        config: LocalEnvironmentConfig,
        *,
        target: Optional[LocalExecutionTarget] = None,
    ) -> Self:
        selected = await cls.resolve_target(
            config, None if target is None else target.root
        )
        if target is not None and selected != target:
            raise EnvironmentError(
                ErrorCode.INVALID_PATH, "execution target resolves to a different root"
            )
        root = Path(selected.root)
        from .processes.posix import PosixProcessDriver

        settings = config.model_copy(deep=True)
        return cls(
            root,
            settings,
            ProcessManager(
                PosixProcessDriver(root, settings.processes.environment),
                settings.processes,
            ),
        )
