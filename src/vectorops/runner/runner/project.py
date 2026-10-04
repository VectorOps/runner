"""Runner workspace paths and optional TOML configuration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
import tomllib
import typing

import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.environment.configuration import EnvironmentConfig
from vectorops.runner.tool_policy import ToolPolicy


class RetentionPolicy(pydantic.BaseModel):
    """Optional completed-history limits; protected executions always survive."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    max_age_days: typing.Optional[int] = pydantic.Field(default=None, ge=0, strict=True)
    max_completed_executions: typing.Optional[int] = pydantic.Field(
        default=None, ge=0, strict=True
    )


class RunnerConfig(pydantic.BaseModel):
    """Directory defaults, resolved relative to the workspace root."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    workflows_directory: str = "workflows"
    data_directory: str = "data"
    retention: RetentionPolicy = pydantic.Field(default_factory=RetentionPolicy)
    logging: diagnostics.LogConfig = pydantic.Field(
        default_factory=diagnostics.LogConfig
    )
    components: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    environment: typing.Optional[EnvironmentConfig] = None

    @pydantic.field_validator("workflows_directory", "data_directory", mode="after")
    @classmethod
    def validate_directory(cls, value: str) -> str:
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts or path == Path("."):
            raise ValueError("directory must be a nonempty relative workspace path")
        return value


@dataclass(frozen=True)
class RunnerProject:
    root: Path
    config: RunnerConfig
    tool_policy: ToolPolicy = field(
        default_factory=ToolPolicy, compare=False, repr=False
    )

    @property
    def config_path(self) -> Path:
        return self.root / "config.toml"

    @property
    def workflows_path(self) -> Path:
        return self.root / self.config.workflows_directory

    @property
    def data_path(self) -> Path:
        return self.root / self.config.data_directory

    @classmethod
    async def open(cls, path: typing.Union[str, Path]) -> typing.Self:
        """Read configuration and prepare workspace directories off the event loop."""
        return await asyncio.to_thread(cls._open, Path(path))

    @classmethod
    def _open(cls, path: Path) -> typing.Self:
        root = path.expanduser().resolve()
        if not root.is_dir():
            raise NotADirectoryError(f"runner workspace does not exist: {root}")
        config_path = root / "config.toml"
        if config_path.exists():
            with config_path.open("rb") as stream:
                config = RunnerConfig.model_validate(tomllib.load(stream))
        else:
            config = RunnerConfig()
        project = cls(root=root, config=config)
        for directory in (project.workflows_path, project.data_path):
            if not directory.resolve().is_relative_to(root):
                raise ValueError("workspace directories must remain inside the root")
            directory.mkdir(parents=True, exist_ok=True)
        return project
