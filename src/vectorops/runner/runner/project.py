"""Runner workspace paths and optional TOML configuration."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cached_property
import math
from pathlib import Path
import tomllib
import typing

import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.environment.configuration import EnvironmentConfig
from vectorops.runner.tool_policy import ToolPolicy

if typing.TYPE_CHECKING:
    from vectorops.runner.runner.resolver import WorkflowResolver


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


Settings = typing.TypeVar("Settings", bound=pydantic.BaseModel)
_JSON: typing.Final[pydantic.TypeAdapter[pydantic.JsonValue]] = pydantic.TypeAdapter(
    pydantic.JsonValue
)


def validate_name(name: str) -> None:
    if not name or any(not part.strip() for part in name.split(".")):
        raise ValueError(
            "configuration name must contain nonempty dotted path segments"
        )


def json_value(value: pydantic.JsonValue) -> pydantic.JsonValue:
    result = _JSON.validate_python(value)

    def finite(item: pydantic.JsonValue) -> None:
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("configuration values must be finite JSON values")
        if isinstance(item, dict):
            for child in item.values():
                finite(child)
        elif isinstance(item, list):
            for child in item:
                finite(child)

    finite(result)
    return deepcopy(result)


class Configuration:
    """Workspace-owned component snapshot with explicit, atomic reloads.

    Getters only access memory and have no workflow or history dependencies.
    Runner-level settings remain fixed for the lifetime of the project.
    """

    def __init__(self, path: Path, config: RunnerConfig) -> None:
        self.path = path
        self._values = deepcopy(config.components)
        self._lock = asyncio.Lock()

    def get(self, name: str, default: pydantic.JsonValue = None) -> pydantic.JsonValue:
        validate_name(name)
        fallback = json_value(default)
        value: pydantic.JsonValue = self._values
        for part in name.split("."):
            if not isinstance(value, dict) or part not in value:
                return fallback
            value = value[part]
        return json_value(value)

    def get_validated(self, name: str, model: type[Settings]) -> Settings:
        validate_name(name)
        try:
            return model.model_validate(self.get(name, {}))
        except Exception:
            raise ValueError("component configuration is invalid") from None

    async def reload(self) -> None:
        """Publish a validated replacement; failed reloads retain the old snapshot."""
        async with self._lock:
            config = await asyncio.to_thread(self.read, self.path)
            self._values = deepcopy(config.components)

    @staticmethod
    def read(path: Path) -> RunnerConfig:
        try:
            with path.open("rb") as stream:
                return RunnerConfig.model_validate(tomllib.load(stream))
        except FileNotFoundError:
            return RunnerConfig()


@dataclass(frozen=True)
class RunnerProject:
    root: Path
    config: RunnerConfig
    tool_policy: ToolPolicy = field(
        default_factory=ToolPolicy, compare=False, repr=False
    )
    workflow_resolver: typing.Optional[WorkflowResolver] = field(
        default=None, compare=False, repr=False
    )

    @cached_property
    def configuration(self) -> Configuration:
        return Configuration(self.config_path, self.config)

    @cached_property
    def resolver(self) -> WorkflowResolver:
        from vectorops.runner.runner.resolver import WorkflowResolver

        return self.workflow_resolver or WorkflowResolver(self.workflows_path)

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
        config = Configuration.read(config_path)
        project = cls(root=root, config=config)
        for directory in (project.workflows_path, project.data_path):
            if not directory.resolve().is_relative_to(root):
                raise ValueError("workspace directories must remain inside the root")
            directory.mkdir(parents=True, exist_ok=True)
        return project
