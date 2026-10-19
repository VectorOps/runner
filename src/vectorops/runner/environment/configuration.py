"""Backend configuration, independent of runner workspace configuration."""

import typing

import pydantic

from .processes.models import ProcessConfig
from .target import ProjectPath


class LocalEnvironmentConfig(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    kind: typing.Literal["local"] = "local"
    base_project_path: ProjectPath
    max_read_bytes: int = pydantic.Field(default=1024 * 1024, gt=0, strict=True)
    max_write_bytes: int = pydantic.Field(default=16 * 1024 * 1024, gt=0, strict=True)
    max_list_entries: int = pydantic.Field(default=1000, gt=0, strict=True)
    processes: ProcessConfig = pydantic.Field(default_factory=ProcessConfig)


EnvironmentConfig = typing.Annotated[
    LocalEnvironmentConfig, pydantic.Field(discriminator="kind")
]
