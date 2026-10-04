"""Backend configuration, independent of runner workspace configuration."""

from pathlib import Path
import typing

import pydantic


class LocalEnvironmentConfig(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    kind: typing.Literal["local"] = "local"
    base_project_path: str = pydantic.Field(min_length=1)
    max_read_bytes: int = pydantic.Field(default=1024 * 1024, gt=0, strict=True)
    max_write_bytes: int = pydantic.Field(default=16 * 1024 * 1024, gt=0, strict=True)
    max_list_entries: int = pydantic.Field(default=1000, gt=0, strict=True)

    @pydantic.field_validator("base_project_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if "\0" in value or not Path(value).is_absolute():
            raise ValueError("base_project_path must be an absolute path without NULs")
        return value


EnvironmentConfig = typing.Annotated[
    LocalEnvironmentConfig, pydantic.Field(discriminator="kind")
]
