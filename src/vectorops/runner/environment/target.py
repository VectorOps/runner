"""Durable execution targets and local attachment validation."""

import asyncio
import errno
import os
from pathlib import Path
import typing

import pydantic

from .errors import EnvironmentError, ErrorCode


def validate_project_path(value: str) -> str:
    if "\0" in value or not Path(value).is_absolute():
        raise ValueError("base_project_path must be an absolute path without NULs")
    return value


ProjectPath = typing.Annotated[
    str, pydantic.Field(min_length=1), pydantic.AfterValidator(validate_project_path)
]


class LocalExecutionTarget(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    kind: typing.Literal["local"] = "local"
    root: ProjectPath

    @classmethod
    async def resolve(cls, path: str) -> typing.Self:
        """Validate and canonicalize an existing directory off the event loop."""
        if os.name != "posix":
            raise EnvironmentError(
                ErrorCode.UNSUPPORTED, "local execution requires POSIX"
            )
        selected = cls(root=path)

        def attach() -> Path:
            root = Path(selected.root).resolve(strict=True)
            if not root.is_dir():
                raise NotADirectoryError(
                    errno.ENOTDIR, "target is not a directory", str(root)
                )
            return root

        try:
            root = await asyncio.to_thread(attach)
        except OSError as exc:
            raise EnvironmentError.from_os_error(exc) from exc
        return cls(root=str(root))
