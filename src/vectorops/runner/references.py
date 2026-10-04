"""Canonical references to workspace and importable Python workflows."""

from dataclasses import dataclass
from pathlib import PurePosixPath


@dataclass(frozen=True)
class WorkflowReference:
    source: str
    callable: str
    library: bool = False

    @classmethod
    def parse(cls, value: str) -> "WorkflowReference":
        source, separator, callable_name = value.rpartition(":")
        if not separator or not source or not callable_name.isidentifier():
            raise ValueError("workflow reference must identify a file and callable")
        if source.startswith("python:"):
            module = source.removeprefix("python:")
            if not all(part.isidentifier() for part in module.split(".")):
                raise ValueError("workflow module must be an importable module name")
            return cls(module, callable_name, library=True)
        path = PurePosixPath(source)
        if (
            ":" in source
            or "\\" in source
            or path.is_absolute()
            or any(part in ("", ".", "..") for part in source.split("/"))
            or path.suffix
        ):
            raise ValueError("workflow path must be relative and extensionless")
        return cls(source, callable_name)

    def __str__(self) -> str:
        prefix = "python:" if self.library else ""
        return f"{prefix}{self.source}:{self.callable}"
