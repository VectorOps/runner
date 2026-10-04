"""Serializable filesystem requests, results, and capability descriptions."""

import typing

import pydantic


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, ser_json_bytes="base64", val_json_bytes="base64"
    )


class PathRequest(Model):
    path: str = "."


class ReadRequest(PathRequest):
    offset: int = pydantic.Field(default=0, ge=0, strict=True)
    limit: int = pydantic.Field(default=65536, gt=0, strict=True)


class ReadResult(Model):
    content: bytes
    eof: bool


class LineReadRequest(PathRequest):
    start_line: int = pydantic.Field(default=1, ge=1, strict=True)
    max_lines: int = pydantic.Field(default=100, gt=0, strict=True)
    byte_limit: int = pydantic.Field(default=1024 * 1024, gt=0, strict=True)


class LineReadResult(Model):
    text: str
    complete_lines: int
    bytes_read: int
    eof: bool
    truncated: bool


class ListRequest(PathRequest):
    limit: int = pydantic.Field(default=1000, gt=0, strict=True)


class FileInfo(Model):
    path: str
    kind: typing.Literal["file", "directory", "symlink", "other"]
    size: int
    modified_ns: int


class ListResult(Model):
    entries: list[FileInfo]
    truncated: bool


class WriteRequest(PathRequest):
    content: bytes
    mode: typing.Literal["create", "replace", "upsert"] = "create"


class MkdirRequest(PathRequest):
    parents: bool = False
    exist_ok: bool = False


class RemoveRequest(PathRequest):
    recursive: bool = False


class RenameRequest(PathRequest):
    destination: str
    overwrite: bool = False


class Capabilities(Model):
    filesystem: bool = True
    atomic_file_replacement: bool = True
    conditional_updates: bool = False
    processes: bool = False
    patching: bool = False


class EnvironmentDescription(Model):
    kind: typing.Literal["local"] = "local"
    identity: str
    platform: str
    capabilities: Capabilities = pydantic.Field(default_factory=Capabilities)
    max_read_bytes: int
    max_write_bytes: int
    max_list_entries: int
    process_execution_allowed: bool = False
    race_resistant_containment: bool = False
    process_isolation: bool = False
