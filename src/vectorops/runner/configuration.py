"""Component override contracts and workspace-owned transactional persistence."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import sqlite3
import typing

import pydantic

from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class PathQuery(Model):
    path: list[str] = pydantic.Field(default_factory=list, max_length=64)

    @pydantic.field_validator("path")
    @classmethod
    def valid_path(cls, value: list[str]) -> list[str]:
        if any(not part or len(part) > 512 for part in value):
            raise ValueError("path segments must contain 1-512 characters")
        return value


class PathUpdate(PathQuery):
    value: pydantic.JsonValue
    permanent: bool = pydantic.Field(default=False, strict=True)

    @pydantic.field_validator("value")
    @classmethod
    def finite(cls, value: pydantic.JsonValue) -> pydantic.JsonValue:
        json.dumps(value, allow_nan=False)
        return value


class Snapshot(PathQuery):
    exists: bool
    value: pydantic.JsonValue = None


class SuggestQuery(PathQuery):
    prefix: str = ""


class Suggestions(Model):
    values: list[pydantic.JsonValue] = pydantic.Field(default_factory=list)


def path_name(path: list[str]) -> str:
    return ".".join(part.replace("~", "~0").replace(".", "~1") for part in path)


def name_path(name: str) -> list[str]:
    return [part.replace("~1", ".").replace("~0", "~") for part in name.split(".")]


class Query(Model):
    name: str = pydantic.Field(min_length=1, max_length=65536)

    @pydantic.field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if any(not part for part in value.split(".")):
            raise ValueError(
                "configuration name must contain nonempty dotted path segments"
            )
        return value


class Update(Query):
    value: pydantic.JsonValue
    permanent: bool = pydantic.Field(default=False, strict=True)

    @pydantic.field_validator("value")
    @classmethod
    def finite(cls, value: pydantic.JsonValue) -> pydantic.JsonValue:
        json.dumps(value, allow_nan=False)
        return value


class Value(Query):
    exists: bool
    value: pydantic.JsonValue = None
    source: typing.Literal["toml", "temporary", "permanent", "mixed", "missing"]


class Override(Model):
    value: pydantic.JsonValue
    baseline: str
    permanent: bool = False


class Manifest(Model):
    version: typing.Literal[1] = 1
    overrides: dict[str, Override] = pydantic.Field(default_factory=dict)

    @pydantic.model_validator(mode="after")
    def validate_overrides(self) -> typing.Self:
        for name, item in self.overrides.items():
            Update(name=name, value=item.value, permanent=item.permanent)
            if not item.permanent:
                raise ValueError("temporary overrides cannot be persisted")
        names = sorted(self.overrides)
        if any(right.startswith(left + ".") for left, right in zip(names, names[1:])):
            raise ValueError("overrides must not overlap")
        return self


def lookup(values: pydantic.JsonValue, name: str) -> tuple[bool, pydantic.JsonValue]:
    for part in name_path(name):
        if not isinstance(values, dict) or part not in values:
            return False, None
        values = values[part]
    return True, values


def fingerprint(values: pydantic.JsonValue, name: str) -> str:
    exists, value = lookup(values, name)

    def tagged(item: pydantic.JsonValue) -> object:
        if isinstance(item, dict):
            return ["dict", [[key, tagged(item[key])] for key in sorted(item)]]
        if isinstance(item, list):
            return ["list", [tagged(child) for child in item]]
        if isinstance(item, float) and not math.isfinite(item):
            return ["float", repr(item)]
        return [type(item).__name__, item]

    payload = json.dumps(
        [exists, tagged(value)], allow_nan=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def overlaps(left: str, right: str) -> bool:
    return left == right or left.startswith(right + ".") or right.startswith(left + ".")


def effective(
    defaults: dict[str, pydantic.JsonValue], overrides: dict[str, Override]
) -> dict[str, pydantic.JsonValue]:
    values = deepcopy(defaults)
    for name, item in overrides.items():
        target = values
        parts = name_path(name)
        for part in parts[:-1]:
            child = target.get(part)
            if not isinstance(child, dict):
                child = {}
                target[part] = child
            target = child
        target[parts[-1]] = deepcopy(item.value)
    return values


class Store:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database

    def load(self, defaults: dict[str, pydantic.JsonValue]) -> dict[str, Override]:
        def read(connection: sqlite3.Connection) -> dict[str, Override]:
            self.database.document(
                connection, "configuration", lambda: Manifest().model_dump_json()
            )
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload FROM documents WHERE name = 'configuration'"
                ).fetchone()
                items = Manifest.model_validate_json(row[0]).overrides
                valid = {
                    name: item
                    for name, item in items.items()
                    if item.baseline == fingerprint(defaults, name)
                }
                if valid != items:
                    self.database.put_document(
                        connection,
                        "configuration",
                        Manifest(overrides=valid).model_dump_json(),
                    )
                return valid

        return self.database.run_sync(read)

    async def save(self, overrides: dict[str, Override]) -> None:
        await self.database.save_document(
            "configuration",
            Manifest(
                overrides={
                    name: item for name, item in overrides.items() if item.permanent
                }
            ).model_dump_json(),
        )
