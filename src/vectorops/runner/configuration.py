"""Component override contracts and workspace-owned transactional persistence."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import sqlite3
import typing

import pydantic

from vectorops.runner.runner import sql
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class Query(Model):
    name: str = pydantic.Field(default="", max_length=65536)

    @pydantic.field_validator("name")
    @classmethod
    def valid_path(cls, value: str) -> str:
        if value and any(not part.strip() for part in value.split(".")):
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


class Snapshot(Query):
    exists: bool
    value: pydantic.JsonValue = None


class SuggestQuery(Query):
    prefix: str = ""


class Suggestions(Model):
    values: list[pydantic.JsonValue] = pydantic.Field(default_factory=list)


def canonical_name(name: str) -> str:
    """Resolve component shorthand and TOML-rooted names to one dotted path."""
    if not name:
        return name
    Query(name=name)
    root = name.split(".", 1)[0]
    if root in {
        "components",
        "data_directory",
        "workflows_directory",
        "environment",
        "worktrees",
        "logging",
        "retention",
    }:
        return name
    return "components." + name


def name_path(name: str) -> list[str]:
    return [part.replace("~1", ".").replace("~0", "~") for part in name.split(".")]


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
            if not name:
                raise ValueError("persisted override name must not be empty")
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


def merge_defaults(
    base: dict[str, pydantic.JsonValue], override: dict[str, pydantic.JsonValue]
) -> dict[str, pydantic.JsonValue]:
    """Merge startup mappings recursively, replacing scalars and lists."""
    result = deepcopy(base)
    for key, value in override.items():
        previous = result.get(key)
        result[key] = (
            merge_defaults(previous, value)
            if isinstance(previous, dict) and isinstance(value, dict)
            else deepcopy(value)
        )
    return result


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
                documents = sql.Table("documents")
                row = sql.execute(
                    connection,
                    sql.Query.from_(documents)
                    .select(documents.payload)
                    .where(documents.name == "configuration"),
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
