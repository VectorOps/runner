"""Recorded path-based reads of live workspace component configuration."""

import asyncio
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import tomllib
import typing

import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext

_JSON: typing.Final[pydantic.TypeAdapter[pydantic.JsonValue]] = pydantic.TypeAdapter(
    pydantic.JsonValue
)


def _read(path: Path, name: str, default: pydantic.JsonValue) -> pydantic.JsonValue:
    try:
        with path.open("rb") as stream:
            value: object = tomllib.load(stream).get("components", {})
    except FileNotFoundError:
        return deepcopy(default)
    for part in name.split("."):
        if not isinstance(value, dict) or part not in value:
            return deepcopy(default)
        value = value[part]
    result = _JSON.validate_python(value)
    _finite(result)
    return result


def _finite(value: pydantic.JsonValue) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("configuration values must be finite JSON values")
    if isinstance(value, dict):
        for item in value.values():
            _finite(item)
    elif isinstance(value, list):
        for item in value:
            _finite(item)


@c.activity_wrapper(name="get", kind="configuration.get", version=1)
async def get(
    call: c.ActivityCall, name: str, default: pydantic.JsonValue = None
) -> pydantic.JsonValue:
    """Record a dotted-path lookup, preserving old values during activity replay.

    Paths are relative to the workspace's components table. Only the selected
    value is recorded; consumers own validation and must not read secrets here.
    """
    if not name or any(not part.strip() for part in name.split(".")):
        raise ValueError(
            "configuration name must contain nonempty dotted path segments"
        )
    fallback = deepcopy(_JSON.validate_python(default))
    _finite(fallback)
    path = c.workflow_context().project.config_path

    async def invoke(context: ActivityExecutionContext) -> pydantic.JsonValue:
        return await asyncio.to_thread(_read, path, name, fallback)

    result = await call.run(input={"name": name, "default": fallback}, invoke=invoke)
    return deepcopy(result)


@c.activity_wrapper(name="get_validated", kind="configuration.validated", version=1)
async def get_validated(
    call: c.ActivityCall,
    name: str,
    model: type[pydantic.BaseModel],
    *,
    contract_digest: typing.Optional[str] = None,
) -> pydantic.JsonValue:
    """Record only owner-validated settings, with payload-free validation failures."""
    if not name or any(not part.strip() for part in name.split(".")):
        raise ValueError(
            "configuration name must contain nonempty dotted path segments"
        )
    path = c.workflow_context().project.config_path
    schema = json.dumps(
        model.model_json_schema(), sort_keys=True, separators=(",", ":")
    )

    def read() -> pydantic.JsonValue:
        try:
            return model.model_validate(_read(path, name, {})).model_dump(mode="json")
        except Exception:
            raise ValueError("component configuration is invalid") from None

    async def invoke(context: ActivityExecutionContext) -> pydantic.JsonValue:
        return await asyncio.to_thread(read)

    return await call.run(
        input={
            "name": name,
            "schema_digest": contract_digest
            or hashlib.sha256(schema.encode()).hexdigest(),
        },
        invoke=invoke,
    )
