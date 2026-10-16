"""Advisory configuration values from existing schemas and the Connect catalog."""

import json
import typing

import connect
import pydantic

from vectorops.runner import configuration
from vectorops.runner.api.llm import models as llm_models
from vectorops.runner.workflows import settings as workflow_settings


def _enum_values(
    schema: dict[str, pydantic.JsonValue],
    path: list[str],
    definitions: dict[str, pydantic.JsonValue],
) -> list[pydantic.JsonValue]:
    reference = schema.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        target = definitions.get(reference.removeprefix("#/$defs/"))
        if isinstance(target, dict):
            return _enum_values(target, path, definitions)
    result: list[pydantic.JsonValue] = []
    for keyword in ("anyOf", "oneOf"):
        variants = schema.get(keyword)
        if isinstance(variants, list):
            for variant in variants:
                if isinstance(variant, dict):
                    result.extend(_enum_values(variant, path, definitions))
    if path:
        properties = schema.get("properties")
        child = properties.get(path[0]) if isinstance(properties, dict) else None
        if isinstance(child, dict):
            result.extend(_enum_values(child, path[1:], definitions))
    else:
        values = schema.get("enum")
        if isinstance(values, list):
            result.extend(values)
        if "const" in schema:
            result.append(schema["const"])
    return result


def suggest(query: configuration.SuggestQuery) -> configuration.Suggestions:
    """Offer known values without validating or restricting configuration writes."""
    path = configuration.name_path(configuration.canonical_name(query.name))
    model: typing.Optional[type[pydantic.BaseModel]] = None
    field: list[str] = []
    if path[:2] == ["components", "llm"]:
        field = path[2:]
        if field[:1] == ["profiles"]:
            field = field[2:]
        model = llm_models.Configuration
    elif path[:2] == ["components", "workflows"] and len(path) >= 3:
        workflow_models: dict[str, type[pydantic.BaseModel]] = {
            "defaults": workflow_settings.Settings,
            "agent": workflow_settings.AgentSettings,
            "helper": workflow_settings.HelperSettings,
        }
        model = workflow_models.get(path[2])
        field = path[3:]
    values: list[pydantic.JsonValue] = []
    if model is not None and field:
        if field[-1] == "model" or field in (
            ["strong_model"],
            ["medium_model"],
            ["weak_model"],
        ):
            values = [
                f"{item.provider}/{item.model}"
                for item in connect.default_model_registry.list_models()
            ]
        elif field[-1] == "provider":
            values = list(connect.default_model_registry.providers())
        else:
            schema = typing.cast(
                dict[str, pydantic.JsonValue], model.model_json_schema()
            )
            definitions = schema.get("$defs")
            values = _enum_values(
                schema, field, definitions if isinstance(definitions, dict) else {}
            )
    unique = {json.dumps(value): value for value in values}
    return configuration.Suggestions(
        values=[
            unique[key]
            for key in sorted(unique)
            if str(unique[key]).startswith(query.prefix)
        ]
    )
