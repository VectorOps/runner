"""Deterministic JSON Schema validation without network reference retrieval."""

import jsonschema
from referencing import Registry
import pydantic


def validator(schema: dict[str, pydantic.JsonValue]) -> jsonschema.Draft202012Validator:
    pending: list[tuple[pydantic.JsonValue, int]] = [(schema, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > 4096 or depth > 32:
            raise ValueError("MCP schema exceeds the complexity limit")
        if isinstance(item, dict):
            pending.extend((value, depth + 1) for value in item.values())
        elif isinstance(item, list):
            pending.extend((value, depth + 1) for value in item)
    if schema.get("type") != "object":
        raise ValueError("MCP schemas must describe an object")
    if schema.get("$schema", "https://json-schema.org/draft/2020-12/schema") != (
        "https://json-schema.org/draft/2020-12/schema"
    ):
        raise ValueError("MCP schemas must use JSON Schema 2020-12")
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema, registry=Registry())
