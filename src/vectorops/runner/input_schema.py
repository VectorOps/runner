"""Bounded, local-only JSON Schema 2020-12 contracts for durable input.

Contracts support the validator's assertion keywords and Pydantic annotations.
References must be acyclic local JSON pointers; remote retrieval, alternate
dialects, dynamic references, and unsupported keywords are rejected at creation.
"""

import re
import typing
from urllib.parse import unquote

import jsonschema
import pydantic
from referencing import Registry

DIALECT: typing.Final = "https://json-schema.org/draft/2020-12/schema"
Schema: typing.TypeAlias = dict[str, pydantic.JsonValue]
_ANNOTATIONS: typing.Final = {
    "$schema",
    "$defs",
    "$comment",
    "title",
    "description",
    "default",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
    "discriminator",
}
_MAPS: typing.Final = {"$defs", "properties", "patternProperties", "dependentSchemas"}
_SINGLE: typing.Final = {
    "additionalProperties",
    "unevaluatedProperties",
    "propertyNames",
    "items",
    "unevaluatedItems",
    "contains",
    "not",
    "if",
    "then",
    "else",
}
_ARRAYS: typing.Final = {"allOf", "anyOf", "oneOf", "prefixItems"}


class ValidationIssue(pydantic.BaseModel):
    path: list[typing.Union[str, int]]
    message: str


class InputValidationError(ValueError):
    def __init__(self, issues: list[ValidationIssue]) -> None:
        super().__init__("input does not match the request contract")
        self.issues = issues

    def details(self) -> pydantic.JsonValue:
        return {"issues": [issue.model_dump(mode="json") for issue in self.issues]}


def validator(schema: Schema) -> jsonschema.Draft202012Validator:
    """Check the supported dialect and every reference before accepting a contract."""
    count = 0

    def visit(node: pydantic.JsonValue, ancestors: frozenset[int]) -> None:
        nonlocal count
        count += 1
        if count > 4096 or len(ancestors) > 32:
            raise ValueError("input schema exceeds the complexity limit")
        if isinstance(node, bool):
            return
        if not isinstance(node, dict):
            raise ValueError("input schema must be an object or boolean")
        if id(node) in ancestors:
            raise ValueError("recursive input schemas are not supported")
        ancestors = ancestors | {id(node)}
        if node.get("$schema", DIALECT) != DIALECT:
            raise ValueError("input schemas must use JSON Schema 2020-12")
        allowed = (
            set(jsonschema.Draft202012Validator.VALIDATORS)
            | _ANNOTATIONS
            | _MAPS
            | _SINGLE
            | _ARRAYS
        )
        unsupported = set(node) - allowed
        unsupported.update(set(node) & {"$dynamicRef", "format"})
        if unsupported:
            raise ValueError(
                "unsupported input schema keywords: " + ", ".join(sorted(unsupported))
            )
        if "$ref" in node:
            ref = node["$ref"]
            if not isinstance(ref, str) or not ref.startswith("#/"):
                raise ValueError("input schema references must be local JSON pointers")
            target: pydantic.JsonValue = schema
            try:
                for part in unquote(ref[2:]).split("/"):
                    key = part.replace("~1", "/").replace("~0", "~")
                    if isinstance(target, dict):
                        target = target[key]
                    elif isinstance(target, list):
                        target = target[int(key)]
                    else:
                        raise ValueError("invalid input schema reference")
            except (KeyError, IndexError, ValueError) as exc:
                raise ValueError("unresolved input schema reference") from exc
            visit(target, ancestors)
        for key, value in node.items():
            if key in _MAPS and isinstance(value, dict):
                for child in value.values():
                    visit(child, ancestors)
            elif key in _ARRAYS and isinstance(value, list):
                for child in value:
                    visit(child, ancestors)
            elif key in _SINGLE:
                visit(value, ancestors)

    try:
        visit(schema, frozenset())
        jsonschema.Draft202012Validator.check_schema(schema)
    except (jsonschema.SchemaError, re.error) as exc:
        raise ValueError("invalid input schema") from exc
    return jsonschema.Draft202012Validator(schema, registry=Registry())


def validate(schema: Schema, value: pydantic.JsonValue, *, field: str) -> None:
    issues: list[ValidationIssue] = []

    def collect(error: jsonschema.ValidationError) -> None:
        if len(issues) >= 20:
            return
        if error.context:
            for child in error.context:
                collect(child)
        else:
            issues.append(
                ValidationIssue(
                    path=[field, *error.absolute_path],
                    message=f"must satisfy {error.validator}",
                )
            )

    for error in validator(schema).iter_errors(value):
        collect(error)
        if len(issues) >= 20:
            break
    if issues:
        raise InputValidationError(issues)
