"""Explicit public names for packaged workflows, independent of module layout."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

import pydantic

from vectorops.runner.api.composition import WorkflowDefinition
from vectorops.runner.workflows.chat import chat

WORKFLOWS: Final[Mapping[str, WorkflowDefinition[..., pydantic.JsonValue]]] = (
    MappingProxyType({"chat": chat})
)


def resolve(name: str) -> WorkflowDefinition[..., pydantic.JsonValue]:
    try:
        return WORKFLOWS[name]
    except KeyError:
        raise ValueError(f"unknown built-in workflow: builtin:{name}") from None
