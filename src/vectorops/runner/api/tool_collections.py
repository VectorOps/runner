"""Recorded named tool selection with per-factory configuration ownership."""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
import importlib
from typing import Annotated, Optional

import pydantic

from . import composition, tool_registry
from .configuration import get_config
from .llm.tools import ToolBinding

CollectionKey = Annotated[str, pydantic.Field(pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$")]


class ToolCollection(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    tools: list[str] = pydantic.Field(default_factory=list)
    config: dict[str, dict[str, pydantic.JsonValue]] = pydantic.Field(
        default_factory=dict
    )

    @pydantic.field_validator("tools", mode="after")
    @classmethod
    def validate_tools(cls, values: list[str]) -> list[str]:
        if any(not value.isidentifier() for value in values):
            raise ValueError("tool entry names must be identifiers")
        if len(set(values)) != len(values):
            raise ValueError("tool entries must be unique")
        return values


class Modules(pydantic.RootModel[list[str]]):
    @pydantic.field_validator("root", mode="after")
    @classmethod
    def validate_modules(cls, values: list[str]) -> list[str]:
        if any(
            not all(part.isidentifier() for part in value.split("."))
            for value in values
        ):
            raise ValueError("tool modules must be importable module names")
        if len(set(values)) != len(values):
            raise ValueError("tool modules must be unique")
        return values


def _resolve(
    supplied: ToolCollection, default: Optional[ToolCollection]
) -> ToolCollection:
    """Lists replace; selected entry options overlay defaults one field at a time."""
    names = (
        supplied.tools
        if "tools" in supplied.model_fields_set
        else (default.tools if default is not None else [])
    )
    unused = set(supplied.config) - set(names)
    if unused:
        raise ValueError(
            f"configuration for unselected tools: {', '.join(sorted(unused))}"
        )
    options = {}
    for name in names:
        base = deepcopy(default.config.get(name, {})) if default is not None else {}
        override = supplied.config.get(name, {})
        if "factory" in override and override["factory"] != base.get("factory", name):
            base = {}
        options[name] = {**base, **deepcopy(override)}
    return ToolCollection(tools=list(names), config=options)


async def load_collection(
    name: str, *, defaults: Optional[Mapping[str, ToolCollection]] = None
) -> list[ToolBinding]:
    """Load one collection inside a workflow; reuse its bindings across LLM turns.

    Only module names and the selected collection are recorded, never approval
    policy. Factories are rebuilt on replay and own any recorded discovery I/O.
    """
    key = pydantic.TypeAdapter(CollectionKey).validate_python(name)
    ctx = composition.workflow_context()
    fallback = defaults.get(key) if defaults is not None else None
    raw = await get_config(
        ctx,
        f"tools.collections.{key}",
        fallback.model_dump(mode="json") if fallback is not None else None,
    )
    if raw is None:
        if fallback is None:
            raise ValueError(f"unknown tool collection: {key}")
        supplied = ToolCollection()
    else:
        supplied = ToolCollection.model_validate(raw)
    selected = _resolve(supplied, fallback)
    modules = Modules.model_validate(await get_config(ctx, "tools.modules", []))
    await asyncio.to_thread(importlib.import_module, "vectorops.runner.api.tools")
    for module in modules.root:
        await asyncio.to_thread(importlib.import_module, module)
    registry = tool_registry.registrations()
    prepared: list[tuple[tool_registry.Registration, pydantic.BaseModel]] = []
    for entry in selected.tools:
        options = dict(selected.config[entry])
        factory = options.pop("factory", entry)
        if not isinstance(factory, str) or factory not in registry:
            raise ValueError(f"unknown tool factory for entry {entry}: {factory}")
        registration = registry[factory]
        if not registration.module.startswith("vectorops.runner.api.") and (
            registration.module not in modules.root
        ):
            raise ValueError(f"tool module is not enabled: {registration.module}")
        prepared.append((registration, registration.config.model_validate(options)))
    bindings: list[ToolBinding] = []
    exposed: set[str] = set()
    for registration, validated in prepared:
        for binding in await registration.build(validated):
            tool_name = binding.definition.name
            if tool_name in exposed:
                raise ValueError(f"duplicate exposed tool name: {tool_name}")
            exposed.add(tool_name)
            bindings.append(binding)
    return bindings
