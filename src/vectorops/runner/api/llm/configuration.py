"""Resolve LLM policies from the active workflow's component configuration."""

from copy import deepcopy
import typing

import pydantic

from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.api.llm import models

_BUILTIN_PROFILES: typing.Final = frozenset({"strong", "medium", "weak"})
_PROFILE_CATALOG_ADAPTER: typing.Final = pydantic.TypeAdapter(
    dict[str, dict[str, pydantic.JsonValue]]
)


def explicit_options(
    settings: typing.Optional[
        typing.Union[models.Settings, models.SettingsOverrides]
    ] = None,
    *,
    compaction: typing.Union[
        models.Compaction, None, models.Unset
    ] = models.Unset.VALUE,
    **overrides: typing.Unpack[models.SettingsParameters],
) -> dict[str, pydantic.JsonValue]:
    """Snapshot explicit overrides without reading workspace defaults."""
    supplied: dict[str, pydantic.JsonValue] = (
        {} if settings is None else settings.model_dump(mode="json", exclude_unset=True)
    )
    explicit = models.SettingsOverrides.model_validate(overrides).model_dump(
        mode="json", exclude_unset=True
    )
    supplied.update(explicit)
    if compaction is not models.Unset.VALUE:
        supplied["compaction"] = (
            None
            if compaction is None
            else compaction.model_dump(mode="json", exclude_unset=True)
        )
    return supplied


def _merge(
    base: dict[str, pydantic.JsonValue], override: dict[str, pydantic.JsonValue]
) -> dict[str, pydantic.JsonValue]:
    result = deepcopy(base)
    for key, value in override.items():
        previous = result.get(key)
        result[key] = (
            _merge(previous, value)
            if isinstance(previous, dict) and isinstance(value, dict)
            else deepcopy(value)
        )
    return result


async def resolve_configuration(
    supplied: dict[str, pydantic.JsonValue],
    *,
    profile: typing.Optional[str] = None,
) -> models.Configuration:
    """Read one recorded snapshot before interpreting LLM-specific policy."""
    values = await get_config(composition.workflow_context(), "llm", {})
    return interpret_configuration(values, supplied, profile=profile)


def interpret_configuration(
    values: pydantic.JsonValue,
    supplied: dict[str, pydantic.JsonValue],
    *,
    profile: typing.Optional[str] = None,
) -> models.Configuration:
    """Interpret a configuration snapshot independently of activity recording."""
    if not isinstance(values, dict):
        raise ValueError("configuration llm must be a mapping")
    defaults = dict(values)
    profiles = _PROFILE_CATALOG_ADAPTER.validate_python(defaults.pop("profiles", {}))
    if profile is not None:
        if profile not in profiles and profile not in _BUILTIN_PROFILES:
            raise ValueError(f"unknown LLM profile: {profile!r}")
        defaults = _merge(defaults, profiles.get(profile, {}))
    return models.Configuration.model_validate(_merge(defaults, supplied))


async def resolve_settings(
    settings: typing.Optional[
        typing.Union[models.Settings, models.SettingsOverrides]
    ] = None,
    *,
    profile: typing.Optional[str] = None,
    **overrides: typing.Unpack[models.SettingsParameters],
) -> models.Settings:
    """Resolve generation settings for workflow-side LLM helpers."""
    resolved = await resolve_configuration(
        explicit_options(settings, **overrides), profile=profile
    )
    return resolved.generation_settings()
