"""Resolve LLM policies from the active workflow's component configuration."""

from copy import deepcopy
import typing

import pydantic

from vectorops.runner.api import configuration
from vectorops.runner.api.llm import models


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
) -> models.Configuration:
    """Read one recorded snapshot before interpreting LLM-specific policy."""
    values = await configuration.get("llm", {})
    if not isinstance(values, dict):
        raise ValueError("configuration llm must be a mapping")
    return models.Configuration.model_validate(_merge(values, supplied))


async def resolve_settings(
    settings: typing.Optional[
        typing.Union[models.Settings, models.SettingsOverrides]
    ] = None,
    **overrides: typing.Unpack[models.SettingsParameters],
) -> models.Settings:
    """Resolve generation settings for workflow-side LLM helpers."""
    resolved = await resolve_configuration(explicit_options(settings, **overrides))
    return models.Settings.model_validate(
        resolved.model_dump(exclude={"compaction", "max_rounds", "retry"})
    )
