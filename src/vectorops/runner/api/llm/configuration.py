"""Resolve LLM policies from the active workflow's component configuration."""

from copy import deepcopy
from pathlib import Path
import typing

import pydantic

from vectorops.runner import errors
from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.api.llm import models

_BUILTIN_PROFILES: typing.Final = frozenset({"strong", "medium", "weak"})
_PROFILE_CATALOG_ADAPTER: typing.Final = pydantic.TypeAdapter(
    dict[str, dict[str, pydantic.JsonValue]]
)


class ConfigurationError(ValueError):
    """An LLM configuration failure with a user-facing message and diagnostic cause."""


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
    supplied = merge_settings(supplied, explicit)
    if compaction is not models.Unset.VALUE:
        supplied["compaction"] = (
            None
            if compaction is None
            else compaction.model_dump(mode="json", exclude_unset=True)
        )
    return supplied


def merge_settings(
    base: dict[str, pydantic.JsonValue], override: dict[str, pydantic.JsonValue]
) -> dict[str, pydantic.JsonValue]:
    """Merge nested policy mappings while replacing scalar and list values."""
    result = deepcopy(base)
    for key, value in override.items():
        previous = result.get(key)
        result[key] = (
            merge_settings(previous, value)
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
    context = composition.workflow_context()
    values = await get_config(context, "llm", {})
    return interpret_configuration(
        values, supplied, profile=profile, source_path=context.project.config_path
    )


def interpret_configuration(
    values: pydantic.JsonValue,
    supplied: dict[str, pydantic.JsonValue],
    *,
    profile: typing.Optional[str] = None,
    source_path: typing.Optional[Path] = None,
) -> models.Configuration:
    """Interpret a configuration snapshot independently of activity recording."""
    context = errors.ErrorContext(
        summary="Invalid LLM configuration.",
        path=source_path,
        section="components.llm",
        subject=None if profile is None else f"profile {profile!r}",
        hint="Select model as '<provider>/<model>'; a separate provider setting is not supported.",
    )
    if not isinstance(values, dict):
        error = ValueError("configuration llm must be a mapping")
        context = context.model_copy(
            update={"hint": "Configure [components.llm] as a mapping of settings."}
        )
        raise ConfigurationError(errors.format_error(error, context=context)) from error
    defaults = dict(values)
    try:
        profiles = _PROFILE_CATALOG_ADAPTER.validate_python(
            defaults.pop("profiles", {})
        )
    except pydantic.ValidationError as exc:
        context = context.model_copy(update={"section": "components.llm.profiles"})
        raise ConfigurationError(errors.format_error(exc, context=context)) from exc
    if profile is not None:
        if profile not in profiles and profile not in _BUILTIN_PROFILES:
            error = ValueError(f"unknown LLM profile: {profile!r}")
            context = context.model_copy(
                update={
                    "summary": "Unknown LLM profile.",
                    "hint": f"Define [components.llm.profiles.{profile}], "
                    "or select an existing profile.",
                }
            )
            raise ConfigurationError(
                errors.format_error(error, context=context)
            ) from error
    try:
        if profile is not None:
            defaults = merge_settings(defaults, profiles.get(profile, {}))
        return models.Configuration.model_validate(merge_settings(defaults, supplied))
    except pydantic.ValidationError as exc:
        if any(
            issue["loc"] == ("model",) and issue["type"] == "missing"
            for issue in exc.errors()
        ):
            hint = "Set 'model' in [components.llm]."
            if profile is not None:
                hint = (
                    f"Set 'model' in [components.llm.profiles.{profile}], "
                    "or set a shared model in [components.llm]."
                )
            context = context.model_copy(update={"hint": hint})
        raise ConfigurationError(errors.format_error(exc, context=context)) from exc


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
