from pathlib import Path

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.workflows import settings


@pytest.mark.parametrize("tier", list(settings.ModelTier))
def test_model_tiers(tier: settings.ModelTier) -> None:
    defaults = settings.Settings(
        strong_model="strong-model",
        medium_model="medium-model",
        weak_model="weak-model",
        thinking_level="high",
    )
    assert defaults.llm_overrides(tier).model_dump(exclude_unset=True) == {
        "model": f"{tier.value}-model",
        "reasoning": {"effort": "high"},
    }


@pytest.mark.parametrize("level", ["none", "minimal", "low", "medium", "high", "xhigh"])
def test_thinking_levels(level: str) -> None:
    defaults = settings.Settings.model_validate({"thinking_level": level})
    assert defaults.llm_overrides(settings.ModelTier.STRONG).model_dump(
        exclude_unset=True
    ) == {"reasoning": {"effort": level}}


@pytest.mark.parametrize("tier", list(settings.ModelTier))
def test_unset_options_are_not_explicit_overrides(tier: settings.ModelTier) -> None:
    assert settings.Settings().llm_overrides(tier).model_dump(exclude_unset=True) == {}


@pytest.mark.parametrize(
    "values",
    [
        {"strong_model": ""},
        {"medium_model": " "},
        {"weak_model": "\t"},
        {"weak_model": 123},
        {"thinking_level": "huge"},
        {"thinking_level": True},
        {"typo": "model"},
    ],
)
def test_invalid_settings(values: dict[str, pydantic.JsonValue]) -> None:
    with pytest.raises(pydantic.ValidationError):
        settings.Settings.model_validate(values)


async def test_load_and_resolve_with_replay(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "fallback"\ntemperature = 0.4\n'
        'reasoning = {effort = "low", summary = "concise"}\n'
        '[components.workflows.defaults]\nstrong_model = "strong-model"\n'
        'weak_model = "weak-model"\nthinking_level = "high"\n'
    )

    async def workflow() -> pydantic.JsonValue:
        defaults = await settings.load_settings()
        strong = await llm.resolve_settings(
            defaults.llm_overrides(settings.ModelTier.STRONG)
        )
        medium = await llm.resolve_settings(
            defaults.llm_overrides(settings.ModelTier.MEDIUM)
        )
        weak = await llm.resolve_settings(
            defaults.llm_overrides(settings.ModelTier.WEAK)
        )
        assert (strong.model, medium.model, weak.model) == (
            "strong-model",
            "fallback",
            "weak-model",
        )
        assert strong.temperature == 0.4
        assert strong.reasoning is not None
        assert strong.reasoning.effort == "high"
        assert strong.reasoning.summary == "concise"
        return defaults.model_dump(mode="json")

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text('[components.workflows.defaults]\nthinking_level = "invalid"\n')
    second = await replay.run(history, workflow, first.id, workspace=tmp_path)
    assert second.outcome == first.outcome


async def test_missing_defaults_inherit_llm(
    tmp_path: Path, history: MemoryHistory
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "fallback"\nreasoning = {effort = "low"}\n'
    )

    async def workflow() -> pydantic.JsonValue:
        defaults = await settings.load_settings()
        assert defaults == settings.Settings()
        resolved = await llm.resolve_settings(
            defaults.llm_overrides(settings.ModelTier.WEAK)
        )
        assert resolved.model == "fallback"
        assert resolved.reasoning is not None
        assert resolved.reasoning.effort == "low"
        return None

    assert (await replay.run(history, workflow, workspace=tmp_path)).outcome.kind == (
        "completed"
    )


async def test_invalid_configuration_is_a_recorded_failure(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.workflows.defaults]\nstrong_model = " "\n')

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ActivityError, match="component configuration is invalid"):
            await settings.load_settings()
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.unlink()
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
