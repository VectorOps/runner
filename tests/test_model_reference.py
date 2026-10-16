from typing import Optional

import pydantic
import pytest

from vectorops.runner import model_reference
from vectorops.runner.api.llm import models


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        ("openai/gpt-4.1", "openai"),
        ("openrouter/anthropic/claude:tag", "openrouter"),
        ("openai/gpt-4.1:tag", "openai"),
        ("custom-model", None),
    ],
)
def test_provider_selection_preserves_settings(
    model: str, expected: Optional[str]
) -> None:
    assert model_reference.provider(model) == expected
    settings = models.Settings(model=model)
    assert settings.model == model
    assert "provider" not in settings.model_dump()
    assert models.Settings.model_validate_json(settings.model_dump_json()) == settings


@pytest.mark.parametrize(
    "model",
    [
        "/model",
        "openai/",
        "openai/:tag",
        "openai/ ",
        " openai/model",
    ],
)
@pytest.mark.parametrize("schema", [models.Settings, models.SettingsOverrides])
def test_invalid_selections_fail_validation(
    model: str, schema: type[pydantic.BaseModel]
) -> None:
    with pytest.raises(pydantic.ValidationError):
        schema.model_validate({"model": model})


@pytest.mark.parametrize(
    "schema", [models.Settings, models.SettingsOverrides, models.Configuration]
)
@pytest.mark.parametrize("provider", [None, "openai"])
def test_separate_provider_is_rejected(
    schema: type[pydantic.BaseModel], provider: Optional[str]
) -> None:
    with pytest.raises(
        pydantic.ValidationError, match="Extra inputs are not permitted"
    ):
        schema.model_validate({"model": "openai/gpt-4.1", "provider": provider})
