from typing import Optional

import pydantic
import pytest

from vectorops.runner import model_reference
from vectorops.runner.api.llm import models


@pytest.mark.parametrize(
    ("model", "explicit", "expected"),
    [
        ("openai/gpt-4.1", None, "openai"),
        ("openrouter/anthropic/claude:tag", None, "openrouter"),
        ("openai/gpt-4.1:tag", "openai", "openai"),
        ("gpt-4.1", "openai", "openai"),
        ("custom-model", None, None),
    ],
)
def test_provider_selection_preserves_settings(
    model: str, explicit: Optional[str], expected: Optional[str]
) -> None:
    assert model_reference.provider(model, explicit) == expected
    settings = models.Settings(model=model, provider=explicit)
    assert settings.model == model
    assert settings.provider == explicit
    assert models.Settings.model_validate_json(settings.model_dump_json()) == settings


@pytest.mark.parametrize(
    ("model", "explicit"),
    [
        ("openai/gpt-4.1", "anthropic"),
        ("/model", None),
        ("openai/", None),
        ("openai/:tag", None),
        ("openai/ ", None),
        (" openai/model", None),
        ("model", " "),
    ],
)
@pytest.mark.parametrize("schema", [models.Settings, models.SettingsOverrides])
def test_invalid_selections_fail_validation(
    model: str, explicit: Optional[str], schema: type[pydantic.BaseModel]
) -> None:
    with pytest.raises(pydantic.ValidationError):
        schema.model_validate({"model": model, "provider": explicit})
