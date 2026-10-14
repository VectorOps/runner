from pathlib import Path

import pydantic
import pytest

from vectorops.runner import errors
from vectorops.runner.api.llm import configuration


class NestedSettings(pydantic.BaseModel):
    max_retries: int = pydantic.Field(ge=0)


class Settings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    model: str
    temperature: float = pydantic.Field(ge=0, le=2)
    retry: NestedSettings


def test_validation_errors_are_contextual_and_value_safe() -> None:
    with pytest.raises(pydantic.ValidationError) as caught:
        Settings.model_validate(
            {"temperature": 9, "retry": {"max_retries": -1}, "typo": "secret-token"}
        )
    message = errors.format_error(
        caught.value,
        context=errors.ErrorContext(
            summary="Invalid configuration.",
            subject="profile 'strong'",
            path=Path(".voc/config.toml"),
            section="components.llm",
            hint="Check the settings above.",
        ),
    )
    assert message == (
        "Invalid configuration for profile 'strong'.\n"
        "File: .voc/config.toml\nSection: components.llm\n\n"
        "- model: required setting is missing.\n"
        "- temperature: must be at most 2.\n"
        "- retry.max_retries: must be at least 0.\n"
        "- typo: unknown setting.\n\nCheck the settings above."
    )
    assert "secret-token" not in message
    assert "input_value" not in message
    assert "pydantic.dev" not in message


@pytest.mark.parametrize(
    "annotation,value,expected",
    [
        (int, "secret-token", "must be an integer"),
        (float, "secret-token", "must be a number"),
        (bool, "secret-token", "must be a boolean"),
        (str, {"secret-token": 1}, "must be a string"),
        (dict[str, int], "secret-token", "must be a mapping"),
        (list[int], "secret-token", "must be a list"),
    ],
)
def test_wrong_types(annotation: object, value: object, expected: str) -> None:
    with pytest.raises(pydantic.ValidationError) as caught:
        pydantic.TypeAdapter(annotation).validate_python(value)
    message = errors.format_error(caught.value, context=errors.ErrorContext())
    assert expected in message
    assert "secret-token" not in message


def test_custom_validator_text_is_not_exposed() -> None:
    class PrivateSettings(pydantic.BaseModel):
        token: str

        @pydantic.field_validator("token")
        @classmethod
        def reject(cls, value: str) -> str:
            raise ValueError(f"rejected credential: {value}")

    with pytest.raises(pydantic.ValidationError) as caught:
        PrivateSettings(token="secret-token")
    message = errors.format_error(caught.value, context=errors.ErrorContext())
    assert "token: value is invalid" in message
    assert "secret-token" not in message


@pytest.mark.parametrize(
    "error", [ValueError("secret-token"), RuntimeError("secret-token")]
)
def test_unknown_exception_text_is_not_exposed(error: Exception) -> None:
    message = errors.format_error(error, context=errors.ErrorContext(summary="Failed."))
    assert message.startswith("Failed.\n\n")
    assert "secret-token" not in message
    assert "File:" not in message
    assert "Section:" not in message


@pytest.mark.parametrize("value", [None, [], "secret-token"])
def test_llm_mapping_errors_use_formatter(value: pydantic.JsonValue) -> None:
    with pytest.raises(configuration.ConfigurationError) as caught:
        configuration.interpret_configuration(value, {}, profile="strong")
    assert "Configure [components.llm] as a mapping" in str(caught.value)
    assert "secret-token" not in str(caught.value)
    assert isinstance(caught.value.__cause__, ValueError)


def test_llm_catalog_error_has_correct_section() -> None:
    with pytest.raises(configuration.ConfigurationError) as caught:
        configuration.interpret_configuration({"profiles": {"strong": None}}, {})
    assert "Section: components.llm.profiles" in str(caught.value)
    assert "strong: must be a mapping" in str(caught.value)


def test_missing_shared_model_has_specific_remedy() -> None:
    with pytest.raises(configuration.ConfigurationError) as caught:
        configuration.interpret_configuration({}, {})
    assert "Set 'model' in [components.llm]." in str(caught.value)


def test_nested_list_indices_and_root_errors() -> None:
    with pytest.raises(pydantic.ValidationError) as caught:
        pydantic.TypeAdapter(list[NestedSettings]).validate_python(
            [{"max_retries": -1}]
        )
    message = errors.format_error(caught.value, context=errors.ErrorContext())
    assert "0.max_retries: must be at least 0" in message


@pytest.mark.parametrize(
    "constraint,value,expected",
    [
        ({"gt": 0}, 0, "must be greater than 0"),
        ({"lt": 1}, 1, "must be less than 1"),
        ({"ge": 0.25}, 0, "must be at least 0.25"),
    ],
)
def test_numeric_constraints(
    constraint: dict[str, float], value: float, expected: str
) -> None:
    model = pydantic.create_model(
        "Bounded",
        value=(
            float,
            pydantic.Field(
                gt=constraint.get("gt"),
                lt=constraint.get("lt"),
                ge=constraint.get("ge"),
            ),
        ),
    )
    with pytest.raises(pydantic.ValidationError) as caught:
        model.model_validate({"value": value})
    assert expected in errors.format_error(caught.value, context=errors.ErrorContext())
