"""Workflow-owned metadata and input validation."""

from collections.abc import Callable
import inspect
import typing

import pydantic
from jsonschema import Draft202012Validator

from vectorops.runner import presentation as presentation_models


class WorkflowDescriptor(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    reference: str
    name: str
    description: typing.Optional[str] = None
    kind: str = "workflow"
    version: int = pydantic.Field(default=1, ge=1)
    input_schema: dict[str, pydantic.JsonValue]
    presentation: presentation_models.WorkflowPresentation = pydantic.Field(
        default_factory=presentation_models.WorkflowPresentation
    )

    def preflight_input(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        """Check the published schema, not Python validators or input normalization."""
        Draft202012Validator(self.input_schema).validate(arguments)

    def validate_input(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        """Compatibility alias for schema-only preflight_input."""
        self.preflight_input(arguments)


def input_model(func: Callable[..., object]) -> type[pydantic.BaseModel]:
    """Build keyword input validation, excluding the injected runtime context."""
    parameters = list(inspect.signature(func).parameters.values())
    if not parameters:
        raise TypeError("workflow implementations require a first context parameter")
    hints = typing.get_type_hints(func, include_extras=True)
    fields: dict[str, typing.Any] = {}
    for parameter in parameters[1:]:
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise TypeError("workflow inputs must be named parameters")
        annotation = hints.get(parameter.name, pydantic.JsonValue)
        default = (
            ... if parameter.default is inspect.Parameter.empty else parameter.default
        )
        fields[parameter.name] = (annotation, default)
    return pydantic.create_model(
        f"{func.__name__}Input",
        __config__=pydantic.ConfigDict(extra="forbid", allow_inf_nan=False),
        **fields,
    )


def validated_input(
    func: Callable[..., object], arguments: dict[str, pydantic.JsonValue]
) -> dict[str, object]:
    """Apply authoritative Python validation while retaining typed values for invocation."""
    validated = input_model(func).model_validate(arguments)
    return dict(validated)
