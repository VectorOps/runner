"""Contextual, value-safe presentation of errors for terminal and API consumers."""

from collections.abc import Mapping
from pathlib import Path
import typing

import pydantic


class ErrorContext(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    summary: str = "Unable to complete the operation."
    path: typing.Optional[Path] = None
    section: typing.Optional[str] = None
    subject: typing.Optional[str] = None
    hint: typing.Optional[str] = None


_VALIDATION_MESSAGES: typing.Final = {
    "missing": "required setting is missing",
    "extra_forbidden": "unknown setting",
    "string_type": "must be a string",
    "int_type": "must be an integer",
    "int_parsing": "must be an integer",
    "int_from_float": "must be an integer",
    "float_type": "must be a number",
    "float_parsing": "must be a number",
    "bool_type": "must be a boolean",
    "bool_parsing": "must be a boolean",
    "dict_type": "must be a mapping",
    "list_type": "must be a list",
    "model_type": "must be a mapping",
    "finite_number": "must be a finite number",
    "literal_error": "must be one of the allowed values",
    "enum": "must be one of the allowed values",
    "string_pattern_mismatch": "does not match the required format",
}
_BOUND_MESSAGES: typing.Final = {
    "greater_than": ("gt", "must be greater than"),
    "greater_than_equal": ("ge", "must be at least"),
    "less_than": ("lt", "must be less than"),
    "less_than_equal": ("le", "must be at most"),
    "string_too_short": ("min_length", "must contain at least"),
    "string_too_long": ("max_length", "must contain at most"),
}


def _validation_message(kind: str, constraints: Mapping[str, object]) -> str:
    if kind in _BOUND_MESSAGES:
        key, message = _BOUND_MESSAGES[kind]
        bound = constraints.get(key)
        if isinstance(bound, (int, float)) and not isinstance(bound, bool):
            suffix = " characters" if kind.startswith("string_") else ""
            return f"{message} {bound:g}{suffix}"
    return _VALIDATION_MESSAGES.get(kind, "value is invalid")


def format_error(error: Exception, *, context: ErrorContext) -> str:
    """Format structured issues without exposing inputs or arbitrary exception text.

    Context is caller-owned display text. Custom validator messages and unrecognized
    exceptions use a safe fallback because their text may contain sensitive values.
    """
    header = context.summary
    if context.subject:
        header = f"{header.rstrip('.')} for {context.subject}."
    lines = [header]
    if context.path is not None:
        lines.append(f"File: {context.path}")
    if context.section:
        lines.append(f"Section: {context.section}")
    lines.append("")
    if isinstance(error, pydantic.ValidationError):
        for issue in error.errors(include_url=False, include_input=False):
            field = ".".join(str(part) for part in issue["loc"]) or "configuration"
            constraints = typing.cast(Mapping[str, object], issue.get("ctx", {}))
            message = _validation_message(issue["type"], constraints)
            lines.append(f"- {field}: {message}.")
    elif isinstance(error, ValueError):
        lines.append("The supplied value is invalid.")
    else:
        lines.append("The operation failed. See diagnostics for details.")
    if context.hint:
        lines.extend(["", context.hint])
    return "\n".join(lines)
