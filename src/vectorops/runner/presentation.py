"""Workflow appearance, invocation overrides, and client-side visibility policy."""

from collections.abc import Sequence
from enum import Enum
import typing

import pydantic


class PresentationOption(str, Enum):
    PREFIX_MESSAGES = "prefix_messages"
    LLM_ONLY = "llm_only"
    HIDE_SUBWORKFLOWS = "hide_subworkflows"


def _options(values: list[PresentationOption]) -> list[PresentationOption]:
    return sorted(set(values), key=lambda option: option.value)


class _Appearance(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    short_name: typing.Optional[str] = pydantic.Field(
        default=None, strict=True, min_length=1, max_length=40
    )
    bgcolor: typing.Optional[str] = pydantic.Field(
        default=None, strict=True, pattern=r"^#[0-9a-fA-F]{6}$"
    )

    @pydantic.model_validator(mode="before")
    @classmethod
    def normalize(cls, value: object) -> object:
        if isinstance(value, dict):
            value = dict(value)
            name = value.get("short_name")
            if isinstance(name, str):
                value["short_name"] = " ".join(name.split())
            color = value.get("bgcolor")
            if isinstance(color, str):
                value["bgcolor"] = color.lower()
        return value


class WorkflowPresentationOverride(_Appearance):
    """Null fields inherit defaults; an explicit options list replaces them."""

    options: typing.Optional[list[PresentationOption]] = None

    @pydantic.field_validator("options", mode="after")
    @classmethod
    def normalize_options(
        cls, values: typing.Optional[list[PresentationOption]]
    ) -> typing.Optional[list[PresentationOption]]:
        return None if values is None else _options(values)


class WorkflowPresentation(_Appearance):
    options: list[PresentationOption] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("options", mode="after")
    @classmethod
    def normalize_options(
        cls, values: list[PresentationOption]
    ) -> list[PresentationOption]:
        return _options(values)

    def with_overrides(
        self, override: typing.Optional[WorkflowPresentationOverride]
    ) -> typing.Self:
        if override is None:
            return self.model_copy(deep=True)
        return type(self).model_validate(
            {**self.model_dump(), **override.model_dump(exclude_none=True)}
        )


class DisplayWorkflow(typing.Protocol):
    @property
    def id(self) -> str: ...

    @property
    def name(self) -> typing.Optional[str]: ...

    @property
    def definition_ref(self) -> str: ...

    @property
    def presentation(self) -> WorkflowPresentation: ...


DisplayKind: typing.TypeAlias = typing.Literal[
    "message", "tool", "activity", "status", "action"
]


class WorkflowDisplay(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    label: str
    bgcolor: typing.Optional[str] = None
    options: list[PresentationOption] = pydantic.Field(default_factory=list)
    hidden: bool = False

    @property
    def prefix(self) -> typing.Optional[str]:
        return (
            self.label if PresentationOption.PREFIX_MESSAGES in self.options else None
        )

    def shows(self, kind: DisplayKind) -> bool:
        if kind in ("status", "action"):
            return True
        if self.hidden:
            return False
        return kind == "message" or PresentationOption.LLM_ONLY not in self.options


def resolve_display(
    workflow: DisplayWorkflow,
    *,
    ancestors: Sequence[WorkflowPresentation] = (),
    disambiguate: bool = False,
    show_all: bool = False,
) -> WorkflowDisplay:
    """Project subtree restrictions without mutating workflow metadata or messages.

    Ancestors belong to the same execution, excluding the workflow itself.
    Clients classify rendered blocks, including tool blocks in LLM responses, and
    keep status and action items outside the filtered transcript.
    """
    own = workflow.presentation
    options = set(own.options)
    hidden = False
    for ancestor in ancestors:
        options.update(ancestor.options)
        hidden = hidden or PresentationOption.HIDE_SUBWORKFLOWS in ancestor.options
    if show_all:
        hidden = False
        options.discard(PresentationOption.LLM_ONLY)
        options.discard(PresentationOption.HIDE_SUBWORKFLOWS)
    label = own.short_name or workflow.name or workflow.definition_ref
    label = " ".join(label.split())[:40]
    if disambiguate:
        label = f"{label} · {workflow.id[:8]}"
    return WorkflowDisplay(
        label=label,
        bgcolor=own.bgcolor,
        options=_options(list(options)),
        hidden=hidden,
    )
