import itertools
import typing

import pydantic
import pytest

from vectorops.runner import models, presentation as p
from vectorops.runner.control import protocol


def test_defaults_normalization_and_overrides() -> None:
    assert p.WorkflowPresentation().options == []
    defaults = p.WorkflowPresentation.model_validate(
        {
            "short_name": "  Research  team ",
            "bgcolor": "#AABBCC",
            "options": ["prefix_messages", "llm_only", "prefix_messages"],
        }
    )
    assert defaults.short_name == "Research team"
    assert defaults.bgcolor == "#aabbcc"
    assert defaults.options == [
        p.PresentationOption.LLM_ONLY,
        p.PresentationOption.PREFIX_MESSAGES,
    ]
    assert defaults.with_overrides(None) == defaults
    assert defaults.with_overrides(p.WorkflowPresentationOverride()) == defaults
    replaced = defaults.with_overrides(
        p.WorkflowPresentationOverride.model_validate(
            {"options": ["hide_subworkflows"]}
        )
    )
    assert replaced.options == [p.PresentationOption.HIDE_SUBWORKFLOWS]
    assert replaced.short_name == defaults.short_name
    assert replaced.bgcolor == defaults.bgcolor
    cleared = defaults.with_overrides(p.WorkflowPresentationOverride(options=[]))
    assert cleared.options == []
    assert defaults.options != []
    assert (
        defaults.with_overrides(
            p.WorkflowPresentationOverride(bgcolor="#123456")
        ).short_name
        == defaults.short_name
    )


@pytest.mark.parametrize(
    "value",
    [
        {"bgcolor": "red"},
        {"bgcolor": "#fff"},
        {"bgcolor": "#11223344"},
        {"bgcolor": "112233"},
        {"bgcolor": "#gg0000"},
        {"short_name": "   "},
        {"options": ["unknown"]},
    ],
)
@pytest.mark.parametrize(
    "model", [p.WorkflowPresentation, p.WorkflowPresentationOverride]
)
def test_invalid_presentation(
    model: type[pydantic.BaseModel], value: dict[str, object]
) -> None:
    with pytest.raises(pydantic.ValidationError):
        model.model_validate(value)


def test_workflow_roundtrip_legacy_and_configuration_comparison() -> None:
    legacy = models.Workflow(id="root", definition_ref="research:main")
    decorated = legacy.model_copy(
        update={
            "presentation": p.WorkflowPresentation(
                short_name="Research", bgcolor="#abcdef"
            )
        }
    )
    assert legacy.same_configuration(decorated)
    assert models.Workflow.model_validate_json(decorated.model_dump_json()) == decorated
    assert (
        models.Workflow.model_validate(
            {"id": "root", "definition_ref": "research:main"}
        ).presentation
        == p.WorkflowPresentation()
    )


@pytest.mark.parametrize("flags", list(itertools.product([False, True], repeat=3)))
def test_independent_subtree_options(flags: tuple[bool, bool, bool]) -> None:
    prefix, llm_only, hide_children = flags
    options = [
        option for enabled, option in zip(flags, p.PresentationOption) if enabled
    ]
    parent = models.Workflow(
        id="parent",
        definition_ref="main:parent",
        presentation=p.WorkflowPresentation(short_name="Parent", options=options),
    )
    child = models.Workflow(
        id="child",
        definition_ref="main:child",
        presentation=p.WorkflowPresentation(short_name="Child", bgcolor="#123456"),
    )
    own = p.resolve_display(parent)
    display = p.resolve_display(child, ancestors=[parent.presentation])
    assert own.shows("message")
    assert own.shows("tool") is not llm_only
    assert display.shows("message") is not hide_children
    assert display.shows("tool") == (not hide_children and not llm_only)
    assert display.shows("activity") == (not hide_children and not llm_only)
    assert display.prefix == ("Child" if prefix else None)
    assert display.bgcolor == "#123456"
    assert display.shows("action")
    assert display.shows("status")
    assert child.presentation.options == []
    debug = p.resolve_display(child, ancestors=[parent.presentation], show_all=True)
    assert all(
        debug.shows(typing.cast(p.DisplayKind, kind))
        for kind in ("message", "tool", "activity", "action", "status")
    )
    independent = p.resolve_display(child)
    assert independent.shows("tool")
    assert independent.prefix is None


def test_nested_restrictions_and_parallel_attribution() -> None:
    root = p.WorkflowPresentation.model_validate(
        {"options": ["prefix_messages", "llm_only"]}
    )
    middle = p.WorkflowPresentation.model_validate({"options": ["hide_subworkflows"]})
    children = [
        models.Workflow(
            id=identifier,
            definition_ref="research:main",
            presentation=p.WorkflowPresentation(short_name="Research"),
        )
        for identifier in ("a12f0000", "b34f0000")
    ]
    displays = [
        p.resolve_display(child, ancestors=[root], disambiguate=True)
        for child in children
    ]
    assert displays[0].prefix != displays[1].prefix
    assert all(
        display.shows("message") and not display.shows("tool") for display in displays
    )
    nested = p.resolve_display(children[0], ancestors=[root, middle])
    assert not nested.shows("message")
    assert nested.shows("action") and nested.shows("status")
    fallback = models.Workflow(
        id="fallback", definition_ref="research:main", name="Task " * 24
    )
    assert len(p.resolve_display(fallback).label) <= 40
    assert (
        p.resolve_display(fallback.model_copy(update={"name": None})).label
        == "research:main"
    )


def test_control_start_preserves_omitted_and_empty_options() -> None:
    inherited = protocol.Start.model_validate(
        {
            "reference": "main:run",
            "idempotency_key": "one",
            "presentation": {"bgcolor": "#ABCDEF"},
        }
    )
    cleared = protocol.Start.model_validate(
        {
            "reference": "main:run",
            "idempotency_key": "two",
            "presentation": {"options": []},
        }
    )
    assert inherited.presentation is not None and inherited.presentation.options is None
    assert inherited.presentation.bgcolor == "#abcdef"
    assert cleared.presentation is not None and cleared.presentation.options == []
