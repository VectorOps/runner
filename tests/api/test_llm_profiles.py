from collections.abc import AsyncGenerator
from pathlib import Path

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm
from vectorops.runner.api.llm import configuration
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.streaming import StreamItem


async def test_profile_precedence_and_settings_replacement(
    history: MemoryHistory, tmp_path: Path
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "default"\nprovider = "openai"\n'
        "temperature = 0.8\ntimeout = 30\nmax_output_tokens = 500\n"
        'reasoning = {effort = "low", summary = "detailed"}\n'
        '[components.llm.profiles.strong]\nmodel = "strong"\n'
        'temperature = 0.4\ncredential_profile = "work"\n'
        'reasoning = {effort = "high"}\n'
        '[components.llm.profiles.weak]\nmodel = "weak"\n'
    )
    backend = FakeProvider([answer("first"), answer("second")])

    async def workflow() -> pydantic.JsonValue:
        assert (await llm.resolve_settings()).model == "default"
        assert (await llm.resolve_settings(profile="weak")).model == "weak"
        agent = llm.LLM(
            "profile",
            llm.SettingsOverrides(model="positional", temperature=0.2),
            profile="strong",
            model="explicit",
            reasoning=None,
            provider=None,
            backend=backend,
        )
        agent.add_user_message("hello")
        await agent.generate()
        assert agent.settings.model == "explicit"
        assert agent.settings.temperature == 0.2
        assert agent.settings.reasoning is None
        assert agent.settings.provider is None
        assert agent.settings.credential_profile == "work"
        agent.set_settings(llm.SettingsOverrides())
        agent.add_user_message("continue")
        await agent.generate()
        selected = await llm.resolve_settings(profile="strong")
        assert agent.settings == selected
        assert selected.model == "strong"
        assert selected.temperature == 0.4
        assert selected.provider == "openai"
        assert selected.timeout == 30
        assert selected.max_output_tokens == 500
        assert selected.reasoning is not None
        assert selected.reasoning.effort == "high"
        assert selected.reasoning.summary == "detailed"
        assert len(agent.history) == 4
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed", first.outcome
    path.write_text('[components.llm]\nmodel = "changed"\n')
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert len(backend.requests) == 2


async def test_profile_runtime_policies(history: MemoryHistory, tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.llm]\nmax_rounds = 20\n"
        "retry = {initial_delay = 2, max_delay = 10}\n"
        "compaction = {context_window = 1000, keep_recent_ratio = 0.2}\n"
        '[components.llm.profiles.strong]\nmodel = "strong"\nmax_rounds = 4\n'
        "retry = {max_retries = 0}\n"
        "compaction = {trigger_threshold_ratio = 0.6}\n"
    )

    async def workflow() -> pydantic.JsonValue:
        resolved = await configuration.resolve_configuration({}, profile="strong")
        assert resolved.model == "strong"
        assert resolved.max_rounds == 4
        assert resolved.retry.max_retries == 0
        assert resolved.retry.initial_delay == 2
        assert resolved.retry.max_delay == 10
        assert resolved.compaction is not None
        assert resolved.compaction.context_window == 1000
        assert resolved.compaction.keep_recent_ratio == 0.2
        assert resolved.compaction.trigger_threshold_ratio == 0.6
        explicit = await configuration.resolve_configuration(
            {"retry": {"max_retries": 1}, "compaction": None, "max_rounds": 2},
            profile="strong",
        )
        assert explicit.retry.max_retries == 1
        assert explicit.retry.initial_delay == 2
        assert explicit.compaction is None
        assert explicit.max_rounds == 2
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed", result.outcome


@pytest.mark.parametrize(
    "values",
    [
        {"profiles": []},
        {"profiles": {"strong": None}},
        {"profiles": {"strong": {"temperature": 9}}},
        {"profiles": {"strong": {"reasoning": {"typo": True}}}},
        {"profiles": {"strong": {"typo": True}}},
        {"profiles": {"strong": {"profiles": {}}}},
        {"profiles": {"strong": {"model": " "}}},
        {"profiles": {"strong": {"retry": {"max_retries": -1}}}},
        {"profiles": {"strong": {"compaction": {"context_window": 0}}}},
        {"profiles": {"strong": {"max_rounds": 0}}},
    ],
)
async def test_invalid_profiles(
    history: MemoryHistory, tmp_path: Path, values: dict[str, pydantic.JsonValue]
) -> None:
    project = await RunnerProject.open(tmp_path)
    await project.configuration.set("llm", {"model": "default", **values})

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(pydantic.ValidationError):
            await llm.resolve_settings(profile="strong")
        return None

    result = await replay.run(history, workflow, project=project)
    assert result.outcome.kind == "completed", result.outcome


@pytest.mark.parametrize("profile", ["strong", "medium", "weak"])
@pytest.mark.parametrize(
    "catalog", ["", '[components.llm.profiles.custom]\nmodel = "custom"\n']
)
async def test_builtin_profiles_inherit_shared_settings_and_replay(
    history: MemoryHistory, tmp_path: Path, profile: str, catalog: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "default"\nprovider = "openai"\n'
        'reasoning = {effort = "high"}\nmax_rounds = 7\n'
        "retry = {max_retries = 0}\ncompaction = {context_window = 1000}\n" + catalog
    )
    backend = FakeProvider([answer("done")])

    async def workflow() -> pydantic.JsonValue:
        shared = await configuration.resolve_configuration({})
        assert await configuration.resolve_configuration({}, profile=profile) == shared
        explicit = await llm.resolve_settings(profile=profile, model="explicit")
        assert explicit.model == "explicit"
        agent = llm.LLM("inherited", profile=profile, backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        assert agent.settings == shared.generation_settings()
        return agent.settings.model

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed", first.outcome
    path.write_text(f'[components.llm.profiles.{profile}]\nmodel = "changed"\n')
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert len(backend.requests) == 1


@pytest.mark.parametrize("profile", ["strong", "medium", "weak"])
async def test_builtin_profiles_still_require_a_model(
    history: MemoryHistory, tmp_path: Path, profile: str
) -> None:
    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(pydantic.ValidationError, match="model"):
            await llm.resolve_settings(profile=profile)
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed", result.outcome


async def test_missing_profile_and_empty_profile(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "default"\n[components.llm.profiles.strong]\n'
    )

    async def workflow() -> pydantic.JsonValue:
        assert (await llm.resolve_settings(profile="strong")).model == "default"
        with pytest.raises(ValueError, match="unknown LLM profile"):
            await llm.LLM("missing", profile="missing", model="explicit").generate()
        with pytest.raises(ValueError, match="unknown LLM profile"):
            await llm.resolve_settings(profile="missing")
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed", result.outcome


@pytest.mark.parametrize("permanent", [False, True])
async def test_profile_changes_between_tool_rounds_and_replay(
    history: MemoryHistory, tmp_path: Path, permanent: bool
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.llm.profiles.strong]\nmodel = "old"\n')
    project = await RunnerProject.open(tmp_path)

    class ChangingProvider(FakeProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            async for item in super().stream(context, settings, request):
                yield item
            await project.configuration.set(
                "llm.profiles.strong.model", "new", permanent=permanent
            )

    backend = ChangingProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(id="call", name="unknown", arguments={})
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("changing", profile="strong", backend=backend)
        agent.add_user_message("hello")
        await agent.run()
        return agent.settings.model

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed", first.outcome
    assert first.outcome.result == "new"
    assert [settings.model for settings in backend.settings] == ["old", "new"]
    reopened = await RunnerProject.open(tmp_path)
    assert reopened.configuration.get("llm.profiles.strong.model") == (
        "new" if permanent else "old"
    )
    path.write_text('[components.llm]\nmodel = "changed"\n')
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert len(backend.requests) == 2


async def test_changed_profile_selector_rejects_changed_request_on_replay(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm.profiles.strong]\nmodel = "strong"\n'
        '[components.llm.profiles.weak]\nmodel = "weak"\n'
    )
    backend = FakeProvider([answer("done")])
    profile = "strong"

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("profile", profile=profile, backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed", first.outcome
    profile = "weak"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    assert len(backend.requests) == 1
