import asyncio
from pathlib import Path
from collections.abc import AsyncGenerator
from typing import Optional

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm
from vectorops.runner.api.llm.models import RoundLimitError
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem
from vectorops.runner.runner.project import RunnerProject


async def test_omitted_options_and_explicit_overrides(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "workspace-model"\nprovider = "openai"\ntemperature = 0.7\nmax_output_tokens = 500\ntimeout = 30\n[components.llm.reasoning]\neffort = "high"\nsummary = "detailed"\n'
    )
    backend = FakeProvider([answer("ok")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("default", backend=backend)
        with pytest.raises(RuntimeError, match="first generation"):
            _ = agent.settings
        agent.add_user_message("hello")
        await agent.generate()
        assert agent.settings.model == "workspace-model"
        settings = await llm.resolve_settings(
            llm.SettingsOverrides(temperature=0),
            model="explicit",
            reasoning=connect.ReasoningConfig(effort="low"),
        )
        assert settings.model == "explicit"
        assert settings.temperature == 0
        assert settings.timeout == 30
        assert settings.reasoning is not None
        assert settings.reasoning.effort == "low"
        assert settings.reasoning.summary == "detailed"
        cleared = await llm.resolve_settings(
            llm.Settings(model="other", reasoning=None, provider=None)
        )
        assert cleared.reasoning is None and cleared.provider is None
        assert cleared.max_output_tokens == 500
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "changed"\n')
    await replay.run(history, workflow, first.id, workspace=tmp_path)
    assert len(backend.requests) == 1
    request = backend.requests[0]
    assert request.reasoning is not None and request.reasoning.effort == "high"
    assert request.temperature == 0.7 and request.max_output_tokens == 500


async def test_overridden_model_changes_and_recorded_settings_on_replay(
    history: MemoryHistory,
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.llm]\nmodel = "old"\nreasoning = {effort = "low"}\n')
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
                "llm", {"model": "new", "reasoning": {"effort": "high"}}
            )

    backend = ChangingProvider([answer("first"), answer("second")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("changing", backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        await agent.generate()
        return agent.settings.model

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed" and first.outcome.result == "new"
    assert [settings.model for settings in backend.settings] == ["old", "new"]
    assert [
        request.reasoning.effort for request in backend.requests if request.reasoning
    ] == ["low", "high"]
    path.write_text('[components.llm]\nmodel = "third"\n')
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert len(backend.requests) == 2


@pytest.mark.parametrize("profile", [None, "strong"])
async def test_interrupted_generation_keeps_old_settings_then_refreshes(
    history: MemoryHistory,
    tmp_path: Path,
    profile: Optional[str],
) -> None:
    path = tmp_path / "config.toml"
    section = (
        "components.llm" if profile is None else f"components.llm.profiles.{profile}"
    )
    path.write_text(f'[{section}]\nmodel = "old"\n')
    backend = FakeProvider([answer("first"), answer("second")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("interrupted", profile=profile, backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        await agent.generate()
        return None

    task = asyncio.create_task(replay.run(history, workflow, workspace=tmp_path))
    await asyncio.wait_for(backend.started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    path.write_text(f'[{section}]\nmodel = "new"\n')
    backend.release.set()
    result = await replay.run(
        history, workflow, backend.step_ids[0], workspace=tmp_path
    )
    assert result.outcome.kind == "completed"
    assert [settings.model for settings in backend.settings] == ["old", "old", "new"]
    assert backend.step_ids[0] == backend.step_ids[1]


@pytest.mark.parametrize(
    "settings",
    [
        "",
        'model = " "',
        'model = "test"\nreasoning = {effort = "huge"}',
        'model = "test"\nreasoning = {typo = true}',
        'model = "test"\ntemperature = 9',
        'model = "test"\nmax_rounds = 0',
        'model = "test"\ntypo = true',
    ],
)
async def test_invalid_resolved_settings(
    history: MemoryHistory, settings: str, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text("[components.llm]\n" + settings)

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(pydantic.ValidationError):
            await llm.LLM("invalid").generate()
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed"


async def test_workspace_compaction_and_explicit_disable(
    history: MemoryHistory,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "test"\n[components.llm.compaction]\nmax_context_characters = 1\nkeep_recent_messages = 1\n'
    )
    backend = FakeProvider([answer("summary"), answer("done"), answer("uncompacted")])

    async def workflow() -> pydantic.JsonValue:
        for name, agent in (
            ("compact", llm.LLM("compact", backend=backend)),
            ("disabled", llm.LLM("disabled", backend=backend, compaction=None)),
        ):
            agent.add_user_message("old")
            agent.add_message(answer("old response"))
            agent.add_user_message(name)
            await agent.generate()
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed"
    assert len(backend.requests) == 3
    assert len(backend.requests[-1].messages) == 3


async def test_round_limit_from_configuration(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "test"\nmax_rounds = 1\n'
    )
    backend = FakeProvider(
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
        agent = llm.LLM("limited", backend=backend)
        agent.add_user_message("hello")
        with pytest.raises(RoundLimitError):
            await agent.run()
        await agent.run(max_rounds=2)
        return None

    result = await replay.run(
        history,
        workflow,
        workspace=tmp_path,
    )
    assert result.outcome.kind == "completed"


async def test_explicit_settings_change_still_rejects_replay(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("done")])
    effort = "high"

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            reasoning=connect.ReasoningConfig.model_validate({"effort": effort}),
            backend=backend,
        )
        agent.add_user_message("hello")
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    effort = "low"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id)
    assert len(backend.requests) == 1
