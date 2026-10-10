from pathlib import Path
from unittest.mock import patch

import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import pending, settled
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from tests.workflows.test_agent import configure
from tests.workflows.test_chat import reply
from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.workflows import prompts, settings


@pytest.mark.parametrize("model", [settings.AgentSettings, settings.HelperSettings])
@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({}, "default\n\ncontext"),
        ({"system_prompt": None, "system_append": None}, "default\n\ncontext"),
        ({"system_prompt": "custom"}, "custom\n\ncontext"),
        ({"system_append": "extra"}, "default\n\nextra\n\ncontext"),
        (
            {"system_prompt": "custom", "system_append": "extra"},
            "custom\n\nextra\n\ncontext",
        ),
        ({"system_prompt": "", "system_append": ""}, "\n\ncontext"),
        ({"system_append": ""}, "default\n\ncontext"),
        ({"system_prompt": "  ${MODEL}\n"}, "  ${MODEL}\n\n\ncontext"),
    ],
)
def test_composition(
    model: type[settings.PromptSettings],
    options: dict[str, pydantic.JsonValue],
    expected: str,
) -> None:
    assert (
        model.model_validate(options).compose_prompt("default", "context") == expected
    )


@pytest.mark.parametrize("model", [settings.AgentSettings, settings.HelperSettings])
@pytest.mark.parametrize("field", ["system_prompt", "system_append"])
@pytest.mark.parametrize("value", [1, True, [], {}])
def test_invalid_prompt_types(
    model: type[settings.PromptSettings], field: str, value: pydantic.JsonValue
) -> None:
    with pytest.raises(pydantic.ValidationError):
        model.model_validate({field: value})


@pytest.mark.parametrize("role", ["agent", "helper"])
async def test_recorded_settings_replay(
    role: str, tmp_path: Path, history: MemoryHistory
) -> None:
    model = settings.AgentSettings if role == "agent" else settings.HelperSettings

    async def workflow() -> pydantic.JsonValue:
        options = model.model_validate(
            await get_config(
                composition.workflow_context(), f"workflows.{role}", {}, model=model
            )
        )
        assert options.system_prompt is None
        assert options.compose_prompt("default", "context") == "default\n\ncontext"
        return options.model_dump(mode="json")

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    (tmp_path / "config.toml").write_text(
        f'[components.workflows.{role}]\nsystem_prompt = "new"\n'
    )
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome


@pytest.mark.parametrize("replace", [False, True])
async def test_helper_prompt_and_replay(tmp_path: Path, replace: bool) -> None:
    extra = (
        '[components.workflows.helper]\nsystem_append = "Additional helper rules."\n'
    )
    if replace:
        extra += 'system_prompt = "Replacement helper."\n'
    configure(tmp_path, tmp_path, extra)
    (tmp_path / "AGENTS.md").write_text("Repository-specific instructions.")
    backend = FakeProvider([answer("done")])
    runner = WorkflowRunner(tmp_path)
    with patch.object(provider, "ConnectProvider", return_value=backend):
        assert await runner.run("builtin:helper", input={"task": "Inspect."}) == "done"
        prompt = backend.requests[0].system_prompt
        assert isinstance(prompt, str)
        base = "Replacement helper." if replace else prompts.HELPER
        assert prompt.startswith(base + "\n\nAdditional helper rules.\n\n")
        assert "Repository-specific instructions." in prompt
        configure(
            tmp_path,
            tmp_path,
            '[components.workflows.helper]\nsystem_prompt = "Changed"\n',
        )
        assert await runner.resume() == "done"
        assert len(backend.requests) == 1


@pytest.mark.parametrize("replace", [False, True])
async def test_agent_prompt_retained_across_restart(
    tmp_path: Path, replace: bool
) -> None:
    extra = '[components.workflows.agent]\nagents = []\nsystem_append = "Additional agent rules."\n'
    if replace:
        extra += 'system_prompt = "Replacement agent."\n'
    configure(tmp_path, tmp_path, extra)
    (tmp_path / "AGENTS.md").write_text("Repository-specific instructions.")
    backend = FakeProvider([answer("first"), answer("second")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            await reply(
                connection, await pending(connection, accepted.execution_id), "hello"
            )
            request = await pending(connection, accepted.execution_id)
            prompt = backend.requests[0].system_prompt
            assert isinstance(prompt, str)
            base = "Replacement agent." if replace else prompts.AGENT
            assert prompt.startswith(base + "\n\nAdditional agent rules.\n\n")
            assert "Repository-specific instructions." in prompt
        configure(
            tmp_path,
            tmp_path,
            '[components.workflows.agent]\nsystem_prompt = "Changed"\n',
        )
        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, accepted.execution_id)
            assert restored.request_id == request.request_id
            await reply(connection, restored, "continue")
            await pending(connection, accepted.execution_id)
            assert len(backend.requests) == 2
            assert backend.requests[1].system_prompt == prompt
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)
