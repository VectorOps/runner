import json
from pathlib import Path
from unittest.mock import patch

import connect
import pydantic
import pytest
from jsonschema.exceptions import ValidationError

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import pending, settled
from tests.fixtures import sources
from tests.fixtures.user_input import reply
from vectorops.runner.api import tools
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.workflows import settings, tools as workflow_tools


def configure(
    workspace: Path, target: Path, extra: str = "", *, approve: bool = True
) -> None:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(target))}\n'
        '[components.llm.profiles.strong]\nmodel = "strong"\nreasoning = {effort = "high"}\n'
        '[components.llm.profiles.medium]\nmodel = "medium"\n'
        '[components.llm.profiles.weak]\nmodel = "weak"\n'
        '[components.workflows.defaults]\nstrong_model = "strong"\nweak_model = "weak"\n'
        'thinking_level = "high"\n'
        "[components.tools.approval]\napprove_all = "
        + str(approve).lower()
        + "\n"
        + extra
    )


def call(
    name: str, arguments: dict[str, pydantic.JsonValue]
) -> connect.AssistantMessage:
    return connect.AssistantMessage(
        content=[connect.ToolCallBlock(id=name, name=name, arguments=arguments)],
        finish_reason="tool_call",
    )


async def test_helper_context_tools_and_replay(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "AGENTS.md").write_text("Use focused tests and preserve user edits.")
    (target / ".vocode/skills/review").mkdir(parents=True)
    skill_path = target / ".vocode/skills/review/SKILL.md"
    skill_path.write_text(
        "---\nname: review\ndescription: Review code for correctness.\n---\n"
        "Review carefully."
    )
    (target / "evidence.txt").write_text("verified evidence")
    configure(
        tmp_path,
        target,
        '[components.workflows.helper]\nthinking_level = "xhigh"\ntemperature = 0.4\n',
    )
    backend = FakeProvider(
        [
            call("list_files", {"path": "."}),
            call("read_file", {"path": "evidence.txt"}),
            answer("evidence.txt: verified evidence"),
        ]
    )
    runner = WorkflowRunner(tmp_path)
    with patch.object(provider, "ConnectProvider", return_value=backend):
        result = await runner.run(
            "builtin:helper", input={"task": "Read evidence.txt only."}
        )
        assert result == "evidence.txt: verified evidence"
        assert backend.settings[0].model == "weak"
        assert backend.settings[0].temperature == 0.4
        assert backend.settings[0].reasoning is not None
        assert backend.settings[0].reasoning.effort == "xhigh"
        prompt = str(backend.requests[0].system_prompt)
        assert "Use focused tests" in prompt
        assert (
            "- review: Review code for correctness. "
            "(file: .vocode/skills/review/SKILL.md)" in prompt
        )
        assert "read its SKILL.md" in prompt
        assert "Review carefully." not in prompt
        assert prompt.count("## Available project skills") == 1
        assert "300 words" in prompt
        names = {tool.name for tool in backend.requests[0].tools}
        assert names == {"read_file", "list_files", "exec", "poll_process"}
        assert "verified evidence" in str(backend.requests[-1].messages)
        (target / "AGENTS.md").write_text("changed instructions")
        skill_path.unlink()
        (target / "evidence.txt").unlink()
        with (
            patch(
                "vectorops.runner.environment.paths.open_regular",
                side_effect=AssertionError("replay read"),
            ),
            patch("os.scandir", side_effect=AssertionError("replay list")),
        ):
            assert await runner.resume() == result
        assert len(backend.requests) == 3


async def test_parent_delegates_then_patches_and_resumes(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path)
    backend = FakeProvider(
        [
            call(
                "run_agent",
                {"reference": "builtin:helper", "input": {"task": "Report one fact."}},
            ),
            answer("One verified fact."),
            call(
                "apply_patch",
                {
                    "text": "*** Begin Patch\n*** Add File: result.txt\n+done\n*** End Patch"
                },
            ),
            answer("Implemented and checked."),
            answer("Continued."),
        ]
    )
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            await reply(connection, request, "Investigate and implement.")
            next_request = await pending(connection, accepted.execution_id)
            assert next_request.channel == "user.message"
            assert (tmp_path / "result.txt").read_text() == "done\n"
            assert [value.model for value in backend.settings] == [
                "strong",
                "weak",
                "strong",
                "strong",
            ]
            assert "One verified fact." in str(backend.requests[2].messages)
            assert "run_agent" not in {tool.name for tool in backend.requests[1].tools}
            assert "update_plan" in {tool.name for tool in backend.requests[0].tools}
            assert "clear\ndeliverable" in str(backend.requests[0].system_prompt)
            identifier = accepted.execution_id
        (tmp_path / "result.txt").write_text("external change\n")
        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, identifier)
            assert restored.request_id == next_request.request_id
            assert len(backend.requests) == 4
            assert (tmp_path / "result.txt").read_text() == "external change\n"
            await reply(connection, restored, "Continue")
            await pending(connection, identifier)
            assert len(backend.requests) == 5
            await connection.stop(identifier)
            await settled(connection, identifier)


@pytest.mark.parametrize("agents", [[], ["custom:research"]])
async def test_parent_configures_available_agents(
    tmp_path: Path, agents: list[str]
) -> None:
    configure(
        tmp_path,
        tmp_path,
        "[components.workflows.agent]\nagents = " + json.dumps(agents) + "\n",
    )
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/custom.py").write_text(sources.read("research.py"))
    backend = FakeProvider([answer("done")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(
                    reference="builtin:agent",
                    idempotency_key="start",
                )
            )
            await reply(
                connection, await pending(connection, accepted.execution_id), "hello"
            )
            await pending(connection, accepted.execution_id)
            assert backend.settings[0].model == "strong"
            bindings = {tool.name: tool for tool in backend.requests[0].tools}
            assert ("run_agent" in bindings) is bool(agents)
            if agents:
                assert "custom:research" in str(bindings["run_agent"])
                assert "builtin:helper" not in str(bindings["run_agent"])
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)


async def test_helper_exec_requires_normal_approval(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path, approve=False)
    backend = FakeProvider(
        [
            call("exec", {"command": {"kind": "argv", "argv": ["must-not-run"]}}),
            answer("Execution denied."),
        ]
    )
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(
                    reference="builtin:helper",
                    input={"task": "Inspect only."},
                    idempotency_key="start",
                )
            )
            request = await pending(connection, accepted.execution_id)
            assert request.channel == "tool.approval"
            await connection.answer_input(
                mailbox.Answer(
                    execution_id=accepted.execution_id,
                    request_id=request.request_id,
                    response={"decision": "deny"},
                    idempotency_key="deny",
                )
            )
            await settled(connection, accepted.execution_id)
            assert (
                await connection.execution_summary(accepted.execution_id)
            ).status == "completed"
            assert any(
                message.role == "tool" and message.is_error
                for message in backend.requests[-1].messages
            )


async def test_helper_round_limit(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path, "[components.workflows.helper]\nmax_rounds = 1\n")
    backend = FakeProvider([call("list_files", {"path": "."})])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        with pytest.raises(ActivityError, match="round"):
            await WorkflowRunner(tmp_path).run(
                "builtin:helper", input={"task": "Inspect."}
            )
    assert len(backend.requests) == 1


async def test_agent_without_config_reaches_user_input(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_text("Default project instructions.")
    backend = FakeProvider([])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path, workspace_directory=".voc") as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            assert request.channel == "user.message"
            assert not backend.requests
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)
    assert (tmp_path / ".voc/data").is_dir()
    assert not (tmp_path / ".voc/config.toml").exists()


@pytest.mark.parametrize("use_profile", [False, True])
async def test_agent_uses_strong_profile_or_shared_model(
    tmp_path: Path, use_profile: bool
) -> None:
    config = (
        '[components.llm]\nmodel = "shared"\n'
        '[components.workflows.defaults]\nstrong_model = "legacy"\n'
    )
    if use_profile:
        config += '[components.llm.profiles.strong]\nmodel = "profile"\n'
    (tmp_path / "config.toml").write_text(config)
    backend = FakeProvider([answer("done")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(
                    reference="builtin:agent",
                    idempotency_key="start",
                )
            )
            request = await pending(connection, accepted.execution_id)
            await reply(connection, request, "hello")
            next_request = await pending(connection, accepted.execution_id)
            assert next_request.request_id != request.request_id
            assert next_request.channel == "user.message"
            assert len(backend.requests) == 1
            assert backend.settings[0].model == ("profile" if use_profile else "shared")
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)


async def test_helper_uses_default_project_context(tmp_path: Path) -> None:
    workspace = tmp_path / ".voc"
    workspace.mkdir()
    (workspace / "config.toml").write_text('[components.llm]\nmodel = "test"\n')
    (tmp_path / "AGENTS.md").write_text("Default project instructions.")
    (workspace / "AGENTS.md").write_text("Wrong runner directory instructions.")
    backend = FakeProvider([answer("Inspected.")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        runner = WorkflowRunner(tmp_path, workspace_directory=".voc")
        assert (
            await runner.run("builtin:helper", input={"task": "Inspect."})
            == "Inspected."
        )
    prompt = str(backend.requests[0].system_prompt)
    assert "Default project instructions." in prompt
    assert "Wrong runner directory instructions." not in prompt


@pytest.mark.parametrize("task", ["", " ", "\n", "x" * 65537])
async def test_helper_task_validation(tmp_path: Path, task: str) -> None:
    resolver = (await WorkflowRunner(tmp_path).project()).resolver
    descriptor = await resolver.describe("builtin:helper")
    with pytest.raises(ValidationError):
        descriptor.validate_input({"task": task})


@pytest.mark.parametrize("agents", [["invalid"], ["builtin:helper", "builtin:helper"]])
def test_agent_configuration_validation(agents: list[str]) -> None:
    with pytest.raises(pydantic.ValidationError):
        settings.AgentSettings(agents=agents)


def test_helper_overrides_and_tool_boundaries() -> None:
    defaults = settings.Settings(weak_model="cheap", thinking_level="high")
    assert settings.HelperSettings().llm_overrides(defaults) == defaults.llm_overrides(
        settings.ModelTier.WEAK
    )
    for invalid in (
        {"max_rounds": 0},
        {"thinking_level": "huge"},
        {"temperature": 3},
        {"max_rounds": True},
    ):
        with pytest.raises(pydantic.ValidationError):
            settings.HelperSettings.model_validate(invalid)
    helper = {binding.definition.name for binding in workflow_tools.helper_tools()}
    assert not helper.intersection(
        {"apply_patch", "run_agent", "write_stdin", "update_plan"}
    )
    assert tools.apply_patch_tool().definition.name == "apply_patch"
