from dataclasses import replace
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
from vectorops.runner import global_config, skills
from vectorops.runner.api import tools
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.workflows import registry, settings, tools as workflow_tools


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
    (target / ".voc/skills/review").mkdir(parents=True)
    skill_path = target / ".voc/skills/review/SKILL.md"
    skill_path.write_text(
        "---\nname: review\ndescription: Review code for correctness.\n---\n"
        "Review carefully."
    )
    (target / "evidence.txt").write_text("verified evidence")
    external_skill = tmp_path / "shared-skills/external/SKILL.md"
    external_skill.parent.mkdir(parents=True)
    external_skill.write_text(
        "---\nname: external\ndescription: External guidance.\n---\nExternal skill body."
    )
    builtin_skill = skills.BUNDLED_SKILLS / "workflow-authoring/SKILL.md"
    configure(
        tmp_path,
        target,
        '[components.workflows.helper]\nthinking_level = "xhigh"\ntemperature = 0.4\n'
        '[components.skills]\ndirectories = ["../shared-skills"]\n',
    )
    backend = FakeProvider(
        [
            call("list_files", {"path": "."}),
            call("read_file", {"path": "evidence.txt"}),
            call("read_file", {"path": str(external_skill)}),
            call("read_file", {"path": str(builtin_skill)}),
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
            "(file: .voc/skills/review/SKILL.md)" in prompt
        )
        assert "read its SKILL.md" in prompt
        assert "Review carefully." not in prompt
        assert f"(file: {external_skill})" in prompt
        assert f"(file: {builtin_skill})" in prompt
        assert "External skill body." not in prompt
        assert prompt.count("## Available skills") == 1
        assert "300 words" in prompt
        names = {tool.name for tool in backend.requests[0].tools}
        assert names == {"read_file", "list_files", "exec", "poll_process"}
        assert "verified evidence" in str(backend.requests[-1].messages)
        assert "External skill body." in str(backend.requests[-1].messages)
        assert "# Workflow authoring" in str(backend.requests[-1].messages)
        (target / "AGENTS.md").write_text("changed instructions")
        skill_path.unlink()
        external_skill.unlink()
        (target / "evidence.txt").unlink()
        await (await runner.project()).configuration.set(
            "skills", {"directories": [""]}
        )
        with (
            patch(
                "vectorops.runner.environment.paths.open_regular",
                side_effect=AssertionError("replay read"),
            ),
            patch("os.scandir", side_effect=AssertionError("replay list")),
        ):
            assert await runner.resume() == result
        assert len(backend.requests) == 5


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


async def test_helper_default_round_limit_is_unlimited(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path)
    backend = FakeProvider([call("unknown", {}) for _ in range(31)] + [answer("done")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        assert (
            await WorkflowRunner(tmp_path).run(
                "builtin:helper", input={"task": "Inspect."}
            )
            == "done"
        )
    assert len(backend.requests) == 32


@pytest.mark.parametrize("section", ["workflows.helper", "llm"])
async def test_helper_round_limit(tmp_path: Path, section: str) -> None:
    configure(tmp_path, tmp_path, f"[components.{section}]\nmax_rounds = 1\n")
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
@pytest.mark.parametrize("user_defaults", [False, True])
async def test_agent_uses_strong_profile_or_shared_model(
    tmp_path: Path, use_profile: bool, user_defaults: bool
) -> None:
    config = (
        '[components.llm]\nmodel = "shared"\n'
        '[components.workflows.defaults]\nstrong_model = "legacy"\n'
    )
    if use_profile:
        config += '[components.llm.profiles.strong]\nmodel = "profile"\n'
    directory = global_config.directory() if user_defaults else tmp_path
    directory.mkdir(exist_ok=True)
    (directory / "config.toml").write_text(config)
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
    if user_defaults:
        assert not (tmp_path / "config.toml").exists()


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


@pytest.mark.parametrize("version", [2, 3, 4, 5])
async def test_agent_web_fetch_defaults_and_versioned_resume(
    tmp_path: Path, version: int
) -> None:
    configure(tmp_path, tmp_path)
    backend = FakeProvider([answer("done"), answer("continued")])
    workflows = {
        **registry.WORKFLOWS,
        "agent": replace(registry.agent, version=version),
    }
    with patch.object(provider, "ConnectProvider", return_value=backend):
        with patch.object(registry, "WORKFLOWS", workflows):
            async with client.embedded(tmp_path) as connection:
                accepted = await connection.start(
                    protocol.Start(reference="builtin:agent", idempotency_key="start")
                )
                await reply(
                    connection,
                    await pending(connection, accepted.execution_id),
                    "hello",
                )
                waiting = await pending(connection, accepted.execution_id)
                names = {tool.name for tool in backend.requests[0].tools}
                assert ("web_fetch" in names) == (version >= 4)
                assert "prefer web_fetch" in str(backend.requests[0].system_prompt)
        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, accepted.execution_id)
            assert restored.request_id == waiting.request_id
            assert len(backend.requests) == 1
            await reply(connection, restored, "continue")
            await pending(connection, accepted.execution_id)
            assert len(backend.requests) == 2
            assert {tool.name for tool in backend.requests[-1].tools} == names
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)


def test_web_fetch_is_agent_only_by_default() -> None:
    collections = workflow_tools.default_collections([])
    assert "web_fetch" in collections["agent"].tools
    assert "web_fetch" not in collections["helper"].tools


@pytest.mark.parametrize(
    "version,policy,compacts",
    [
        (4, "omitted", False),
        (5, "omitted", True),
        (5, "custom", False),
        (5, "disabled", False),
    ],
)
async def test_agent_default_compaction_threshold_and_versioned_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version: int,
    policy: str,
    compacts: bool,
) -> None:
    configure(tmp_path, tmp_path)
    monkeypatch.setattr(
        connect,
        "default_model_registry",
        connect.ModelRegistry(
            [
                connect.ModelSpec(
                    provider="test",
                    model="strong",
                    api_family="test",
                    context_window=1000000,
                    max_output_tokens=128000,
                )
            ]
        ),
    )
    responses = []
    for tokens in (100000, 200000, 300000, 500000, 790000, 800000):
        response = answer("Progress")
        response.usage = connect.Usage(
            input_tokens=tokens, output_tokens=10, completeness="final"
        )
        responses.append(response)
    if compacts:
        responses.append(
            answer("The user requires verified behavior. Implementation is unfinished.")
        )
    responses.append(answer("Continued"))
    backend = FakeProvider(responses)
    workflows = {
        **registry.WORKFLOWS,
        "agent": replace(registry.agent, version=version),
    }
    with patch.object(provider, "ConnectProvider", return_value=backend):
        with patch.object(registry, "WORKFLOWS", workflows):
            async with client.embedded(tmp_path) as connection:
                if policy != "omitted":
                    await connection.set_configuration(
                        "components.llm.profiles.strong.compaction",
                        (
                            None
                            if policy == "disabled"
                            else {"trigger_threshold_ratio": 0.9}
                        ),
                    )
                accepted = await connection.start(
                    protocol.Start(reference="builtin:agent", idempotency_key="start")
                )
                waiting = await pending(connection, accepted.execution_id)
                for index in range(6):
                    await reply(
                        connection,
                        waiting,
                        "Preserve the user requirements and verify behavior. " * 200,
                    )
                    waiting = await pending(connection, accepted.execution_id)
                    assert len(backend.requests) == index + 1 + int(
                        compacts and index == 5
                    )
                if compacts:
                    summary = backend.requests[-1]
                    assert summary.tools == []
                    assert str(summary.system_prompt).startswith(
                        "Summarize this conversation"
                    )
                    assert "Preserve the user requirements" in str(summary.messages)
        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, accepted.execution_id)
            assert restored.request_id == waiting.request_id
            assert len(backend.requests) == 6 + int(compacts)
            await reply(connection, restored, "Continue")
            await pending(connection, accepted.execution_id)
            assert len(backend.requests) == 7 + int(compacts)
            if compacts:
                assert "generated continuation summary" in str(
                    backend.requests[-1].messages[0].content
                )
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)
