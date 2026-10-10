import json
from pathlib import Path
from unittest.mock import patch

import connect
import pytest
from jsonschema.exceptions import ValidationError

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import command, pending, settled
from vectorops.runner import cli
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.workflow import WorkflowRunner


async def reply(
    connection: client.RunnerClient, request: mailbox.InputRequest, text: str
) -> None:
    await connection.answer_input(
        mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response={"content": [{"type": "text", "text": text}]},
            idempotency_key=request.request_id,
        )
    )


@pytest.mark.parametrize("tier", ["strong", "medium", "weak"])
async def test_chat_start_turns_and_restart(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], tier: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "fallback"\n'
        '[components.workflows.defaults]\nstrong_model = "strong-model"\n'
        'medium_model = "medium-model"\nweak_model = "weak-model"\n'
        'thinking_level = "high"\n'
    )
    backend = FakeProvider([answer("first"), answer("second")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            await command(
                connection,
                "execution",
                "start",
                "builtin:chat",
                "--input",
                json.dumps({"model_tier": tier}) if tier != "strong" else "{}",
                "--json",
            )
            identifier = json.loads(capsys.readouterr().out)["execution_id"]
            request = await pending(connection, identifier)
            assert request.request_type == "chat.message.v1"
            assert backend.requests == []
            assert (await connection.execution_summary(identifier)).reference == (
                "builtin:chat"
            )
            await reply(connection, request, "hello")
            next_request = await pending(connection, identifier)
            assert next_request.request_id != request.request_id
            assert len(backend.requests) == 1
            assert backend.settings[0].model == f"{tier}-model"
            assert backend.settings[0].reasoning is not None
            assert backend.settings[0].reasoning.effort == "high"
            assert not backend.requests[0].tools

        path.write_text('[components.workflows.defaults]\nstrong_model = "changed"\n')
        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, identifier)
            assert restored.request_id == next_request.request_id
            assert len(backend.requests) == 1
            await reply(connection, restored, "continue")
            third = await pending(connection, identifier)
            assert third.request_id != restored.request_id
            assert len(backend.requests) == 2
            assert backend.settings[1].model == f"{tier}-model"
            assert [message.role for message in backend.requests[1].messages] == [
                "user",
                "assistant",
                "user",
            ]
            assert backend.requests[1].messages[1].content == [
                connect.TextBlock(text="first")
            ]
            await command(connection, "execution", "stop", identifier)
            await settled(connection, identifier)
            assert (
                await connection.execution_summary(identifier)
            ).status == "cancelled"


async def test_builtin_chat_as_subagent(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    (tmp_path / "workflows/parent.py").write_text(
        "import connect\n"
        "from vectorops.runner.api import llm\n"
        "async def main(ctx):\n"
        '    tool = await llm.subagent_tool(["builtin:chat"])\n'
        "    return (await llm.invoke_tool(tool, connect.ToolCallBlock(\n"
        '        id="chat", name="run_agent", arguments={"reference": "builtin:chat"}\n'
        '    ))).model_dump(mode="json")\n'
    )
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            protocol.Start(reference="parent:main", idempotency_key="child")
        )
        request = await pending(connection, accepted.execution_id)
        summary = await connection.execution_summary(accepted.execution_id)
        assert request.workflow_id != summary.root_workflow_id
        output = tmp_path / "history.jsonl"
        await WorkflowRunner(tmp_path).export_execution(output, accepted.execution_id)
        header = json.loads(output.read_text().splitlines()[0])
        assert {item["definition_ref"] for item in header["workflows"]} == {
            "parent:main",
            "builtin:chat",
        }
        await command(connection, "execution", "stop", accepted.execution_id)
        await settled(connection, accepted.execution_id)


def test_foreground_start_accepts_builtin_reference(tmp_path: Path) -> None:
    args = cli.parser().parse_args(
        ["--project", str(tmp_path), "start", "builtin:chat"], namespace=cli.Arguments()
    )
    assert args.workflow == "builtin:chat"
    assert args.input == {}


async def test_chat_rejects_invalid_tier(tmp_path: Path) -> None:
    descriptor = await (await WorkflowRunner(tmp_path).project()).resolver.describe(
        "builtin:chat"
    )
    with pytest.raises(ValidationError):
        descriptor.validate_input({"model_tier": "unknown"})
