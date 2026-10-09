import asyncio
import json
from pathlib import Path
import sys

import connect
from mcp import types
import pydantic
import pytest

from vectorops.runner.mcp import models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.workflow import WorkflowRunner


def configure(workspace: Path, *, approve: bool = True) -> None:
    server_path = Path(__file__).parents[1] / "fixtures" / "mcp_server.py"
    (workspace / "config.toml").write_text(
        f"[components.tools.approval]\napprove_all = {str(approve).lower()}\n"
        '[components.mcp.servers.test]\nkind = "stdio"\n'
        f"command = {json.dumps(sys.executable)}\nargs = [{json.dumps(str(server_path))}]\n"
    )


async def test_sqlite_replay_does_not_contact_server_or_read_live_configuration(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(workspace)
    runner = WorkflowRunner(workspace)
    first = await runner.run("python:tests.fixtures.mcp_workflows:echo")
    response = connect.ToolResultMessage.model_validate(first)
    assert not response.is_error
    assert isinstance(response.content[0], connect.TextBlock)
    assert response.content[0].text == "hello"
    identifier = runner.last_execution_id
    assert identifier is not None

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("completed replay contacted MCP")

    monkeypatch.setattr(MCPRuntime, "discover", forbidden)
    monkeypatch.setattr(MCPRuntime, "call", forbidden)
    (workspace / "config.toml").write_text("")
    assert await WorkflowRunner(workspace).resume(identifier) == first


@pytest.mark.parametrize("approved", [True, False])
async def test_approval_controls_remote_calls_and_replays_outcome(
    workspace: Path,
    approved: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(workspace, approve=False)
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("python:tests.fixtures.mcp_workflows:echo"))
    calls: list[str] = []
    original = MCPRuntime.call

    async def tracked(
        self: MCPRuntime,
        server: models.ServerConfig,
        name: str,
        arguments: dict[str, pydantic.JsonValue],
    ) -> types.CallToolResult:
        calls.append("call")
        return await original(self, server, name, arguments)

    monkeypatch.setattr(MCPRuntime, "call", tracked)
    async with asyncio.timeout(5):
        while True:
            identifier = runner.last_execution_id
            if identifier is not None:
                page = await runner.pending_inputs(
                    mailbox.RequestQuery(execution_id=identifier)
                )
                if page.requests:
                    break
            await asyncio.sleep(0.01)
    assert calls == []
    request = page.requests[0]
    await runner.answer_input(
        mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response={"approved": approved},
            idempotency_key="answer",
        )
    )
    result = await task
    assert connect.ToolResultMessage.model_validate(result).is_error is not approved
    assert calls == (["call"] if approved else [])
    assert await runner.resume(identifier) == result
    assert calls == (["call"] if approved else [])


async def test_invalid_arguments_do_not_request_approval(workspace: Path) -> None:
    configure(workspace, approve=False)
    runner = WorkflowRunner(workspace)
    result = await runner.run(
        "python:tests.fixtures.mcp_workflows:echo", input={"value": 1}
    )
    assert connect.ToolResultMessage.model_validate(result).is_error
    assert runner.last_execution_id is not None
    assert not (
        await runner.pending_inputs(
            mailbox.RequestQuery(execution_id=runner.last_execution_id)
        )
    ).requests


async def test_invalid_secret_configuration_never_enters_history(
    workspace: Path,
) -> None:
    configure(workspace)
    with (workspace / "config.toml").open("a") as stream:
        stream.write('env = { TOKEN = "must-not-persist" }\n')
    runner = WorkflowRunner(workspace)
    with pytest.raises(ActivityError, match="component configuration is invalid"):
        await runner.run("python:tests.fixtures.mcp_workflows:echo")
    assert runner.last_execution_id is not None
    output = workspace / "history.jsonl"
    await runner.export_execution(output, runner.last_execution_id)
    assert "must-not-persist" not in output.read_text()


async def test_child_borrows_root_runtime(workspace: Path) -> None:
    assert (
        await WorkflowRunner(workspace).run(
            "python:tests.fixtures.mcp_workflows:parent"
        )
        is True
    )
