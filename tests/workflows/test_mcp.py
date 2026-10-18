from collections.abc import AsyncGenerator
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import patch

import connect
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import pending, settled
from tests.fixtures.user_input import reply
from tests.mcp.test_runtime import server
from tests.workflows.test_agent import call, configure
from vectorops.runner.api import llm
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem
from vectorops.runner.workflows import registry


class MCPProvider(FakeProvider):
    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        names = [tool.name for tool in request.tools]
        results = [message for message in request.messages if message.role == "tool"]
        search = next(
            (name for name in names if name.startswith("mcp_discover_")), None
        )
        echo = next((name for name in names if name.startswith("mcp_test_echo_")), None)
        if not results and search is not None:
            self.responses.append(call(search, {"query": "echo"}))
        elif echo is not None and not any(
            result.tool_name == echo for result in results
        ):
            self.responses.append(call(echo, {"value": "configured MCP"}))
        else:
            self.responses.append(answer("done"))
        async for item in super().stream(context, settings, request):
            yield item


def mcp_config(mode: str = "dynamic", extra: str = "") -> str:
    connection = server()
    return (
        f'[mcp]\nmode = "{mode}"\nmax_active = 1\n'
        '[mcp.servers.test]\nkind = "stdio"\n'
        f"command = {json.dumps(connection.command)}\nargs = {json.dumps(connection.args)}\n"
        + extra
    )


@pytest.mark.parametrize("mode", ["dynamic", "static"])
async def test_default_agent_mcp_modes_and_offline_replay(
    tmp_path: Path, mode: str
) -> None:
    configure(tmp_path, tmp_path, mcp_config(mode))
    backend = MCPProvider([])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            await reply(
                connection, await pending(connection, accepted.execution_id), "Use echo"
            )
            waiting = await pending(connection, accepted.execution_id)
            first = [tool.name for tool in backend.requests[0].tools]
            assert "read_file" in first
            assert "run_agent" in first
            assert any(name.startswith("mcp_discover_") for name in first) == (
                mode == "dynamic"
            )
            assert any(name.startswith("mcp_test_echo_") for name in first) == (
                mode == "static"
            )
            assert len(backend.requests) == (3 if mode == "dynamic" else 2)
            assert "configured MCP" in str(backend.requests[-1].messages)
            assert not any(
                message.is_error
                for message in backend.requests[-1].messages
                if message.role == "tool"
            )
            if mode == "dynamic":
                assert (
                    len(backend.requests[1].tools) == len(backend.requests[0].tools) + 1
                )
            count = len(backend.requests)
        configure(tmp_path, tmp_path)
        with (
            patch.object(
                MCPRuntime, "discover", side_effect=AssertionError("replay discovery")
            ),
            patch.object(MCPRuntime, "call", side_effect=AssertionError("replay call")),
        ):
            async with client.embedded(tmp_path) as connection:
                restored = await pending(connection, accepted.execution_id)
                assert restored.request_id == waiting.request_id
                assert len(backend.requests) == count
                await connection.stop(accepted.execution_id)
                await settled(connection, accepted.execution_id)


@pytest.mark.parametrize("extra", ['tools = ["missing"]\n', 'exclude = ["echo"]\n'])
async def test_default_agent_filters_remove_all_mcp_tools(
    tmp_path: Path, extra: str
) -> None:
    configure(tmp_path, tmp_path, mcp_config(extra=extra))
    backend = FakeProvider([answer("done")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            await reply(
                connection, await pending(connection, accepted.execution_id), "hello"
            )
            await pending(connection, accepted.execution_id)
            assert not any(
                tool.name.startswith("mcp_") for tool in backend.requests[0].tools
            )
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)


async def test_agent_v2_resume_keeps_legacy_collection(tmp_path: Path) -> None:
    configure(
        tmp_path,
        tmp_path,
        '[mcp.servers.unused]\nkind = "stdio"\ncommand = "must-not-run"\n',
    )
    backend = FakeProvider([answer("old"), answer("continued")])
    workflows = {**registry.WORKFLOWS, "agent": replace(registry.agent, version=2)}
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
        with patch.object(
            MCPRuntime, "discover_current", side_effect=AssertionError("legacy MCP I/O")
        ):
            async with client.embedded(tmp_path) as connection:
                restored = await pending(connection, accepted.execution_id)
                assert restored.request_id == waiting.request_id
                assert len(backend.requests) == 1
                await reply(connection, restored, "continue")
                await pending(connection, accepted.execution_id)
                assert len(backend.requests) == 2
                assert not any(
                    tool.name.startswith("mcp_") for tool in backend.requests[-1].tools
                )
                await connection.stop(accepted.execution_id)
                await settled(connection, accepted.execution_id)


async def test_default_agent_requires_concrete_mcp_approval(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path, mcp_config(), approve=False)
    backend = MCPProvider([])
    with (
        patch.object(provider, "ConnectProvider", return_value=backend),
        patch.object(MCPRuntime, "call", side_effect=AssertionError("denied MCP call")),
    ):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            await reply(
                connection, await pending(connection, accepted.execution_id), "Use echo"
            )
            for prefix, decision in (
                ("mcp_discover_", "approve"),
                ("mcp_test_echo_", "deny"),
            ):
                request = await pending(connection, accepted.execution_id)
                assert request.channel == "tool.approval"
                assert isinstance(request.payload, dict)
                invoked = connect.ToolCallBlock.model_validate(request.payload["call"])
                assert invoked.name.startswith(prefix)
                await connection.answer_input(
                    mailbox.Answer(
                        execution_id=accepted.execution_id,
                        request_id=request.request_id,
                        response={"decision": decision, "scope": "call"},
                        idempotency_key=request.request_id,
                    )
                )
            await pending(connection, accepted.execution_id)
            results = [
                message
                for message in backend.requests[-1].messages
                if message.role == "tool"
            ]
            assert not results[0].is_error
            assert results[1].is_error
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)
