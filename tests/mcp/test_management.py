import asyncio
import json
import os
from pathlib import Path

import pydantic
import pytest
from mcp import types

from tests.fixtures import mcp_workflows
from tests.mcp.test_replay import configure
from tests.mcp.test_runtime import server
from vectorops.runner.authentication import models as am
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control.client import RunnerClient
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.control.protocol import RemoteError
from vectorops.runner.mcp import models
from vectorops.runner.mcp.management import Query
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_management_tcp_and_workflow_reuse_one_connection(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure(workspace)
    monkeypatch.setattr(mcp_workflows, "connected", asyncio.Event())
    monkeypatch.setattr(mcp_workflows, "release", asyncio.Event())
    async with RunnerService(workspace) as service:
        transport = RunnerServer(
            service, (await service.configuration.load()).control_token
        )
        port = await transport.listen()
        try:
            async with await RunnerClient.tcp(port) as client:
                assert [item.name for item in (await client.mcp_servers()).servers] == [
                    "test"
                ]
                first = await client.mcp_connect("test")
                assert first.connected
                assert (await client.mcp_discover("test")).tools
                task = asyncio.create_task(
                    service.runner.run("python:tests.fixtures.mcp_workflows:shared")
                )
                try:
                    await asyncio.wait_for(mcp_workflows.connected.wait(), 5)
                    status = await client.mcp_status("test")
                    assert status.session_id == first.session_id and status.leases == 1
                    assert len(service.mcp_pool.runtime._connections) == 1
                    with pytest.raises(RemoteError, match="leased"):
                        await client.mcp_disconnect("test")
                    with pytest.raises(RemoteError, match="leased"):
                        await client.mcp_roots("test", [models.Root(uri="file:///new")])
                    mcp_workflows.release.set()
                    assert await task == ["test"]
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                assert (await client.mcp_status("test")).connected
                updated = await client.mcp_roots(
                    "test", [models.Root(uri="file:///new")]
                )
                assert updated.connected and updated.session_id != first.session_id
                assert (await client.mcp_discover("test")).server.roots == updated.roots
                await client.mcp_disconnect("test")
                assert not (await client.mcp_status("test")).connected
        finally:
            await transport.aclose()


async def test_pool_isolation_release_and_invalidation() -> None:
    events: list[dict[str, pydantic.JsonValue]] = []
    pool = SharedPool(AuthenticationService(), events.append)
    first = MCPRuntime(pool.authentication, "first", pool=pool)
    second = MCPRuntime(pool.authentication, "second", pool=pool)
    config = server()
    try:
        await asyncio.gather(
            first.discover("test", config), second.discover("test", config)
        )
        assert pool.status(config).leases == 2
        connection = next(iter(pool.runtime._connections.values()))
        connection.tools_list_changed = True
        await connection._message(
            types.ServerNotification(
                types.ToolListChangedNotification(
                    method="notifications/tools/list_changed"
                )
            )
        )
        assert pool.status(config).catalog_stale
        assert events[0]["kind"] == "mcp.catalog_changed"
        await first.close()
        assert pool.status(config).leases == 1
        assert (
            await second.call(config, "echo", {"value": "live"})
        ).structuredContent == {"value": "live"}
        await second.discover("test", config)
        assert not pool.status(config).catalog_stale
        with pytest.raises(MCPError):
            await pool.disconnect(config)
        await second.close()
        await pool.disconnect(config)
    finally:
        await asyncio.gather(first.close(), second.close())
        await pool.close()


def test_configuration_roots_merge_headers_and_secret_guards() -> None:
    settings = models.Settings.model_validate(
        {
            "roots": [{"path": "/project"}],
            "servers": {
                "inherited": {"kind": "stdio", "command": "unused"},
                "empty": {"kind": "stdio", "command": "unused", "roots": []},
                "merged": {
                    "kind": "stdio",
                    "command": "unused",
                    "roots_mode": "merge",
                    "roots": [{"path": "/other"}],
                },
            },
        }
    )
    restored = models.Settings.model_validate(settings.model_dump(mode="json"))
    assert len(restored.server("inherited").roots) == 1
    assert len(restored.server("empty").roots) == 0
    assert len(restored.server("merged").roots) == 2
    for headers in (
        {"Host": "evil"},
        {"X-Foo": "one", "x-foo": "two"},
        {"X-Foo": "bad\r\nvalue"},
        {"Authorization": "secret"},
    ):
        with pytest.raises(pydantic.ValidationError):
            models.HTTPServer(url="https://example.test/mcp", headers=headers)


async def test_stdio_environment_and_secret_precedence() -> None:
    auth = AuthenticationService()
    await auth.provision(
        am.Credential(provider="mcp", profile="test", kind="api_key", secret="private")
    )
    config = server().model_copy(
        update={
            "env": {"MCP_TEST": "ordinary"},
            "env_credentials": {"MCP_SECRET": "test"},
        }
    )
    runtime = MCPRuntime(auth)
    try:
        result = await runtime.call(config, "echo", {"value": "environment"})
        assert result.structuredContent == {"value": "ordinary:private"}
    finally:
        await runtime.close()


async def test_workflow_helpers_and_override_replay(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure(workspace)
    async with RunnerService(workspace) as service:
        base = await service.mcp.server(Query(server="test"))
        await service.mcp_pool.roots(base, [models.Root(uri="file:///selected")])
        first = await service.runner.run("python:tests.fixtures.mcp_workflows:helpers")
        identifier = service.runner.last_execution_id
        assert identifier is not None
        await service.mcp_pool.roots(base, None)

        def forbidden(*args: object, **kwargs: object) -> None:
            raise AssertionError("replay contacted MCP")

        monkeypatch.setattr(MCPRuntime, "discover_content", forbidden)
        monkeypatch.setattr(MCPRuntime, "retrieve_content", forbidden)
        assert await service.runner.resume(identifier) == first


async def test_server_notification_reaches_control_subscriber(workspace: Path) -> None:
    config = server("notify")
    (workspace / "config.toml").write_text(
        '[components.mcp.servers.test]\nkind = "stdio"\n'
        f"command = {json.dumps(config.command)}\nargs = {json.dumps(config.args)}\n"
    )
    async with RunnerService(workspace) as service:
        transport = RunnerServer(
            service, (await service.configuration.load()).control_token
        )
        port = await transport.listen()
        runtime = MCPRuntime(service.authentication, "test", pool=service.mcp_pool)
        try:
            async with await RunnerClient.tcp(port) as client:
                await client.subscribe()
                catalog = await client.mcp_discover("test")
                await runtime.call(catalog.server, "echo", {"value": "notify"})
                async with asyncio.timeout(3):
                    while True:
                        event = await client.next_event()
                        if event.event.get("kind") == "mcp.catalog_changed":
                            break
                assert (await client.mcp_status("test")).catalog_stale
                await client.mcp_refresh("test")
                assert not (await client.mcp_status("test")).catalog_stale
        finally:
            await runtime.close()
            await transport.aclose()


async def test_shutdown_cancels_management_startup_and_reaps_process(
    workspace: Path,
) -> None:
    pid_path = workspace / "pid"
    config = server("hang", str(pid_path))
    (workspace / "config.toml").write_text(
        '[components.mcp.servers.test]\nkind = "stdio"\n'
        f"command = {json.dumps(config.command)}\nargs = {json.dumps(config.args)}\n"
        "[components.mcp.servers.test.limits]\nshutdown_seconds = 0.1\n"
    )
    service = RunnerService(workspace)
    await service.__aenter__()
    task = asyncio.create_task(service.dispatch("mcp.connect", {"server": "test"}))
    try:
        async with asyncio.timeout(3):
            while not pid_path.exists():
                await asyncio.sleep(0.01)
        await asyncio.wait_for(service.aclose(), 3)
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid_path.read_text()), 0)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.aclose()
