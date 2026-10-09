import asyncio
from pathlib import Path
import os
import sys

import pytest

from vectorops.runner.mcp import models
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError


def server(mode: str = "normal", pid: str = "") -> models.StdioServer:
    path = Path(__file__).parents[1] / "fixtures" / "mcp_server.py"
    args = [str(path), mode]
    if pid:
        args.append(pid)
    return models.StdioServer(
        command=sys.executable,
        args=args,
        limits=models.Limits(startup_seconds=2, request_seconds=2, shutdown_seconds=2),
    )


async def test_stdio_discovery_calls_parallel_acquisition_and_shutdown(
    tmp_path: Path,
) -> None:
    runtime = MCPRuntime()
    config = server(pid=str(tmp_path / "pid"))
    try:
        first, second = await asyncio.gather(
            runtime.discover("test", config), runtime.discover("test", config)
        )
        assert first == second
        assert len(runtime._connections) == 1
        results = await asyncio.gather(
            *(runtime.call(config, "echo", {"value": str(i)}) for i in range(3))
        )
        assert [result.structuredContent for result in results] == [
            {"value": str(i)} for i in range(3)
        ]
        assert (await runtime.call(config, "echo", {"value": "error"})).isError
    finally:
        await runtime.close()
    pid = int((tmp_path / "pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    await runtime.close()
    with pytest.raises(MCPError, match="closed"):
        await runtime.discover("test", config)


@pytest.mark.parametrize("mode", ["hang", "oversize", "cursor"])
async def test_failed_discovery_is_bounded(mode: str) -> None:
    runtime = MCPRuntime()
    config = server(mode).model_copy(
        update={
            "limits": models.Limits(
                startup_seconds=0.2, request_seconds=0.2, shutdown_seconds=1
            )
        }
    )
    try:
        with pytest.raises(MCPError):
            await asyncio.wait_for(runtime.discover("test", config), 3)
    finally:
        await runtime.close()


async def test_startup_failure_can_retry() -> None:
    runtime = MCPRuntime()
    config = models.StdioServer(
        command="/does/not/exist", limits=models.Limits(shutdown_seconds=1)
    )
    try:
        for _ in range(2):
            with pytest.raises(MCPError):
                await runtime.discover("test", config)
            assert runtime._connections == {}
        assert (await runtime.discover("test", server())).tools
    finally:
        await runtime.close()


async def test_cancellation_propagates_and_late_result_does_not_break_session() -> None:
    runtime = MCPRuntime()
    config = server()
    try:
        await runtime.discover("test", config)
        task = asyncio.create_task(runtime.call(config, "echo", {"value": "wait"}))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await runtime.call(config, "echo", {"value": "after"})
        assert result.structuredContent == {"value": "after"}
    finally:
        await runtime.close()


async def test_roots_do_not_share_sessions() -> None:
    first, second = MCPRuntime(), MCPRuntime()
    try:
        await asyncio.gather(
            first.discover("test", server()), second.discover("test", server())
        )
        await first.close()
        assert (
            await second.call(server(), "echo", {"value": "live"})
        ).structuredContent == {"value": "live"}
    finally:
        await asyncio.gather(first.close(), second.close())


async def test_startup_cancellation_rolls_back_and_reaps_process(
    tmp_path: Path,
) -> None:
    runtime = MCPRuntime()
    pid_path = tmp_path / "pid"
    task = asyncio.create_task(runtime.discover("test", server("hang", str(pid_path))))
    async with asyncio.timeout(3):
        while not pid_path.exists():
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime._connections == {}
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_path.read_text()), 0)
    await runtime.close()


async def test_request_deadline_does_not_retry_mutation() -> None:
    runtime = MCPRuntime()
    config = server().model_copy(update={"limits": models.Limits(request_seconds=0.1)})
    try:
        await runtime.discover("test", config)
        with pytest.raises(MCPError, match="outcome may be unknown"):
            await runtime.call(config, "echo", {"value": "wait"})
    finally:
        await runtime.close()
