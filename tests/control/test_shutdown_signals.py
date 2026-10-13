import asyncio
import os
from pathlib import Path
import signal
import sys

import pytest

from tests.fixtures import sources
from vectorops.runner.control import client, daemon, protocol as p


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX signals")
@pytest.mark.parametrize("termination", [signal.SIGINT, signal.SIGTERM, signal.SIGKILL])
async def test_signal_restart_recovers_running_execution(
    tmp_path: Path, termination: int
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/wait.py").write_text(sources.read("wait.py"))

    async def launch() -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "vectorops.runner",
            "--project",
            str(tmp_path),
            "daemon",
            "serve",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

    process = await launch()
    try:
        assert process.stdout is not None
        endpoint = daemon.Endpoint.model_validate_json(
            await asyncio.wait_for(process.stdout.readline(), 10)
        )
        async with await client.RunnerClient.tcp(endpoint.port) as connection:
            accepted = await connection.start(
                p.Start(reference="wait:main", idempotency_key="start")
            )
            async with asyncio.timeout(10):
                while True:
                    active = await connection.current(accepted.execution_id)
                    if active is not None and active.status == "running":
                        break
                    await asyncio.sleep(0.01)
        process.send_signal(termination)
        await asyncio.wait_for(process.wait(), 10)
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()

    process = await launch()
    try:
        assert process.stdout is not None
        endpoint = daemon.Endpoint.model_validate_json(
            await asyncio.wait_for(process.stdout.readline(), 10)
        )
        async with await client.RunnerClient.tcp(endpoint.port) as connection:
            assert (
                await connection.execution(accepted.execution_id)
            ).status == "running"
            assert await connection.current(accepted.execution_id) is not None
            await connection.stop(accepted.execution_id)
            async with asyncio.timeout(10):
                while await connection.current(accepted.execution_id) is not None:
                    await asyncio.sleep(0.01)
            assert (
                await connection.execution(accepted.execution_id)
            ).status == "cancelled"
            await connection.shutdown()
        assert await asyncio.wait_for(process.wait(), 10) == 0
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
