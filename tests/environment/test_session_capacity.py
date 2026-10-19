import asyncio
from pathlib import Path
import sys

import pytest

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as m
from vectorops.runner.environment.processes.manager import SessionCapacity


def command(script: str, *, wait: int = 1000) -> m.ExecRequest:
    return m.ExecRequest(
        command=m.ArgvCommand(argv=[sys.executable, "-c", script]),
        yield_timeout_ms=wait,
    )


def test_capacity_release_is_idempotent() -> None:
    capacity = SessionCapacity(1)
    capacity.acquire("first")
    with pytest.raises(EnvironmentError) as error:
        capacity.acquire("second")
    assert error.value.code == ErrorCode.RESOURCE_LIMIT
    capacity.release("first")
    capacity.acquire("second")
    capacity.release("first")
    with pytest.raises(EnvironmentError):
        capacity.acquire("third")


async def test_concurrent_managers_share_admission(tmp_path: Path) -> None:
    config = LocalEnvironmentConfig(
        base_project_path=str(tmp_path), processes=m.ProcessConfig(max_sessions=1)
    )
    capacity = SessionCapacity(1)
    first = await LocalEnvironment.open(config, capacity=capacity)
    second = await LocalEnvironment.open(config, capacity=capacity)
    try:
        results = await asyncio.gather(
            first.processes.exec(command("import time; time.sleep(60)", wait=0)),
            second.processes.exec(command("import time; time.sleep(60)", wait=0)),
            return_exceptions=True,
        )
        assert sum(isinstance(result, m.ProcessResult) for result in results) == 1
        failures = [
            result for result in results if isinstance(result, EnvironmentError)
        ]
        assert len(failures) == 1 and failures[0].code == ErrorCode.RESOURCE_LIMIT
    finally:
        await first.processes.close()
        await second.processes.close()
    capacity.acquire("after-close")


@pytest.mark.parametrize("expire", [False, True])
async def test_retained_sessions_hold_shared_capacity_until_release(
    tmp_path: Path, expire: bool
) -> None:
    config = LocalEnvironmentConfig(
        base_project_path=str(tmp_path),
        processes=m.ProcessConfig(
            max_sessions=1, max_response_bytes=1, retention_seconds=1
        ),
    )
    capacity = SessionCapacity(1)
    first = await LocalEnvironment.open(config, capacity=capacity)
    second = await LocalEnvironment.open(config, capacity=capacity)
    try:
        result = await first.processes.exec(command("print('retained output')"))
        assert result.state == "exited" and result.retained
        assert len(await first.processes.list_processes()) == 1
        with pytest.raises(EnvironmentError) as error:
            await second.processes.exec(command("pass"))
        assert error.value.code == ErrorCode.RESOURCE_LIMIT
        async with asyncio.timeout(5):
            if expire:
                while await first.processes.list_processes():
                    await asyncio.sleep(0.01)
            else:
                while result.retained:
                    result = await first.processes.poll(
                        m.PollRequest(
                            process_id=result.process_id, cursor=result.cursor
                        )
                    )
        assert (await second.processes.exec(command("pass"))).returncode == 0
    finally:
        await first.processes.close()
        await second.processes.close()


async def test_failed_spawn_and_cancelled_caller_do_not_leak_capacity(
    tmp_path: Path,
) -> None:
    config = LocalEnvironmentConfig(
        base_project_path=str(tmp_path), processes=m.ProcessConfig(max_sessions=1)
    )
    capacity = SessionCapacity(1)
    first = await LocalEnvironment.open(config, capacity=capacity)
    second = await LocalEnvironment.open(config, capacity=capacity)
    try:
        failed = await first.processes.exec(
            m.ExecRequest(
                command=m.ArgvCommand(argv=["/does/not/exist"]), yield_timeout_ms=1000
            )
        )
        assert failed.state == "failed" and not failed.retained
        assert (await second.processes.exec(command("pass"))).returncode == 0
        caller = asyncio.create_task(
            first.processes.exec(command("import time; time.sleep(60)", wait=5000))
        )
        try:
            async with asyncio.timeout(5):
                while not (sessions := await first.processes.list_processes()):
                    await asyncio.sleep(0)
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller
            with pytest.raises(EnvironmentError) as error:
                await second.processes.exec(command("pass"))
            assert error.value.code == ErrorCode.RESOURCE_LIMIT
            stopped = await first.processes.kill(
                m.SessionRequest(process_id=sessions[0].process_id)
            )
            assert stopped.termination_confirmed
            final = await first.processes.poll(
                m.PollRequest(process_id=sessions[0].process_id)
            )
            assert not final.retained
            assert (await second.processes.exec(command("pass"))).returncode == 0
        finally:
            caller.cancel()
            await asyncio.gather(caller, return_exceptions=True)
    finally:
        await first.processes.close()
        await second.processes.close()
