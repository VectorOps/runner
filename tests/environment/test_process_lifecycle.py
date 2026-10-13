import asyncio
import os
from pathlib import Path

import pydantic
import pytest

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as m
from vectorops.runner.environment.processes.local import LocalProcessDriver
from vectorops.runner.environment.processes.manager import ProcessManager
from tests.environment import test_processes as helpers

FakeDriver = helpers.FakeDriver
command = helpers.command
finish = helpers.finish
output = helpers.output

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="local processes require POSIX"
)


async def test_shell_and_environment_filtering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROCESS_SECRET", "hidden")
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            processes=m.ProcessConfig(
                allow_shell=True,
                environment=m.EnvPolicy(
                    allowlist=["PATH"], defaults={"VALUE": "default"}
                ),
            ),
        )
    )
    try:
        result = await environment.exec(
            m.ExecRequest(
                command=m.ShellCommand(
                    script='printf "%s:%s" "$VALUE" "${PROCESS_SECRET-unset}"'
                ),
                env={"VALUE": "overlay"},
                yield_timeout_ms=5000,
            )
        )
        assert output(result) == b"overlay:unset"
        assert result.returncode == 0
    finally:
        await environment.close()


async def test_initial_stdin_and_binary_output(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        result = await environment.exec(
            m.ExecRequest(
                command=command("binary_stdin"),
                stdin=b"\x00\xff",
                yield_timeout_ms=5000,
            )
        )
        assert output(result) == b"\x00\xff"
        assert m.ProcessResult.model_validate_json(result.model_dump_json()) == result
    finally:
        await environment.close()


async def test_kill_escalation_cleans_ordinary_child_group(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            processes=m.ProcessConfig(termination_grace_seconds=0.05),
        )
    )
    try:
        result = await environment.exec(
            m.ExecRequest(command=command("child_group"), yield_timeout_ms=100)
        )
        async with asyncio.timeout(5):
            while b"ready" not in output(result):
                result = await environment.poll(
                    m.PollRequest(
                        process_id=result.process_id,
                        cursor=result.cursor,
                        wait_timeout_ms=100,
                    )
                )
        killed = await environment.kill(m.SessionRequest(process_id=result.process_id))
        assert killed.termination_confirmed
        final, _ = await finish(environment, result)
        assert final.returncode == -9
    finally:
        await environment.close()


async def test_slow_spawn_is_bounded() -> None:
    driver = FakeDriver()
    manager = ProcessManager(driver, m.ProcessConfig(io_timeout_seconds=0.02))
    result = await manager.exec(
        m.ExecRequest(command=m.ArgvCommand(argv=["fake"]), yield_timeout_ms=1000)
    )
    assert result.state == "failed" and not result.retained
    await manager.close()


async def test_cancelled_close_continues_cleanup() -> None:
    driver = FakeDriver()
    driver.attach.set()
    manager = ProcessManager(driver, m.ProcessConfig(termination_grace_seconds=0.02))
    await manager.exec(
        m.ExecRequest(command=m.ArgvCommand(argv=["fake"]), yield_timeout_ms=0)
    )
    close = asyncio.create_task(manager.close())
    await asyncio.sleep(0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    reports = await manager.close()
    assert reports[0].termination_confirmed and driver.handle.closed


@pytest.mark.parametrize("phase", ["spawn", "terminate"])
async def test_spawn_cleanup_survives_repeated_cancellation(
    tmp_path: Path, phase: str
) -> None:
    started = asyncio.Event()
    terminating = asyncio.Event()
    spawned = asyncio.Event()
    release = asyncio.Event()

    class Handle(helpers.FakeHandle):
        async def terminate(self, *, force: bool) -> None:
            terminating.set()
            await release.wait()
            await super().terminate(force=force)

    handle = Handle()

    class Driver(LocalProcessDriver):
        async def _spawn(self, request: m.ExecRequest) -> Handle:
            started.set()
            await spawned.wait()
            return handle

    driver = Driver(lambda cwd: tmp_path, m.EnvPolicy())
    task = asyncio.create_task(
        driver.spawn(m.ExecRequest(command=m.ArgvCommand(argv=["fake"])))
    )
    try:
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        if phase == "terminate":
            spawned.set()
            await asyncio.wait_for(terminating.wait(), 5)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
        spawned.set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert handle.exited.is_set()
        assert handle.closed
    finally:
        spawned.set()
        release.set()
        await asyncio.gather(task, return_exceptions=True)


def test_exit_status_validation() -> None:
    with pytest.raises(pydantic.ValidationError):
        m.ExitResult()
    with pytest.raises(pydantic.ValidationError):
        m.ExitResult(outcome="unknown", returncode=0)


async def test_pending_stdin_is_released_before_handle_cleanup() -> None:
    from vectorops.runner.environment.errors import EnvironmentError

    class BlockingHandle(helpers.FakeHandle):
        def __init__(self) -> None:
            super().__init__()
            self.writing = asyncio.Event()
            self.write_finished = False

        async def write_stdin(self, data: bytes) -> None:
            self.writing.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.write_finished = True

        async def close(self) -> None:
            assert self.write_finished
            await super().close()

    driver = FakeDriver()
    handle = BlockingHandle()
    driver.handle = handle
    driver.attach.set()
    manager = ProcessManager(driver, m.ProcessConfig())
    result = await manager.exec(
        m.ExecRequest(
            command=m.ArgvCommand(argv=["fake"]),
            keep_stdin_open=True,
            yield_timeout_ms=0,
        )
    )
    sending = asyncio.create_task(
        manager.write_stdin(
            m.StdinRequest(process_id=result.process_id, data=b"pending")
        )
    )
    await handle.writing.wait()
    handle.exited.set()
    with pytest.raises(EnvironmentError) as error:
        await sending
    assert error.value.may_have_occurred
    await manager.close()
    assert handle.closed
