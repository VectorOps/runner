import asyncio
from collections.abc import AsyncIterator
import os
from pathlib import Path
import sys

import pydantic
import pytest

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as m
from vectorops.runner.environment.processes.environment import build_environment
from vectorops.runner.environment.processes.manager import ProcessManager
from vectorops.runner.environment.processes.output import OutputBuffer


def command(script: str) -> m.ArgvCommand:
    return m.ArgvCommand(argv=[sys.executable, "-u", "-c", script])


def output(result: m.ProcessResult) -> bytes:
    return b"".join(chunk.data for chunk in result.chunks)


@pytest.fixture
async def environment(tmp_path: Path) -> AsyncIterator[LocalEnvironment]:
    if os.name != "posix":
        pytest.skip("local process driver requires POSIX")
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        yield environment
    finally:
        await environment.close()


async def finish(
    environment: LocalEnvironment, result: m.ProcessResult
) -> tuple[m.ProcessResult, bytes]:
    data = output(result)
    async with asyncio.timeout(10):
        while result.retained:
            result = await environment.poll(
                m.PollRequest(
                    process_id=result.process_id,
                    cursor=result.cursor,
                    wait_timeout_ms=100,
                )
            )
            data += output(result)
    return result, data


async def test_quick_command_is_forgotten(environment: LocalEnvironment) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command(
                "import sys; print('out'); print('err', file=sys.stderr); sys.exit(7)"
            ),
            yield_timeout_ms=5000,
        )
    )
    assert result.state == "exited" and result.returncode == 7
    assert not result.retained and result.output_complete
    assert {chunk.stream for chunk in result.chunks} == {"stdout", "stderr"}
    assert await environment.list_processes() == []
    with pytest.raises(EnvironmentError) as error:
        await environment.poll(m.PollRequest(process_id=result.process_id))
    assert error.value.code == ErrorCode.MISSING


async def test_independent_processes_and_stdin(environment: LocalEnvironment) -> None:
    first = await environment.exec(
        m.ExecRequest(
            command=command("import sys; print(sys.stdin.read().upper())"),
            keep_stdin_open=True,
            yield_timeout_ms=0,
        )
    )
    second = await environment.exec(
        m.ExecRequest(command=command("print('independent')"), yield_timeout_ms=5000)
    )
    assert first.process_id != second.process_id
    assert output(second) == b"independent\n"
    assert first.retained
    await environment.write_stdin(
        m.StdinRequest(process_id=first.process_id, data=b"hello", eof=True)
    )
    final, data = await finish(environment, first)
    assert final.returncode == 0 and data == b"HELLO\n"


async def test_cancelled_exec_and_poll_leave_session_alive(
    environment: LocalEnvironment,
) -> None:
    task = asyncio.create_task(
        environment.exec(
            m.ExecRequest(
                command=command("import time; time.sleep(60)"), yield_timeout_ms=30000
            )
        )
    )
    async with asyncio.timeout(5):
        while not await environment.list_processes():
            await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    session = (await environment.list_processes())[0]
    poll = asyncio.create_task(
        environment.poll(
            m.PollRequest(process_id=session.process_id, wait_timeout_ms=30000)
        )
    )
    poll.cancel()
    with pytest.raises(asyncio.CancelledError):
        await poll
    assert (await environment.list_processes())[0].process_id == session.process_id
    killed = await environment.kill(m.SessionRequest(process_id=session.process_id))
    assert killed.termination_confirmed
    final, _ = await finish(environment, session)
    assert final.returncode is not None and final.returncode < 0


async def test_execution_timeout(environment: LocalEnvironment) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("import time; time.sleep(60)"),
            execution_timeout_ms=50,
            yield_timeout_ms=5000,
        )
    )
    result, _ = await finish(environment, result)
    assert result.timed_out
    assert result.returncode is not None and result.returncode < 0


async def test_cwd_and_environment(
    environment: LocalEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROCESS_TEST_INHERITED", "parent")
    (tmp_path / "sub").mkdir()
    result = await environment.exec(
        m.ExecRequest(
            command=command(
                "import os; print(os.getcwd()); print(os.environ['PROCESS_TEST_INHERITED']); print(os.environ['PROCESS_TEST_OVERLAY'])"
            ),
            cwd="sub",
            env={"PROCESS_TEST_OVERLAY": "overlay"},
            yield_timeout_ms=5000,
        )
    )
    assert output(result).splitlines() == [
        str(tmp_path / "sub").encode(),
        b"parent",
        b"overlay",
    ]
    invalid = await environment.exec(
        m.ExecRequest(
            command=command("print('bad')"), cwd="../outside", yield_timeout_ms=5000
        )
    )
    assert invalid.state == "failed" and invalid.error


async def test_unsupported_pty_shell_and_spawn_failure(
    environment: LocalEnvironment,
) -> None:
    with pytest.raises(EnvironmentError) as pty:
        await environment.exec(m.ExecRequest(command=command("pass"), mode="pty"))
    assert pty.value.code == ErrorCode.UNSUPPORTED
    with pytest.raises(EnvironmentError) as shell:
        await environment.exec(
            m.ExecRequest(command=m.ShellCommand(script="echo test"))
        )
    assert shell.value.code == ErrorCode.DENIED
    result = await environment.exec(
        m.ExecRequest(command=m.ArgvCommand(argv=["/does/not/exist"]))
    )
    assert result.state == "failed" and result.error
    assert not result.retained


async def test_large_output_is_bounded_and_drained(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            processes=m.ProcessConfig(max_output_bytes=128, max_response_bytes=32),
        )
    )
    try:
        result = await environment.exec(
            m.ExecRequest(
                command=command("import sys; sys.stdout.write('x' * 1000000)"),
                yield_timeout_ms=5000,
            )
        )
        assert result.state == "exited" and result.retained
        assert result.dropped_bytes == 1000000 - 128
        assert len(output(result)) == 32
        final, data = await finish(environment, result)
        assert len(data) == 128 and final.cursor == 1000000
    finally:
        await environment.close()


async def test_close_stops_sessions_and_rejects_exec(
    environment: LocalEnvironment,
) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("import time; time.sleep(60)"), yield_timeout_ms=0
        )
    )
    reports = await environment.close()
    assert len(reports) == 1 and reports[0].process_id == result.process_id
    assert reports[0].termination_confirmed
    assert await environment.close() == reports
    with pytest.raises(EnvironmentError):
        await environment.exec(m.ExecRequest(command=command("pass")))


def test_environment_policy_matches_codeng_precedence() -> None:
    parent = {"PATH": "/parent", "KEEP": "parent", "SECRET": "secret", "OMIT": "omit"}
    policy = m.EnvPolicy(
        allowlist=["PATH", "KEEP", "SECRET"],
        denylist=["SECRET"],
        defaults={"KEEP": "default", "NEW": "default"},
    )
    assert build_environment(policy, parent, {"KEEP": "overlay"}) == {
        "PATH": "/parent",
        "KEEP": "overlay",
        "NEW": "default",
    }
    assert build_environment(
        m.EnvPolicy(inherit_parent=False, defaults={"A": "a"}), parent, {"B": "b"}
    ) == {"A": "a", "B": "b"}
    assert (
        build_environment(
            m.EnvPolicy(denylist=["SECRET"]), parent, {"SECRET": "explicit"}
        )["SECRET"]
        == "explicit"
    )
    assert parent["SECRET"] == "secret"


def test_output_buffer_partial_chunks_and_gaps() -> None:
    buffer = OutputBuffer(5)
    buffer.append(m.OutputChunk(stream="stdout", data=b"abcd"))
    buffer.append(m.OutputChunk(stream="stderr", data=b"efgh"))
    chunks, cursor, dropped = buffer.read(0, 3)
    assert [(chunk.stream, chunk.data) for chunk in chunks] == [
        ("stdout", b"d"),
        ("stderr", b"ef"),
    ]
    assert cursor == 6 and dropped == 3
    assert buffer.read(6, 10)[0][0].data == b"gh"
    with pytest.raises(ValueError):
        buffer.read(9, 1)


@pytest.mark.parametrize("data", [{"argv": []}, {"argv": [""]}, {"argv": ["a\0b"]}])
def test_invalid_argv(data: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.ArgvCommand.model_validate(data)


class FakeHandle:
    def __init__(self) -> None:
        self.exited = asyncio.Event()
        self.output: asyncio.Queue[m.OutputChunk] = asyncio.Queue()
        self.written: list[bytes] = []
        self.closed = False
        self.signals: list[bool] = []
        self.result = m.ExitResult(returncode=0)

    async def read_output(self) -> AsyncIterator[m.OutputChunk]:
        await self.exited.wait()
        while not self.output.empty():
            yield self.output.get_nowait()

    async def write_stdin(self, data: bytes) -> None:
        self.written.append(data)

    async def close_stdin(self) -> None:
        pass

    async def wait(self) -> m.ExitResult:
        await self.exited.wait()
        return self.result

    async def terminate(self, *, force: bool) -> None:
        self.signals.append(force)
        if force:
            self.exited.set()

    async def close(self) -> None:
        self.closed = True


class FakeDriver:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.attach = asyncio.Event()
        self.handle = FakeHandle()

    @property
    def capabilities(self) -> m.DriverCapabilities:
        return m.DriverCapabilities(termination_scope="process")

    async def spawn(self, request: m.ExecRequest) -> FakeHandle:
        self.started.set()
        await self.attach.wait()
        return self.handle


async def test_shared_manager_retains_during_spawn_and_escalates() -> None:
    driver = FakeDriver()
    manager = ProcessManager(driver, m.ProcessConfig(termination_grace_seconds=0.01))
    request = asyncio.create_task(
        manager.exec(
            m.ExecRequest(command=m.ArgvCommand(argv=["fake"]), yield_timeout_ms=30000)
        )
    )
    await driver.started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    session = (await manager.list_processes())[0]
    assert session.state == "starting"
    driver.attach.set()
    killed = await manager.kill(m.SessionRequest(process_id=session.process_id))
    assert killed.termination_confirmed and killed.scope == "process"
    assert driver.handle.signals == [False, True]
    assert driver.handle.closed
    await manager.close()


async def test_shared_manager_retention_and_session_cap() -> None:
    driver = FakeDriver()
    driver.attach.set()
    manager = ProcessManager(
        driver, m.ProcessConfig(max_sessions=1, retention_seconds=0.02)
    )
    result = await manager.exec(
        m.ExecRequest(command=m.ArgvCommand(argv=["fake"]), yield_timeout_ms=0)
    )
    with pytest.raises(EnvironmentError) as error:
        await manager.exec(m.ExecRequest(command=m.ArgvCommand(argv=["another"])))
    assert error.value.code == ErrorCode.RESOURCE_LIMIT
    driver.handle.exited.set()
    async with asyncio.timeout(2):
        while await manager.list_processes():
            await asyncio.sleep(0.01)
    with pytest.raises(EnvironmentError):
        await manager.poll(m.PollRequest(process_id=result.process_id))
    await manager.close()


async def test_shared_manager_unknown_remote_outcome() -> None:
    driver = FakeDriver()
    driver.attach.set()
    driver.handle.result = m.ExitResult(outcome="unknown")
    driver.handle.exited.set()
    manager = ProcessManager(driver, m.ProcessConfig())
    result = await manager.exec(m.ExecRequest(command=m.ArgvCommand(argv=["remote"])))
    assert result.state == "unknown" and result.returncode is None
    await manager.close()
