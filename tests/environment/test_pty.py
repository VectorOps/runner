import asyncio
from collections.abc import AsyncIterator
import errno
import json
import os
from pathlib import Path
import signal
import sys

import pydantic
import pytest

from tests.environment import test_processes as helpers
from tests.fixtures import sources
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as m
from vectorops.runner.environment.processes.manager import ProcessManager

pytestmark = pytest.mark.skipif(os.name != "posix", reason="PTYs require POSIX")


@pytest.fixture
async def environment(tmp_path: Path) -> AsyncIterator[ProcessManager]:
    target = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        yield target.processes
    finally:
        await target.processes.close()


def command(mode: str) -> m.ArgvCommand:
    return m.ArgvCommand(
        argv=[sys.executable, "-u", str(sources.path("terminal.py")), mode]
    )


async def until_output(
    environment: ProcessManager, result: m.ProcessResult, marker: bytes
) -> m.ProcessResult:
    data = helpers.output(result)
    async with asyncio.timeout(5):
        while marker not in data:
            assert result.retained, data
            result = await environment.poll(
                m.PollRequest(
                    process_id=result.process_id,
                    cursor=result.cursor,
                    wait_timeout_ms=100,
                )
            )
            data += helpers.output(result)
    return result


@pytest.mark.parametrize("custom_size", [False, True])
async def test_controlling_terminal_and_size(
    environment: ProcessManager,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    custom_size: bool,
) -> None:
    monkeypatch.delenv("TERM", raising=False)
    result = await environment.exec(
        m.ExecRequest(
            command=command("info"),
            mode="pty",
            keep_stdin_open=True,
            terminal_size=m.TerminalSize(rows=40, cols=120) if custom_size else None,
            yield_timeout_ms=5000,
        )
    )
    assert result.state == "exited" and result.returncode == 0
    assert result.output_complete and not result.retained
    assert {chunk.stream for chunk in result.chunks} == {"terminal"}
    assert json.loads(helpers.output(result)) == {
        "isatty": [True, True, True],
        "rows": 40 if custom_size else 50,
        "cols": 120 if custom_size else 160,
        "foreground": True,
        "session_leader": True,
        "term": "dumb",
        "cwd": str(tmp_path),
    }
    assert m.ProcessResult.model_validate_json(result.model_dump_json()) == result


def test_terminal_size_defaults_and_partial_overrides() -> None:
    assert m.TerminalSize().model_dump() == {"rows": 50, "cols": 160}
    assert m.TerminalSize(rows=40).model_dump() == {"rows": 40, "cols": 160}
    assert m.TerminalSize(cols=120).model_dump() == {"rows": 50, "cols": 120}
    properties = m.TerminalSize.model_json_schema()["properties"]
    assert properties["rows"]["default"] == 50
    assert properties["cols"]["default"] == 160


@pytest.mark.parametrize("field", ["rows", "cols"])
@pytest.mark.parametrize("value", [0, -1, 65536, True, 1.5, "24"])
def test_invalid_terminal_size(field: str, value: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.TerminalSize.model_validate({field: value})


def test_terminal_size_requires_pty_and_round_trips() -> None:
    with pytest.raises(pydantic.ValidationError, match="requires PTY"):
        m.ExecRequest(command=command("info"), terminal_size=m.TerminalSize())
    request = m.ExecRequest(
        command=command("info"),
        mode="pty",
        keep_stdin_open=True,
        terminal_size=m.TerminalSize(rows=65535, cols=1),
    )
    assert m.ExecRequest.model_validate_json(request.model_dump_json()) == request


async def test_pty_requires_open_stdin(environment: ProcessManager) -> None:
    with pytest.raises(EnvironmentError, match="keep_stdin_open=True") as error:
        await environment.exec(m.ExecRequest(command=command("info"), mode="pty"))
    assert error.value.code == ErrorCode.UNSUPPORTED
    assert await environment.list_processes() == []


@pytest.mark.parametrize("mode", ["raw", "bulk"])
async def test_raw_key_sequences(environment: ProcessManager, mode: str) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command(mode),
            mode="pty",
            keep_stdin_open=True,
            yield_timeout_ms=0,
        )
    )
    result = await until_output(environment, result, b"ready")
    data = b"\x1b[A\x00\xff" if mode == "raw" else bytes(range(256)) * 256
    await environment.write_stdin(
        m.StdinRequest(process_id=result.process_id, data=data)
    )
    final, output = await helpers.finish(environment, result)
    assert final.returncode == 0
    assert output.endswith(data)


async def test_pty_eof_rejected_without_delivering_input(
    environment: ProcessManager,
) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=helpers.command("uppercase"),
            mode="pty",
            keep_stdin_open=True,
            stdin=b"hello\n",
            yield_timeout_ms=0,
        )
    )
    with pytest.raises(EnvironmentError) as error:
        await environment.write_stdin(
            m.StdinRequest(process_id=result.process_id, data=b"bad", eof=True)
        )
    assert error.value.code == ErrorCode.UNSUPPORTED
    result = await until_output(environment, result, b"hello")
    await environment.write_stdin(
        m.StdinRequest(process_id=result.process_id, data=b"\x04")
    )
    final, output = await helpers.finish(environment, result)
    assert final.returncode == 0
    assert b"HELLO" in output and b"bad" not in output


async def test_ctrl_c_signals_foreground_process(environment: ProcessManager) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("interrupt"),
            mode="pty",
            keep_stdin_open=True,
            yield_timeout_ms=0,
        )
    )
    result = await until_output(environment, result, b"ready")
    await environment.write_stdin(
        m.StdinRequest(process_id=result.process_id, data=b"\x03")
    )
    final, _ = await helpers.finish(environment, result)
    assert final.returncode == -signal.SIGINT


async def test_curses_app(environment: ProcessManager) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("curses"),
            mode="pty",
            keep_stdin_open=True,
            env={"TERM": "xterm-256color"},
            yield_timeout_ms=0,
        )
    )
    result = await until_output(environment, result, b"TUI ready")
    await environment.write_stdin(
        m.StdinRequest(process_id=result.process_id, data=b"q")
    )
    final, output = await helpers.finish(environment, result)
    assert final.returncode == 0
    assert b"TUI finished" in output


async def test_merged_streams_and_exit_status(environment: ProcessManager) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=helpers.command("streams"),
            mode="pty",
            keep_stdin_open=True,
            yield_timeout_ms=5000,
        )
    )
    assert result.returncode == 7
    assert {chunk.stream for chunk in result.chunks} == {"terminal"}
    assert helpers.output(result) == b"out\r\nerr\r\n"


@pytest.mark.parametrize("stop", ["kill", "close", "timeout"])
async def test_pty_lifecycle(environment: ProcessManager, stop: str) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("interrupt"),
            mode="pty",
            keep_stdin_open=True,
            execution_timeout_ms=100 if stop == "timeout" else None,
            yield_timeout_ms=0,
        )
    )
    if stop == "close":
        reports = await environment.close()
        assert reports[0].termination_confirmed
        return
    if stop == "kill":
        result = await until_output(environment, result, b"ready")
        report = await environment.kill(m.SessionRequest(process_id=result.process_id))
        assert report.termination_confirmed and report.scope == "group"
    final, _ = await helpers.finish(environment, result)
    assert final.returncode is not None and final.returncode < 0
    assert final.timed_out is (stop == "timeout")
    assert final.error is None


async def test_spawn_failure_and_recovery(environment: ProcessManager) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=m.ArgvCommand(argv=["/does/not/exist"]),
            mode="pty",
            keep_stdin_open=True,
            yield_timeout_ms=5000,
        )
    )
    assert result.state == "failed" and result.error
    assert not result.retained
    result = await environment.exec(
        m.ExecRequest(
            command=command("info"),
            mode="pty",
            keep_stdin_open=True,
            yield_timeout_ms=5000,
        )
    )
    assert result.returncode == 0


async def test_continuous_output_does_not_block_deadline(
    environment: ProcessManager,
) -> None:
    result = await environment.exec(
        m.ExecRequest(
            command=command("flood"),
            mode="pty",
            keep_stdin_open=True,
            execution_timeout_ms=100,
            yield_timeout_ms=1000,
        )
    )
    final, output = await helpers.finish(environment, result)
    assert output
    assert final.timed_out and final.returncode is not None and final.returncode < 0
    assert final.error is None


@pytest.mark.parametrize("failure", ["none", "exec", "setup", "spawn"])
async def test_pty_descriptors_are_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from vectorops.runner.environment.processes import posix

    descriptors: list[int] = []
    openpty = posix.pty.openpty

    def tracked_openpty() -> tuple[int, int]:
        pair = openpty()
        descriptors.extend(pair)
        return pair

    def fail_size(fd: int, size: tuple[int, int]) -> None:
        raise OSError(errno.EIO, "terminal setup failed")

    async def fail_spawn(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EIO, "spawn failed")

    monkeypatch.setattr(posix.pty, "openpty", tracked_openpty)
    if failure == "setup":
        monkeypatch.setattr(posix.termios, "tcsetwinsize", fail_size)
    if failure == "spawn":
        pipe = os.pipe

        def tracked_pipe() -> tuple[int, int]:
            pair = pipe()
            descriptors.extend(pair)
            return pair

        monkeypatch.setattr(os, "pipe", tracked_pipe)
        monkeypatch.setattr(asyncio.get_running_loop(), "subprocess_exec", fail_spawn)
    driver = posix.PosixProcessDriver(tmp_path, m.EnvPolicy())
    request = m.ExecRequest(
        command=(
            m.ArgvCommand(argv=["/does/not/exist"])
            if failure == "exec"
            else helpers.command("sleep", "60")
        ),
        mode="pty",
        keep_stdin_open=True,
    )
    if failure != "none":
        with pytest.raises(OSError):
            await driver.spawn(request)
    else:
        handle = await driver.spawn(request)
        try:
            await handle.terminate(force=True)
            await handle.wait()
        finally:
            await handle.close()
    assert len(descriptors) == (4 if failure == "spawn" else 2)
    for fd in descriptors:
        with pytest.raises(OSError) as error:
            os.fstat(fd)
        assert error.value.errno == errno.EBADF
