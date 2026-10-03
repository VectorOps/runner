import asyncio
import json
from pathlib import Path
import secrets
import sys

import pytest

from vectorops.runner import cli
from vectorops.runner import global_config
from vectorops.runner.control import client, daemon, protocol as p
from vectorops.runner.control.provisioning import read_token


@pytest.mark.parametrize("source", ["stdin", "file"])
async def test_daemon_subprocess_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    token = secrets.token_urlsafe(32)
    token_file = tmp_path / "token"
    token_file.write_text(token + "\n", encoding="ascii")
    token_file.chmod(0o600)
    options = (
        ["--token-stdin"] if source == "stdin" else ["--token-file", str(token_file)]
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "vectorops.runner",
        "--project",
        str(workspace),
        "daemon",
        "serve",
        *options,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdin is not None and process.stdout is not None
        if source == "stdin":
            process.stdin.write((token + "\n").encode())
            await process.stdin.drain()
        process.stdin.close()
        async with asyncio.timeout(10):
            line = await process.stdout.readline()
        assert token.encode() not in line
        endpoint = daemon.Endpoint.model_validate_json(line)
        assert endpoint.host == "127.0.0.1"
        assert endpoint.workspace == str(workspace.resolve())
        assert not (home / ".vectorops-runner").exists()
        assert not list(workspace.rglob("credential"))
        token_file.unlink()
        async with await client.RunnerClient.tcp(endpoint.port, token) as connection:
            assert connection.instance_id == endpoint.instance_id
            assert await connection.current() is None
        status = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "vectorops.runner",
            "daemon",
            "status",
            "--port",
            str(endpoint.port),
            "--token-stdin",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        async with asyncio.timeout(10):
            output, errors = await status.communicate((token + "\n").encode())
        assert status.returncode == 0, errors
        assert json.loads(output)["current"] is None
        assert token.encode() not in output + errors
        stop = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "vectorops.runner",
            "daemon",
            "stop",
            "--port",
            str(endpoint.port),
            "--token-stdin",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        async with asyncio.timeout(10):
            output, errors = await stop.communicate((token + "\n").encode())
            assert await process.wait() == 0
        assert stop.returncode == 0, errors
        assert json.loads(output) == {"status": "stopping"}
        assert process.stderr is not None
        assert token.encode() not in await process.stderr.read()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"short",
        b"x" * 4097,
        b"x" * 43 + b"\nextra",
        b"x" * 43 + b" ",
        b"\xff" * 43,
    ],
)
async def test_invalid_token_file_is_not_disclosed(
    tmp_path: Path, value: bytes
) -> None:
    path = tmp_path / "token"
    path.write_bytes(value)
    with pytest.raises(ValueError) as error:
        await read_token(token_file=path)
    if value:
        assert repr(value) not in str(error.value)


@pytest.mark.parametrize("ending", ["", "\n", "\r\n"])
async def test_token_file_read_once(tmp_path: Path, ending: str) -> None:
    token = secrets.token_urlsafe(32)
    path = tmp_path / "token"
    path.write_bytes((token + ending).encode())
    assert await read_token(token_file=path) == token
    assert path.read_bytes() == (token + ending).encode()


@pytest.mark.parametrize(
    "args",
    [
        ["daemon", "serve", "--token-stdin", "--token-file", "token"],
        ["daemon", "status", "--token-stdin"],
    ],
)
def test_cli_rejects_conflicting_tokens_and_missing_port(args: list[str]) -> None:
    with pytest.raises(SystemExit):
        cli.parser().parse_args(args)


@pytest.mark.parametrize(
    "args", [["daemon", "serve"], ["daemon", "stop", "--port", "1234"]]
)
def test_cli_allows_persistent_control_token(args: list[str]) -> None:
    parsed = cli.parser().parse_args(args)
    assert parsed.token_file is None
    assert not parsed.token_stdin


async def test_invalid_stdin_fails_without_echo(tmp_path: Path) -> None:
    token = "private-but-invalid"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "vectorops.runner",
        "--project",
        str(tmp_path),
        "daemon",
        "serve",
        "--token-stdin",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    async with asyncio.timeout(10):
        output, errors = await process.communicate((token + "\n").encode())
    assert process.returncode == 1
    assert output == b""
    assert token.encode() not in errors
    assert not (tmp_path / "data").exists()


def test_token_repr_is_redacted() -> None:
    token = secrets.token_urlsafe(32)
    packet = p.Authenticate(token=token)
    assert token not in repr(packet)
    assert json.loads(packet.model_dump_json())["token"] == token


async def test_daemon_restart_uses_persistent_token_without_provisioning(
    tmp_path: Path,
) -> None:
    configuration = global_config.GlobalConfiguration()
    previous = None
    for crash in (True, False):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "vectorops.runner",
            "--project",
            str(tmp_path),
            "daemon",
            "serve",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            assert process.stdout is not None
            async with asyncio.timeout(10):
                endpoint = daemon.Endpoint.model_validate_json(
                    await process.stdout.readline()
                )
            current = await configuration.load()
            if previous is not None:
                assert current == previous
            previous = current
            async with await client.RunnerClient.tcp(endpoint.port) as connection:
                assert await connection.current() is None
                if not crash:
                    await connection.shutdown()
            if crash:
                process.kill()
            async with asyncio.timeout(10):
                await process.wait()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
