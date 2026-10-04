import asyncio
import base64
from datetime import timedelta
import json
from pathlib import Path
import threading

import pydantic
import pytest

from vectorops.runner import diagnostics as d
from vectorops.runner.control import client, protocol as p
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.workflow import WorkflowRunner


async def records(logs: d.DiagnosticLog) -> list[d.LogRecord]:
    listing = await logs.listing()
    result = []
    for file in listing.files:
        chunk = await logs.download(d.LogDownload(file=file.name, end=file.size))
        result.extend(
            d.LogRecord.model_validate_json(line)
            for line in base64.b64decode(chunk.content_base64).splitlines()
        )
    return result


async def test_structured_redaction_snapshot_and_severity(tmp_path: Path) -> None:
    async with d.DiagnosticLog(tmp_path) as logs:
        nested: dict[str, pydantic.JsonValue] = {
            "Authorization": "Bearer secret-auth",
            "nested": [{"api_key": "secret-key", "count": 4}],
            "message": "Bearer secret-bearer password=secret-password",
        }
        with (
            logs.activate(),
            d.bind(execution_id="exec", workflow_id="root", step_id="step"),
        ):
            d.log("ignored", level=d.Level.DEBUG)
            d.log("request.finished", fields=nested)
            nested["message"] = "changed"
            try:
                raise ValueError("secret-exception")
            except ValueError as exc:
                d.log("request.failed", level=d.Level.ERROR, error=exc)
        entries = await records(logs)
        assert len(entries) == 2
        first, failure = entries
        assert first.version == 1
        assert first.timestamp.utcoffset() == timedelta(0)
        assert first.level == "INFO"
        assert (first.execution_id, first.workflow_id, first.step_id) == (
            "exec",
            "root",
            "step",
        )
        assert first.fields["nested"] == [{"api_key": "[REDACTED]", "count": 4}]
        assert first.fields["message"] == "Bearer [REDACTED] password=[REDACTED]"
        assert failure.fields["error_type"] == "ValueError"
        assert failure.fields["frames"]
        assert "secret-" not in "".join(entry.model_dump_json() for entry in entries)


async def test_task_local_context_isolation(tmp_path: Path) -> None:
    async with d.DiagnosticLog(tmp_path) as logs:

        async def child(identifier: str) -> None:
            with d.bind(workflow_id=identifier, step_id=identifier):
                await asyncio.sleep(0)
                d.log("child")
            d.log("parent")

        with logs.activate(), d.bind(execution_id="exec", workflow_id="root"):
            await asyncio.gather(child("one"), child("two"))
        with logs.activate():
            d.log("outside")
        entries = await records(logs)
        assert {
            (entry.workflow_id, entry.step_id)
            for entry in entries
            if entry.event == "child"
        } == {("one", "one"), ("two", "two")}
        assert all(entry.execution_id == "exec" for entry in entries[:-1])
        assert all(
            entry.workflow_id == "root" and entry.step_id is None
            for entry in entries
            if entry.event == "parent"
        )
        assert entries[-1].execution_id is None


async def test_queue_bounds_disk_offload_and_cancelled_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logs = d.DiagnosticLog(tmp_path, d.LogConfig(queue_size=1))
    entered = threading.Event()
    release = threading.Event()
    append = logs._append

    def blocked(content: bytes) -> None:
        entered.set()
        assert release.wait(5)
        append(content)

    monkeypatch.setattr(logs, "_append", blocked)
    await logs.__aenter__()
    try:
        logs.emit("first")
        assert await asyncio.to_thread(entered.wait, 5)
        logs.emit("second")
        logs.emit("overflow")
        assert logs.dropped == 1
        closing = asyncio.create_task(logs.aclose())
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert len(await records(logs)) == 2
    finally:
        release.set()
        await logs.aclose()


async def test_write_failure_is_observational(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with d.DiagnosticLog(tmp_path) as logs:

        def fail(content: bytes) -> None:
            raise OSError("disk full")

        with monkeypatch.context() as patch:
            patch.setattr(logs, "_append", fail)
            logs.emit("lost")
            await logs.flush()
        logs.emit("recovered")
        assert [entry.event for entry in await records(logs)] == ["recovered"]
        listing = await logs.listing()
        assert listing.dropped == listing.write_errors == 1


async def test_rotation_retention_restart_and_oversized_record(tmp_path: Path) -> None:
    config = d.LogConfig(max_bytes=1024, max_files=2)
    async with d.DiagnosticLog(tmp_path, config) as logs:
        logs.emit("oversized", fields={"value": "x" * 2000})
        for index in range(8):
            logs.emit("rotate", fields={"index": index, "padding": "x" * 400})
        listing = await logs.listing()
        assert len(listing.files) == 2
        assert all(file.size <= config.max_bytes for file in listing.files)
        assert logs.dropped == 1
        assert [entry.fields["index"] for entry in await records(logs)] == [6, 7]
        previous = listing.files[-1]
    unrelated = tmp_path / "logs" / "keep.txt"
    unrelated.write_text("keep")
    async with d.DiagnosticLog(tmp_path, config) as reopened:
        assert (
            await reopened.download(d.LogDownload(file=previous.name))
        ).end == previous.size
        reopened.emit("restart", fields={"padding": "x" * 400})
        assert len((await reopened.listing()).files) == 2
        assert unrelated.read_text() == "keep"


async def test_chunk_boundaries_pin_end_and_reject_unsafe_paths(tmp_path: Path) -> None:
    async with d.DiagnosticLog(tmp_path) as logs:
        logs.emit("unicode", fields={"text": "caf\u00e9"})
        file = (await logs.listing()).files[0]
        raw = bytearray()
        offset = 0
        while offset < file.size:
            chunk = await logs.download(
                d.LogDownload(file=file.name, offset=offset, limit=7, end=file.size)
            )
            raw.extend(base64.b64decode(chunk.content_base64))
            offset = chunk.next_offset
        assert json.loads(raw)["fields"]["text"] == "caf\u00e9"
        logs.emit("appended")
        await logs.flush()
        assert (
            await logs.download(
                d.LogDownload(file=file.name, offset=file.size, end=file.size)
            )
        ).eof
        with pytest.raises(ValueError, match="range"):
            await logs.download(d.LogDownload(file=file.name, offset=file.size + 10000))
        with pytest.raises(FileNotFoundError):
            await logs.download(d.LogDownload(file="f" * 32 + ".jsonl"))
        link = tmp_path / "logs" / ("e" * 32 + ".jsonl")
        link.symlink_to(tmp_path / "secret")
        with pytest.raises(OSError):
            await logs.download(d.LogDownload(file=link.name))
        assert link.name not in [item.name for item in (await logs.listing()).files]


@pytest.mark.parametrize(
    "params",
    [
        {"file": "../secret"},
        {"file": "/secret"},
        {"file": "a" * 32 + ".jsonl", "limit": 262145},
        {"file": "a" * 32 + ".jsonl", "offset": -1},
    ],
)
def test_invalid_download(params: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        d.LogDownload.model_validate(params)


@pytest.mark.parametrize(
    "params",
    [{"level": "TRACE"}, {"queue_size": 0}, {"max_files": 0}, {"max_bytes": 0}],
)
def test_invalid_config(params: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        d.LogConfig.model_validate(params)


@pytest.mark.parametrize("tcp", [False, True])
async def test_host_download_and_restart(tmp_path: Path, tcp: bool) -> None:
    import secrets
    from vectorops.runner.control import transport

    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/job.py").write_text(
        "async def main(ctx):\n    return 'private-result'\n"
    )
    async with RunnerService(tmp_path) as service:
        token = secrets.token_urlsafe(32)
        server = RunnerServer(service, token)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(await server.listen(), token)
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, token)
            async with connection:
                await connection.subscribe()
                accepted = await connection.start(
                    p.Start(reference="job:main", idempotency_key="logging")
                )
                async with asyncio.timeout(5):
                    while (await connection.next_event()).event[
                        "kind"
                    ] != "execution.completed":
                        pass
                listing = await connection.logs()
                assert listing.files
                file = listing.files[0]
                chunk = await connection.download_logs(
                    d.LogDownload(file=file.name, end=file.size)
                )
                payload = base64.b64decode(chunk.content_base64)
                entries = [
                    d.LogRecord.model_validate_json(line)
                    for line in payload.splitlines()
                ]
                assert any(
                    entry.event == "workflow.completed"
                    and entry.execution_id == accepted.execution_id
                    and entry.workflow_id
                    for entry in entries
                )
                assert b"private-result" not in payload
                assert token.encode() not in payload
                with pytest.raises(p.RemoteError) as error:
                    await connection.request("logs.download", {"file": "../secret"})
                assert error.value.code == "invalid_argument"
        finally:
            await server.aclose()
    async with client.embedded(tmp_path) as connection:
        assert file.name in [entry.name for entry in (await connection.logs()).files]


async def test_local_runner_logs_failure_and_step_identity(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text('[logging]\nlevel = "DEBUG"\n')
    source = tmp_path / "workflows"
    source.mkdir()
    (source / "job.py").write_text(
        "from vectorops.runner import diagnostics\n"
        "async def main(ctx):\n"
        "    diagnostics.log('custom.event', fields={'password': 'private-input'})\n"
        "    raise ValueError('private-failure')\n"
    )
    runner = WorkflowRunner(tmp_path)
    with pytest.raises(Exception, match="private-failure"):
        await runner.run("job:main")
    async with d.DiagnosticLog(tmp_path / "data") as logs:
        entries = await records(logs)
        assert any(
            entry.event == "step.failed"
            and entry.execution_id == runner.last_execution_id
            and entry.workflow_id
            and entry.step_id
            for entry in entries
        )
        assert any(entry.event == "custom.event" for entry in entries)
        assert "private-" not in "".join(entry.model_dump_json() for entry in entries)
