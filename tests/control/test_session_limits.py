import asyncio
from pathlib import Path
import secrets
import struct

import pytest

from vectorops.runner.control import client, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import history_query as h


async def test_restarted_runner_rejects_previous_token(tmp_path: Path) -> None:
    old_token = secrets.token_urlsafe(32)
    new_token = secrets.token_urlsafe(32)
    for token in (old_token, new_token):
        async with RunnerService(tmp_path) as service:
            server = RunnerServer(service, token)
            port = await server.listen()
            try:
                async with await client.RunnerClient.tcp(port, token) as connection:
                    assert await connection.current() is None
                if token == new_token:
                    with pytest.raises(p.RemoteError) as error:
                        await client.RunnerClient.tcp(port, old_token)
                    assert error.value.code == "authentication_failed"
            finally:
                await server.aclose()


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "hello", "versions": [1], "nonce": "a" * 64},
        {"kind": "authenticate", "version": 1, "token": "a" * 43},
        {"kind": "authenticate", "version": 2},
        {"kind": "authenticate", "version": 2, "token": ""},
    ],
)
async def test_invalid_handshakes_close_connection(
    tmp_path: Path, payload: dict[str, object]
) -> None:
    import json

    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, secrets.token_urlsafe(32))
        host, worker = transport.memory_pair()
        task = server.attach(worker)
        try:
            raw = json.dumps(payload).encode()
            await host.write(struct.pack("!I", len(raw)) + raw)
            async with asyncio.timeout(2):
                with pytest.raises(asyncio.IncompleteReadError):
                    await host.readexactly(4)
                await task
            assert not service.shutdown_requested.is_set()
        finally:
            await server.aclose()


async def test_bearer_authentication_on_each_connection(tmp_path: Path) -> None:
    token = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        first_host, first_worker = transport.memory_pair()
        server.attach(first_worker)
        first = transport.Connection(first_host)
        second_host, second_worker = transport.memory_pair()
        server.attach(second_worker)
        second = transport.Connection(second_host)
        try:
            authentication = p.Authenticate(token=token)
            await first.send(authentication)
            first_session = await first.receive()
            assert isinstance(first_session, p.Authenticated)
            assert first_session.instance_id == service.instance_id
            await second.send(authentication)
            second_session = await second.receive()
            assert isinstance(second_session, p.Authenticated)
            assert second_session.session_id != first_session.session_id
            assert second_session.version == p.VERSION
        finally:
            await first.aclose()
            await second.aclose()
            await server.aclose()


async def test_slow_client_requires_resynchronization() -> None:
    host, worker = transport.memory_pair()
    connection = client.RunnerClient(transport.Connection(host), "test")
    sender = transport.Connection(worker)
    try:
        for index in range(257):
            await sender.send(
                p.Event(
                    subscription_id="sub",
                    sequence=index,
                    event={"kind": "step.updated"},
                )
            )
        async with asyncio.timeout(2):
            await connection.closed.wait()
        assert isinstance(connection.failure, p.RemoteError)
        assert connection.failure.code == "resync_required"
    finally:
        await connection.aclose()
        await sender.aclose()


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e9999"])
async def test_nonfinite_json_rejected(number: str) -> None:
    host, worker = transport.memory_pair()
    connection = transport.Connection(host)
    payload = (
        '{"kind":"request","id":"1","method":"test","params":{"x":' + number + "}}"
    ).encode()
    await worker.write(struct.pack("!I", len(payload)) + payload)
    try:
        with pytest.raises(ValueError):
            await connection.receive()
    finally:
        await connection.aclose()


async def test_backfill_matches_update_while_workflow_is_running(
    tmp_path: Path,
) -> None:
    (tmp_path / "job.py").write_text(
        "import asyncio\n"
        "from vectorops.runner.api import composition as c\n"
        "@c.streamable_activity()\n"
        "async def progress(ctx):\n"
        "    yield c.delta('visible')\n"
        "    await asyncio.sleep(60)\n"
        "    yield c.final('done')\n"
        "async def main(ctx):\n"
        "    return await progress()\n"
    )
    async with client.embedded(tmp_path) as connection:
        await connection.subscribe()
        accepted = await connection.start(
            p.Start(file="job.py", callable="main", idempotency_key="start")
        )
        async with asyncio.timeout(5):
            while True:
                packet = await connection.next_event()
                if packet.event["kind"] == "step.updated":
                    break
        page = await connection.history(
            h.HistoryQuery(execution_id=accepted.execution_id, limit=1)
        )
        assert page.items[0].step.model_dump(mode="json") == packet.event["step"]
        assert page.items[0].step.revision == 2
