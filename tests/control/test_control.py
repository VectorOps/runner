import asyncio
from pathlib import Path
import secrets
import struct

import pytest

from tests.fixtures import sources
from vectorops.runner import models
from vectorops.runner.control import client, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import history_query as h
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.state_store import RunnerStateStore


async def terminal(connection: client.RunnerClient) -> p.Event:
    async with asyncio.timeout(5):
        while True:
            event = await connection.next_event()
            if event.event["kind"] in (
                "execution.completed",
                "execution.failed",
                "execution.cancelled",
            ):
                return event


@pytest.mark.parametrize("tcp", [False, True])
async def test_execution_history_events_and_reconnect(
    tmp_path: Path, tcp: bool
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/job.py").write_text(sources.read("value.py"))
    secret = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, secret)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(
                    await server.listen(), secret
                )
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, secret)
            async with connection:
                await connection.subscribe()
                params = p.Start(
                    reference="job:main",
                    input={"value": 11},
                    idempotency_key="first",
                )
                accepted = await connection.start(params)
                assert (await terminal(connection)).event[
                    "kind"
                ] == "execution.completed"
                assert await connection.current() is None
                assert (
                    await connection.execution(accepted.execution_id)
                ).status == "completed"
                page = await connection.history(
                    h.HistoryQuery(execution_id=accepted.execution_id, limit=1)
                )
                assert len(page.items) == 1
                assert page.items[0].step.outcome == models.StepCompleted(result=11)
                assert await connection.start(params) == accepted
                with pytest.raises(p.RemoteError, match="different arguments"):
                    await connection.start(
                        params.model_copy(update={"input": {"value": 12}})
                    )
                with pytest.raises(p.RemoteError) as error:
                    await connection.request("unsupported.method")
                assert error.value.code == "unsupported_method"
                assert await connection.request("executions.list") == {
                    "execution_ids": [accepted.execution_id],
                    "next_after": None,
                }
                await asyncio.gather(
                    *(connection.request("runner.info") for _ in range(10))
                )
        finally:
            await server.aclose()
    async with client.embedded(tmp_path) as connection:
        assert await connection.start(params) == accepted
        assert (await connection.execution(accepted.execution_id)).status == "completed"


async def test_concurrent_stop_disconnect_and_workspace_ownership(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/wait.py").write_text(sources.read("wait.py"))
    secret = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, secret)
        port = await server.listen()
        try:
            with pytest.raises(RuntimeError, match="another runner"):
                async with RunnerStateStore(tmp_path / "data"):
                    pass
            async with await client.RunnerClient.tcp(port, secret) as first:
                accepted = await first.start(
                    p.Start(reference="wait:main", idempotency_key="wait")
                )
            async with await client.RunnerClient.tcp(port, secret) as second:
                await second.subscribe()
                current = await second.current()
                assert (
                    current is not None
                    and current.execution_id == accepted.execution_id
                )
                other = await second.start(
                    p.Start(reference="wait:main", idempotency_key="other")
                )
                assert other.execution_id != accepted.execution_id
                with pytest.raises(p.RemoteError) as error:
                    await second.stop("wrong")
                assert error.value.code == "execution_conflict"
                assert (await second.stop(accepted.execution_id)).status == "stopping"
                assert (await terminal(second)).event["kind"] == "execution.cancelled"
                assert await second.current(other.execution_id) is not None
        finally:
            await server.aclose()


async def test_authentication_required_and_wrong_secret(tmp_path: Path) -> None:
    secret = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, secret)
        port = await server.listen()
        try:
            with pytest.raises((p.RemoteError, asyncio.IncompleteReadError)):
                await client.RunnerClient.tcp(port, secrets.token_urlsafe(32))
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connection = transport.Connection(transport.TCPStream(reader, writer))
            await connection.send(
                p.Request(id="unauthenticated", method="runner.shutdown")
            )
            async with asyncio.timeout(2):
                with pytest.raises(asyncio.IncompleteReadError):
                    await connection.receive()
            assert not service.shutdown_requested.is_set()
            await connection.aclose()
        finally:
            await server.aclose()


async def test_framing_bounds_and_fragmentation() -> None:
    host, worker = transport.memory_pair()
    connection = transport.Connection(host)
    raw = p.Ping(id="x").model_dump_json().encode()
    data = struct.pack("!I", len(raw)) + raw
    await worker.write(data[:2])
    await worker.write(data[2:7])
    await worker.write(data[7:])
    assert await connection.receive() == p.Ping(id="x")
    await worker.write(struct.pack("!I", p.MAX_FRAME + 1))
    with pytest.raises(ValueError, match="frame size"):
        await connection.receive()
    await host.aclose()


async def test_tail_pagination_is_bounded_and_query_bound(tmp_path: Path) -> None:
    from vectorops.runner.runner.execution_state import ExecutionRecord

    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(models.Workflow(id="root", definition_ref="job:main"))
    await database.save_execution(ExecutionRecord(id="exec", root_workflow_id="root"))
    for index in range(120):
        await database.append_step(
            models.WorkflowStep(
                id=f"s{index}",
                workflow_id="root",
                previous_step_id=f"s{index-1}" if index else None,
                operation=models.StepOperation(kind="test", name="test"),
                outcome=models.StepCompleted(),
            )
        )
    query = h.HistoryQuery(execution_id="exec", limit=10)
    page = await database.query(query)
    assert [item.step.id for item in page.items] == [f"s{i}" for i in range(110, 120)]
    assert page.next_cursor is not None
    await database.append_step(
        models.WorkflowStep(
            id="new",
            workflow_id="root",
            previous_step_id="s119",
            operation=models.StepOperation(kind="test", name="test"),
            outcome=models.StepCompleted(),
        )
    )
    older = await database.query(query.model_copy(update={"cursor": page.next_cursor}))
    assert [item.step.id for item in older.items] == [f"s{i}" for i in range(100, 110)]
    with pytest.raises(ValueError, match="another query"):
        await database.query(
            query.model_copy(
                update={"cursor": page.next_cursor, "workflow_id": "other"}
            )
        )
    selected = await database.query(
        h.HistoryQuery(execution_id="exec", step_ids=["s1", "s70"])
    )
    assert [item.step.id for item in selected.items] == ["s1", "s70"]
    branch = await database.query(
        h.HistoryQuery(execution_id="exec", head_step_id="s3", limit=2)
    )
    assert [item.step.id for item in branch.items] == ["s2", "s3"]


async def test_start_rejects_workspace_escape(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as connection:
        for reference in (
            "../outside:main",
            "/outside:main",
            "workspace:job:main",
            "job.py:main",
        ):
            with pytest.raises(p.RemoteError) as error:
                await connection.request(
                    "executions.start",
                    {"reference": reference, "idempotency_key": reference},
                )
            assert error.value.code == "invalid_argument"
