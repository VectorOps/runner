import asyncio
import json
from pathlib import Path

import pytest

from tests.control.test_concurrent_executions import settled
from tests.control.test_mailbox import pending, response, WORKFLOW
from tests.fixtures import sources
from vectorops.runner.control import client, daemon, protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.retention import ExecutionRetention
from vectorops.runner.runner.project import RetentionPolicy
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner import models


async def test_recovery_ignores_unpublished_incomplete_history(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "data/executions/incomplete/history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="flow:workflow")
    )
    async with client.embedded(tmp_path) as connection:
        assert (await connection.active()).executions == []
    assert database.path.exists()


async def test_stop_before_task_starts_persists_cancellation_and_can_resume(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with RunnerService(tmp_path) as service:
        accepted = p.Accepted.model_validate(
            await service.dispatch(
                "executions.start",
                {"reference": "flow:workflow", "idempotency_key": "first"},
            )
        )
        await service.dispatch(
            "executions.stop", {"execution_id": accepted.execution_id}
        )
        async with asyncio.timeout(10):
            while (
                await service.dispatch(
                    "executions.current", {"execution_id": accepted.execution_id}
                )
                is not None
            ):
                await asyncio.sleep(0.01)
        assert (
            await service.runner.execution_state(accepted.execution_id)
        ).status == "cancelled"
        assert (await service.runner.project_state()).active_execution_ids == []
        await service.dispatch(
            "executions.resume",
            {"execution_id": accepted.execution_id, "idempotency_key": "resume"},
        )
        async with asyncio.timeout(10):
            while not (
                await service.runner.pending_inputs(
                    mailbox.RequestQuery(execution_id=accepted.execution_id)
                )
            ).requests:
                await asyncio.sleep(0.01)
        assert (await service.runner.execution_state(accepted.execution_id)).status in (
            "running",
            "paused",
        )


async def test_authentication_is_independent_of_execution_and_shutdown(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(
        sources.read("authorize.py") + "\n" + sources.read("mailbox_busy.py")
    )
    async with client.embedded(tmp_path) as connection:
        login = await connection.login("chatgpt")
        waiting = await connection.start(
            p.Start(reference="flow:main", idempotency_key="auth")
        )
        busy = await connection.start(
            p.Start(reference="flow:busy", idempotency_key="busy")
        )
        await settled(connection, waiting.execution_id)
        assert (await connection.execution(waiting.execution_id)).status == "failed"
        assert (await connection.pending_authentication())[0].id == login.id
        assert (await connection.pending_authentication())[0].execution_ids == []
        async with asyncio.timeout(10):
            while (
                busy.execution_id
                not in (
                    await RunnerStateStore(tmp_path / "data").load()
                ).active_execution_ids
            ):
                await asyncio.sleep(0.01)
    async with client.embedded(tmp_path) as connection:
        assert (await connection.execution(busy.execution_id)).status == "cancelled"
        assert (await connection.execution(waiting.execution_id)).status == "failed"
        assert await connection.pending_authentication() == []


async def test_daemon_drain_allows_reconnect_and_finishes_all_executions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    task = asyncio.create_task(daemon.serve(tmp_path))
    try:
        async with asyncio.timeout(10):
            while not (output := capsys.readouterr().out):
                await asyncio.sleep(0.01)
        endpoint = daemon.Endpoint.model_validate_json(output.strip())
        async with await client.RunnerClient.tcp(endpoint.port) as connection:
            accepted = [
                await connection.start(
                    p.Start(reference="flow:workflow", idempotency_key=str(i))
                )
                for i in range(2)
            ]
            requests = [
                (await pending(connection, item.execution_id))[0] for item in accepted
            ]
            await daemon.dispatch(tmp_path, "status", port=endpoint.port)
            status = json.loads(capsys.readouterr().out)
            assert status["current"] is None
            assert {
                item["execution_id"] for item in status["active"]["executions"]
            } == {item.execution_id for item in accepted}
            await connection.shutdown("drain")
        assert not task.done()
        async with await client.RunnerClient.tcp(endpoint.port) as connection:
            await connection.answer_input(response(requests[0]))
            await settled(connection, accepted[0].execution_id)
            assert not task.done()
            assert await connection.current(accepted[1].execution_id) is not None
            await connection.answer_input(response(requests[1]))
        await asyncio.wait_for(task, 10)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_live_retention_preserves_waiters_and_resume_reservations(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(
        WORKFLOW + "\n" + sources.read("mailbox_quick.py")
    )
    (tmp_path / "config.toml").write_text("[retention]\nmax_completed_executions = 0\n")
    async with client.embedded(tmp_path) as connection:
        waiter = await connection.start(
            p.Start(reference="flow:workflow", idempotency_key="wait")
        )
        request = (await pending(connection, waiter.execution_id))[0]
        for index in range(3):
            quick = await connection.start(
                p.Start(reference="flow:quick", idempotency_key=str(index))
            )
            await settled(connection, quick.execution_id)
            assert (await connection.execution(waiter.execution_id)).status == "paused"
        await connection.answer_input(response(request))
        await settled(connection, waiter.execution_id)
        assert (await connection.execution(waiter.execution_id)).status == "completed"
    async with RunnerStateStore(tmp_path / "data") as store:
        async with store.reserve(waiter.execution_id):
            async with store.coordination:
                removed = await ExecutionRetention(
                    tmp_path / "data", RetentionPolicy(max_completed_executions=0)
                ).apply(None, protected_ids=store.reserved)
            assert waiter.execution_id not in removed
        removed = await ExecutionRetention(
            tmp_path / "data", RetentionPolicy(max_completed_executions=0)
        ).apply(None)
        assert waiter.execution_id in removed


async def test_execution_subscription_does_not_receive_other_root_events(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        roots = [
            await connection.start(
                p.Start(reference="flow:workflow", idempotency_key=str(i))
            )
            for i in range(2)
        ]
        requests = [(await pending(connection, root.execution_id))[0] for root in roots]
        await connection.subscribe(
            p.Subscribe(
                execution_id=roots[0].execution_id, kinds=["execution.completed"]
            )
        )
        await connection.answer_input(response(requests[1]))
        await settled(connection, roots[1].execution_id)
        await connection.answer_input(response(requests[0]))
        async with asyncio.timeout(10):
            event = await connection.next_event()
        assert event.event["execution_id"] == roots[0].execution_id
        assert event.event["kind"] == "execution.completed"
        await settled(connection, roots[0].execution_id)


async def test_repeated_close_and_cancellation_finish_owned_cleanup(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    service = await RunnerService(tmp_path).__aenter__()
    for index in range(3):
        await service.dispatch(
            "executions.start",
            {"reference": "flow:workflow", "idempotency_key": str(index)},
        )
    first = asyncio.create_task(service.aclose())
    second = asyncio.create_task(service.aclose())
    await asyncio.sleep(0)
    first.cancel()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert results[1] is None
    async with RunnerStateStore(tmp_path / "data"):
        pass
