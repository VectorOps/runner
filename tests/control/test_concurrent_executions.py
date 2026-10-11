import asyncio
from pathlib import Path
import secrets
import sys
import json

import pytest

from tests.control.test_mailbox import pending, response, WORKFLOW
from tests.control.test_paused import status
from tests.fixtures import sources
from vectorops.runner.control import client, protocol as p, transport
from vectorops.runner.control.service import RunnerService
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.runner import state
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner import mailbox


async def settled(connection: client.RunnerClient, identifier: str) -> None:
    async with asyncio.timeout(10):
        while await connection.current(identifier) is not None:
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("tcp", [False, True])
async def test_concurrent_lifecycle_and_idempotency(tmp_path: Path, tcp: bool) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    token = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        port = await server.listen() if tcp else None

        async def connect() -> client.RunnerClient:
            if port is not None:
                return await client.RunnerClient.tcp(port, token)
            host, worker = transport.memory_pair()
            server.attach(worker)
            return await client.RunnerClient.connect(host, token)

        try:
            async with await connect() as first, await connect() as second:
                starts = [
                    p.Start(reference="flow:workflow", idempotency_key=str(i))
                    for i in range(4)
                ]
                accepted = await asyncio.gather(
                    *(first.start(params) for params in starts)
                )
                identifiers = {item.execution_id for item in accepted}
                assert len(identifiers) == 4
                requests = [
                    (await pending(second, item.execution_id))[0] for item in accepted
                ]
                assert (
                    set((await service.runner.project_state()).active_execution_ids)
                    == identifiers
                )
                page = await second.active(p.ExecutionList(limit=2))
                assert len(page.executions) == 2 and page.next_after is not None
                tail = await second.active(
                    p.ExecutionList(after=page.next_after, limit=2)
                )
                assert {
                    item.execution_id for item in page.executions + tail.executions
                } == identifiers
                assert tail.next_after is None
                assert set((await second.executions()).execution_ids) == identifiers
                with pytest.raises(p.RemoteError, match="multiple executions"):
                    await second.current()
                assert await second.start(starts[0]) == accepted[0]
                with pytest.raises(p.RemoteError) as error:
                    await second.resume(
                        p.Resume(
                            execution_id=accepted[0].execution_id,
                            idempotency_key="duplicate",
                        )
                    )
                assert error.value.code == "execution_conflict"
                await first.stop(accepted[0].execution_id)
                await settled(second, accepted[0].execution_id)
                assert (
                    await second.execution(accepted[0].execution_id)
                ).status == "cancelled"
                assert len((await second.active()).executions) == 3
                assert set(
                    (await service.runner.project_state()).active_execution_ids
                ) == identifiers - {accepted[0].execution_id}
                for request in requests[1:]:
                    await second.answer_input(response(request))
                for item in accepted[1:]:
                    await settled(second, item.execution_id)
                    assert (
                        await second.execution(item.execution_id)
                    ).status == "completed"
                params = p.Resume(
                    execution_id=accepted[1].execution_id, idempotency_key="resume"
                )
                resumed = await asyncio.gather(
                    first.resume(params), second.resume(params)
                )
                assert resumed[0] == resumed[1]
                await settled(second, accepted[1].execution_id)
                assert (
                    await second.execution(accepted[1].execution_id)
                ).status == "completed"
                assert (await second.active()).executions == []
                assert (await service.runner.project_state()).active_execution_ids == []
        finally:
            await server.aclose()


async def test_shutdown_recovers_all_waiters_but_not_stopped_or_completed(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = [
            await connection.start(
                p.Start(reference="flow:workflow", idempotency_key=str(i))
            )
            for i in range(4)
        ]
        requests = [
            (await pending(connection, item.execution_id))[0] for item in accepted
        ]
        await connection.stop(accepted[2].execution_id)
        await settled(connection, accepted[2].execution_id)
        await connection.answer_input(response(requests[3]))
        await settled(connection, accepted[3].execution_id)
        for item in accepted[:2]:
            await status(connection, item.execution_id, "paused")
    pointer = await RunnerStateStore(tmp_path / "data").load()
    assert pointer.active_execution_ids == []
    assert pointer.last_execution_id == accepted[3].execution_id
    async with client.embedded(tmp_path) as connection:
        assert {
            item.execution_id for item in (await connection.active()).executions
        } == set(pointer.active_execution_ids)
        for item, request in zip(accepted[:2], requests[:2]):
            assert (await pending(connection, item.execution_id))[0] == request
            await connection.answer_input(response(request))
        for item in accepted[:2]:
            await settled(connection, item.execution_id)
            assert (await connection.execution(item.execution_id)).status == "completed"
        assert (
            await connection.execution(accepted[2].execution_id)
        ).status == "cancelled"
    assert (await RunnerStateStore(tmp_path / "data").load()).active_execution_ids == []


async def test_shutdown_waiter_does_not_suspend_unrelated_work(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(
        WORKFLOW + "\n" + sources.read("mailbox_busy.py")
    )
    async with client.embedded(tmp_path) as connection:
        waiter = await connection.start(
            p.Start(reference="flow:workflow", idempotency_key="wait")
        )
        request = (await pending(connection, waiter.execution_id))[0]
        busy = await connection.start(
            p.Start(reference="flow:busy", idempotency_key="busy")
        )
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
        assert await connection.current(busy.execution_id) is None
        assert (await pending(connection, waiter.execution_id))[0] == request
        await connection.answer_input(response(request))
        await settled(connection, waiter.execution_id)


async def test_drain_waits_for_all_and_rejects_new_work(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with RunnerService(tmp_path) as service:
        accepted = [
            p.Accepted.model_validate(
                await service.dispatch(
                    "executions.start",
                    p.Start(
                        reference="flow:workflow", idempotency_key=str(i)
                    ).model_dump(mode="json"),
                )
            )
            for i in range(2)
        ]
        async with asyncio.timeout(10):
            while len((await service.runner.project_state()).active_execution_ids) != 2:
                await asyncio.sleep(0.01)
        await service.dispatch("runner.shutdown", {"mode": "drain"})
        with pytest.raises(p.RemoteError) as error:
            await service.dispatch(
                "executions.start",
                p.Start(reference="flow:workflow", idempotency_key="new").model_dump(
                    mode="json"
                ),
            )
        assert error.value.code == "runner_stopping"
        closing = asyncio.create_task(service.aclose())
        await asyncio.sleep(0)
        assert not closing.done()
        for item in accepted:
            await service.dispatch(
                "executions.stop", {"execution_id": item.execution_id}
            )
        await asyncio.wait_for(closing, 10)


async def test_same_execution_cannot_resume_twice(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with RunnerService(tmp_path) as service:
        accepted = p.Accepted.model_validate(
            await service.dispatch(
                "executions.start",
                p.Start(reference="flow:workflow", idempotency_key="one").model_dump(
                    mode="json"
                ),
            )
        )
        async with asyncio.timeout(10):
            while (
                accepted.execution_id
                not in (await service.runner.project_state()).active_execution_ids
            ):
                await asyncio.sleep(0.01)
        with pytest.raises(ValueError, match="already active"):
            await service.runner.resume(accepted.execution_id)


async def test_crash_recovers_multiple_executions_without_manifest(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.fixtures.scripts.concurrent_crash",
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        async with asyncio.timeout(15):
            line = await process.stdout.readline()
        assert line
        requests = [
            mailbox.InputRequest.model_validate(value) for value in json.loads(line)
        ]
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
    async with RunnerStateStore(tmp_path / "data") as store:
        await store.save(state.RunnerState())
    async with client.embedded(tmp_path) as connection:
        assert {
            item.execution_id for item in (await connection.active()).executions
        } <= {request.execution_id for request in requests}
        for request in requests:
            assert (await pending(connection, request.execution_id))[0] == request
            await connection.answer_input(response(request))
        for request in requests:
            await settled(connection, request.execution_id)
            assert (
                await connection.execution(request.execution_id)
            ).status == "completed"


async def test_immediate_stop_and_failure_do_not_affect_other_execution(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(
        WORKFLOW + "\n" + sources.read("mailbox_fail.py")
    )
    async with client.embedded(tmp_path) as connection:
        survivor = await connection.start(
            p.Start(reference="flow:workflow", idempotency_key="survivor")
        )
        request = (await pending(connection, survivor.execution_id))[0]
        failed = await connection.start(
            p.Start(reference="flow:fail", idempotency_key="failure")
        )
        await settled(connection, failed.execution_id)
        assert (await connection.execution(failed.execution_id)).status == "failed"
        stopped = await connection.start(
            p.Start(reference="flow:workflow", idempotency_key="stop")
        )
        await connection.stop(stopped.execution_id)
        await settled(connection, stopped.execution_id)
        assert (
            stopped.execution_id
            not in (
                await RunnerStateStore(tmp_path / "data").load()
            ).active_execution_ids
        )
        assert await connection.current(survivor.execution_id) is not None
        await connection.answer_input(response(request))
        await settled(connection, survivor.execution_id)


async def test_state_updates_are_atomic(tmp_path: Path) -> None:
    async with RunnerStateStore(tmp_path) as store:
        assert (await store.load()).active_execution_ids == []
        await asyncio.gather(*(store.activate(str(i)) for i in range(20)))
        assert set((await store.load()).active_execution_ids) == {
            str(i) for i in range(20)
        }
        await asyncio.gather(*(store.deactivate(str(i)) for i in range(10)))
        assert set((await store.load()).active_execution_ids) == {
            str(i) for i in range(10, 20)
        }
    for values in (["../escape"], ["duplicate", "duplicate"], [""]):
        with pytest.raises(ValueError):
            state.RunnerState(active_execution_ids=values)
