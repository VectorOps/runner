import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.control.test_mailbox import WORKFLOW, pending, response
from tests.fixtures import paused_workflows as flows
from vectorops.runner.control import client, protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.workflow import WorkflowRunner


async def status(
    connection: client.RunnerClient, identifier: str, expected: str
) -> None:
    async with asyncio.timeout(10):
        while (await connection.execution(identifier)).status != expected:
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("name,count", [("workflow", 1), ("parallel", 3), ("tool", 1)])
async def test_paused_startup_is_cold_and_answers_activate_once(
    tmp_path: Path, name: str, count: int
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference=f"flow:{name}", idempotency_key="start")
        )
        requests = await pending(connection, accepted.execution_id, count)
        await status(connection, accepted.execution_id, "paused")
        assert await connection.current(accepted.execution_id) is not None
        await asyncio.sleep(1.1)
        assert (await connection.execution(accepted.execution_id)).status == "paused"

    async with RunnerService(tmp_path) as service:
        assert not service._tasks
        assert not service.runner._mailboxes
        assert (
            await service.runner.execution_state(accepted.execution_id)
        ).status == "paused"
        assert (
            await service.runner.pending_inputs(
                mailbox.RequestQuery(execution_id=accepted.execution_id)
            )
        ).requests == requests
        answers = [response(request) for request in requests]
        if name == "tool":
            answers[0] = answers[0].model_copy(
                update={"response": {"decision": "approve"}}
            )
        with patch.object(service, "_schedule", wraps=service._schedule) as schedule:
            await asyncio.gather(
                *(
                    service.dispatch("mailbox.respond", answer.model_dump(mode="json"))
                    for answer in [*answers, answers[0]]
                )
            )
            async with asyncio.timeout(10):
                while service._tasks:
                    await asyncio.sleep(0.01)
            assert schedule.call_count == 1
            await service.dispatch(
                "mailbox.respond", answers[0].model_dump(mode="json")
            )
            assert schedule.call_count == 1
        assert (
            await service.runner.execution_state(accepted.execution_id)
        ).status == "completed"
    async with RunnerService(tmp_path) as service:
        assert not service._tasks


@pytest.mark.parametrize("name", ["mixed", "parallel_tools"])
async def test_parallel_work_keeps_running_until_only_inputs_remain(
    tmp_path: Path, name: str
) -> None:
    flows.started = asyncio.Event()
    flows.release = asyncio.Event()
    flows.finished = asyncio.Event()
    async with client.embedded(tmp_path) as connection:
        await connection.request(
            "tools.rules.create", {"rule": {"tool_name": "work", "decision": "approve"}}
        )
        accepted = await connection.start(
            p.Start(
                reference=f"python:tests.fixtures.paused_workflows:{name}",
                idempotency_key="start",
            )
        )
        request = (await pending(connection, accepted.execution_id))[0]
        await asyncio.wait_for(flows.started.wait(), 5)
        assert (await connection.execution(accepted.execution_id)).status == "running"
        flows.release.set()
        await asyncio.wait_for(flows.finished.wait(), 5)
        await status(connection, accepted.execution_id, "paused")
        answer = response(request)
        if name == "parallel_tools":
            answer = answer.model_copy(update={"response": {"decision": "approve"}})
        await connection.answer_input(answer)
        await status(connection, accepted.execution_id, "completed")


async def test_parallel_child_inputs_repause_after_one_answer(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(
                reference="python:tests.fixtures.paused_workflows:children",
                idempotency_key="start",
            )
        )
        requests = await pending(connection, accepted.execution_id, 2)
        await status(connection, accepted.execution_id, "paused")
    async with client.embedded(tmp_path) as connection:
        assert await connection.current() is None
        await connection.answer_input(response(requests[0]))
        await status(connection, accepted.execution_id, "paused")
        assert await connection.current() is not None
        await connection.answer_input(response(requests[1]))
        await status(connection, accepted.execution_id, "completed")


async def test_cold_answer_survives_restart_before_scheduling(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="flow:parallel", idempotency_key="start")
        )
        requests = await pending(connection, accepted.execution_id, 3)
        await status(connection, accepted.execution_id, "paused")
    runner = WorkflowRunner(tmp_path)
    await runner.answer_input(response(requests[0]))
    assert (await runner.execution_state(accepted.execution_id)).status == "running"
    async with client.embedded(tmp_path) as connection:
        await status(connection, accepted.execution_id, "paused")
        assert await connection.current() is not None
        for request in requests[1:]:
            await connection.answer_input(response(request))
        await status(connection, accepted.execution_id, "completed")


async def test_stop_cold_execution_does_not_load_it(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="flow:workflow", idempotency_key="start")
        )
        request = (await pending(connection, accepted.execution_id))[0]
        await status(connection, accepted.execution_id, "paused")
    async with RunnerService(tmp_path) as service:
        await service.dispatch(
            "executions.stop", {"execution_id": accepted.execution_id}
        )
        assert not service._tasks
        assert (
            await service.runner.execution_state(accepted.execution_id)
        ).status == "cancelled"
        with pytest.raises(p.RemoteError, match="closed"):
            await service.dispatch(
                "mailbox.respond", response(request).model_dump(mode="json")
            )
