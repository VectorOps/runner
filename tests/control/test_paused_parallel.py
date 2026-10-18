import asyncio
from pathlib import Path

from tests.control.test_mailbox import pending, response
from tests.control.test_paused import status
from tests.fixtures import paused_workflows as flows
from vectorops.runner.control import client, protocol as p


async def test_answer_does_not_interrupt_parallel_tool_execution(
    tmp_path: Path,
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
                reference="python:tests.fixtures.paused_workflows:parallel_tools",
                idempotency_key="start",
            )
        )
        request = (await pending(connection, accepted.execution_id))[0]
        await asyncio.wait_for(flows.started.wait(), 5)
        assert (await connection.execution(accepted.execution_id)).status == "running"
        await connection.answer_input(
            response(request).model_copy(update={"response": {"decision": "approve"}})
        )
        assert (await connection.execution(accepted.execution_id)).status == "running"
        assert not flows.finished.is_set()
        flows.release.set()
        await status(connection, accepted.execution_id, "completed")
