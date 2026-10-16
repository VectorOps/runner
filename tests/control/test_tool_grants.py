import asyncio
from pathlib import Path

import pytest

from tests.control.test_mailbox import completed, pending
from tests.fixtures import sources
from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner import mailbox

WORKFLOW = sources.read("tool_grants.py")


def answer(
    request: mailbox.InputRequest, *, scope: str = "call", approved: bool = True
) -> mailbox.Answer:
    return mailbox.Answer(
        execution_id=request.execution_id,
        request_id=request.request_id,
        response={"approved": approved, "scope": scope},
        idempotency_key=request.request_id,
    )


async def wait_channel(
    host: client.RunnerClient, execution_id: str, channel: str
) -> mailbox.InputRequest:
    async with asyncio.timeout(10):
        while True:
            requests = await pending(host, execution_id)
            if requests[0].channel == channel:
                return requests[0]
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("scope", ["call", "workflow"])
async def test_scope_recovery_and_child_isolation(tmp_path: Path, scope: str) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as host:
        run = await host.start(
            p.Start(reference="flow:workflow", idempotency_key="start")
        )
        first = (await pending(host, run.execution_id))[0]
        receipt = await host.answer_input(answer(first, scope=scope))
        checkpoint = await wait_channel(host, run.execution_id, "checkpoint")
    async with client.embedded(tmp_path) as host:
        assert await host.answer_input(answer(first, scope=scope)) == receipt
        assert (await pending(host, run.execution_id))[0] == checkpoint
        await host.answer_input(
            mailbox.Answer(
                execution_id=run.execution_id,
                request_id=checkpoint.request_id,
                response="continue",
                idempotency_key="continue",
            )
        )
        next_request = await wait_channel(host, run.execution_id, "tool.approval")
        assert isinstance(next_request.payload, dict)
        call = next_request.payload["call"]
        assert isinstance(call, dict)
        assert call["name"] == ("other" if scope == "workflow" else "test")
        if scope == "call":
            await host.answer_input(answer(next_request))
            async with asyncio.timeout(10):
                while True:
                    other = await wait_channel(host, run.execution_id, "tool.approval")
                    if other.request_id != next_request.request_id:
                        break
                    await asyncio.sleep(0.01)
            next_request = other
        await host.answer_input(answer(next_request, approved=False))
        async with asyncio.timeout(10):
            while True:
                child = await wait_channel(host, run.execution_id, "tool.approval")
                if child.workflow_id != first.workflow_id:
                    break
                await asyncio.sleep(0.01)
        assert child.execution_id == first.execution_id
        filtered = await host.pending_inputs(
            mailbox.RequestQuery(
                execution_id=run.execution_id,
                workflow_id=first.workflow_id,
            )
        )
        assert not filtered.requests
        await host.answer_input(answer(child))
        await completed(host, run.execution_id)
        fresh = await host.start(
            p.Start(reference="flow:child", idempotency_key="fresh")
        )
        fresh_request = (await pending(host, fresh.execution_id))[0]
        assert fresh_request.execution_id != first.execution_id
        await host.answer_input(answer(fresh_request))
        await completed(host, fresh.execution_id)


async def test_workflow_grant_does_not_answer_existing_parallel_request(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as host:
        run = await host.start(
            p.Start(reference="flow:parallel", idempotency_key="start")
        )
        first, second = await pending(host, run.execution_id, 2)
        await host.answer_input(answer(first, scope="workflow"))
        assert (await pending(host, run.execution_id))[0] == second
        await host.answer_input(answer(second, approved=False))
        await completed(host, run.execution_id)


async def test_desktop_workspace_approval_configuration_requests(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as host:
        run = await host.start(p.Start(reference="flow:child", idempotency_key="start"))
        request = (await pending(host, run.execution_id))[0]
        selected = await host.request(
            "configuration.get", {"name": "tools.approval.whitelist"}
        )
        assert isinstance(selected, dict) and not selected["exists"]
        assert isinstance(request.payload, dict)
        call = request.payload["call"]
        assert isinstance(call, dict) and isinstance(call["name"], str)
        await host.request(
            "configuration.set",
            {
                "name": "tools.approval.whitelist",
                "value": [call["name"]],
                "permanent": True,
            },
        )
        await host.answer_input(answer(request))
        await completed(host, run.execution_id)
        assert (
            await host.get_configuration("components.tools.approval.whitelist")
        ).value == [call["name"]]
    async with client.embedded(tmp_path) as host:
        fresh = await host.start(
            p.Start(reference="flow:child", idempotency_key="fresh")
        )
        await completed(host, fresh.execution_id)
        assert not (
            await host.pending_inputs(
                mailbox.RequestQuery(execution_id=fresh.execution_id)
            )
        ).requests
