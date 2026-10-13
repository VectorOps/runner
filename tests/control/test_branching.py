import asyncio
from pathlib import Path

import pytest

from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner import branching, mailbox, history_query


async def pending(
    connection: client.RunnerClient, identifier: str
) -> mailbox.InputRequest:
    async with asyncio.timeout(5):
        while True:
            try:
                page = await connection.pending_inputs(
                    mailbox.RequestQuery(execution_id=identifier)
                )
                if page.requests:
                    return page.requests[0]
            except p.RemoteError:
                pass
            await asyncio.sleep(0.01)


async def test_structured_branch_validation_and_receipt(workspace: Path) -> None:
    async with client.embedded(workspace) as connection:
        accepted = await connection.start(
            p.Start(reference="branch_inputs:structured", idempotency_key="start")
        )
        original = await pending(connection, accepted.execution_id)
        request = branching.Branch(
            execution_id=accepted.execution_id,
            step_id=original.request_id,
            idempotency_key="edit",
            action=branching.InsertInput(response={"text": "edited"}),
        )
        with pytest.raises(p.RemoteError, match="contract") as invalid:
            await connection.branch(
                request.model_copy(
                    update={"action": branching.InsertInput(response={"text": 4})}
                )
            )
        assert invalid.value.code == "input_validation"
        assert invalid.value.details is not None
        assert (
            await connection.input_request(
                mailbox.RequestRef(
                    execution_id=accepted.execution_id,
                    request_id=original.request_id,
                )
            )
        ).status == "pending"
        receipt = await connection.branch(request)
        assert receipt.execution_id == accepted.execution_id
        assert await connection.branch(request) == receipt
        async with asyncio.timeout(5):
            while (
                await connection.execution(accepted.execution_id)
            ).status != "completed":
                await asyncio.sleep(0.01)
        with pytest.raises(p.RemoteError, match="idempotency"):
            await connection.branch(
                request.model_copy(
                    update={
                        "action": branching.InsertInput(response={"text": "different"})
                    }
                )
            )
    async with client.embedded(workspace) as connection:
        assert await connection.branch(request) == receipt


async def test_insert_at_second_input_preserves_prefix(workspace: Path) -> None:
    async with client.embedded(workspace) as connection:
        accepted = await connection.start(
            p.Start(
                reference="branch_inputs:two_inputs",
                idempotency_key="start",
            )
        )
        identifier = accepted.execution_id
        first = await pending(connection, identifier)
        await connection.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=first.request_id,
                response="first",
                idempotency_key="answer",
            )
        )
        second = await pending(connection, identifier)
        old_prefix = await connection.history(
            history_query.HistoryQuery(
                execution_id=identifier,
                step_ids=[first.request_id],
            )
        )
        await connection.branch(
            branching.Branch(
                execution_id=identifier,
                step_id=second.request_id,
                idempotency_key="insert",
                action=branching.InsertInput(response="second"),
            )
        )
        async with asyncio.timeout(5):
            while (await connection.execution(identifier)).status != "completed":
                await asyncio.sleep(0.01)
        record = await connection.execution(identifier)
        selected = await connection.history(
            history_query.HistoryQuery(
                execution_id=identifier,
                head_step_id=record.checkpoint.heads[record.root_workflow_id],
            )
        )
        assert selected.items[0].step == old_prefix.items[0].step
        assert selected.items[1].step.id != second.request_id
        assert selected.items[-1].step.outcome.model_dump() == {
            "kind": "completed",
            "result": ["first", "second"],
        }
