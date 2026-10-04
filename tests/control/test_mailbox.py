import asyncio
import json
from pathlib import Path
import secrets
import sys

import pytest

from vectorops.runner.control import client, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox

WORKFLOW = """
import asyncio
from vectorops.runner.api.mailbox import request_input

async def workflow(ctx):
    return await request_input("user.message", {"prompt": "Your message?"})

async def parallel(ctx):
    return await asyncio.gather(
        request_input("user.message", "First"),
        request_input("user.message", "Second"),
        request_input("approval", "Approve?"),
    )

async def tool(ctx):
    import connect
    import pydantic
    from vectorops.runner.api import llm

    class Arguments(pydantic.BaseModel):
        value: int

    async def execute(context, arguments):
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    result = await llm.invoke_tool(
        llm.Tool("test", "Test", Arguments, execute),
        connect.ToolCallBlock(id="call", name="test", arguments={"value": 1}),
    )
    return result.model_dump(mode="json")

async def tool_twice(ctx):
    await tool(ctx)
    return await tool(ctx)
"""


async def pending(
    connection: client.RunnerClient, execution_id: str, count: int = 1
) -> list[mailbox.InputRequest]:
    async with asyncio.timeout(10):
        while True:
            try:
                page = await connection.pending_inputs(
                    mailbox.RequestQuery(execution_id=execution_id)
                )
            except p.RemoteError:
                await asyncio.sleep(0.01)
                continue
            if len(page.requests) == count:
                return page.requests
            await asyncio.sleep(0.01)


async def completed(connection: client.RunnerClient, execution_id: str) -> None:
    async with asyncio.timeout(10):
        while await connection.current() is not None:
            await asyncio.sleep(0.01)
    assert (await connection.execution(execution_id)).status == "completed"


def response(request: mailbox.InputRequest, value: str = "hello") -> mailbox.Answer:
    return mailbox.Answer(
        execution_id=request.execution_id,
        request_id=request.request_id,
        response=value,
        idempotency_key=request.request_id,
    )


@pytest.mark.parametrize(
    "callable_name,count", [("workflow", 1), ("parallel", 3), ("tool", 1)]
)
async def test_shutdown_restart_restores_same_wait(
    tmp_path: Path, callable_name: str, count: int
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference=f"flow:{callable_name}", idempotency_key="start")
        )
        requests = await pending(connection, accepted.execution_id, count)
        if callable_name == "workflow":
            assert requests[0].channel == "user.message"
            assert requests[0].payload == {"prompt": "Your message?"}
    async with client.embedded(tmp_path) as connection:
        recovered = await pending(connection, accepted.execution_id, count)
        assert recovered == requests
        for request in recovered[1:]:
            await connection.answer_input(response(request))
        restored = recovered[0]
        answer = response(restored)
        if callable_name == "tool":
            assert restored.channel == "tool.approval"
            answer = answer.model_copy(update={"response": {"approved": True}})
        receipt = await connection.answer_input(answer)
        await completed(connection, accepted.execution_id)
        assert await connection.answer_input(answer) == receipt
        with pytest.raises(p.RemoteError, match="already answered"):
            await connection.answer_input(response(restored, "different"))
        assert not (
            await connection.pending_inputs(
                mailbox.RequestQuery(execution_id=accepted.execution_id)
            )
        ).requests


@pytest.mark.parametrize("tcp", [False, True])
async def test_disconnect_reconnect_parallel_channels(
    tmp_path: Path, tcp: bool
) -> None:
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
            async with await connect() as first:
                accepted = await first.start(
                    p.Start(reference="flow:parallel", idempotency_key="start")
                )
                requests = await pending(first, accepted.execution_id, 3)
            async with await connect() as second:
                assert await pending(second, accepted.execution_id, 3) == requests
                filtered = await second.pending_inputs(
                    mailbox.RequestQuery(
                        execution_id=accepted.execution_id, channel="approval"
                    )
                )
                assert len(filtered.requests) == 1
                page = await second.pending_inputs(
                    mailbox.RequestQuery(execution_id=accepted.execution_id, limit=1)
                )
                assert page.next_after is not None
                tail = await second.pending_inputs(
                    mailbox.RequestQuery(
                        execution_id=accepted.execution_id, after=page.next_after
                    )
                )
                assert len(tail.requests) == 2
                await second.subscribe(p.Subscribe(kinds=["mailbox.answered"]))
                for request in reversed(requests):
                    await second.answer_input(response(request))
                async with asyncio.timeout(5):
                    assert (await second.next_event()).event[
                        "kind"
                    ] == "mailbox.answered"
                await completed(second, accepted.execution_id)
        finally:
            await server.aclose()


@pytest.mark.parametrize("callable_name", ["workflow", "tool"])
async def test_explicit_stop_closes_request(tmp_path: Path, callable_name: str) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference=f"flow:{callable_name}", idempotency_key="start")
        )
        request = (await pending(connection, accepted.execution_id))[0]
        await connection.stop(accepted.execution_id)
        async with asyncio.timeout(10):
            while await connection.current() is not None:
                await asyncio.sleep(0.01)
        assert (await connection.execution(accepted.execution_id)).status == "cancelled"
        with pytest.raises(p.RemoteError, match="closed"):
            await connection.answer_input(response(request))
    async with client.embedded(tmp_path) as connection:
        assert await connection.current() is None
        assert not (
            await connection.pending_inputs(
                mailbox.RequestQuery(execution_id=accepted.execution_id)
            )
        ).requests


@pytest.mark.parametrize("answered", [False, True])
@pytest.mark.parametrize("callable_name", ["workflow", "tool", "tool_twice"])
async def test_process_crash_recovers_wait_and_undelivered_answer(
    tmp_path: Path, answered: bool, callable_name: str
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    script = """
import asyncio, json, sys
from pathlib import Path
from vectorops.runner.control import protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory

async def main():
    async with RunnerService(Path(sys.argv[1])) as service:
        accepted = p.Accepted.model_validate(await service.dispatch("executions.start", {
            "reference": f"flow:{sys.argv[3]}", "idempotency_key": "start"
        }))
        query = mailbox.RequestQuery(execution_id=accepted.execution_id)
        while True:
            try:
                page = await service.runner.pending_inputs(query)
            except (FileNotFoundError, KeyError):
                await asyncio.sleep(0.01)
                continue
            if page.requests:
                break
            await asyncio.sleep(0.01)
        request = page.requests[0]
        if sys.argv[2] == "True":
            original = SQLiteHistory.update_step
            async def hold(self, step):
                if step.operation.kind in (mailbox.KIND, "llm.tool") and step.outcome.kind == "completed":
                    await asyncio.Event().wait()
                await original(self, step)
            SQLiteHistory.update_step = hold
            await service.runner.answer_input(mailbox.Answer(
                execution_id=request.execution_id, request_id=request.request_id,
                response={"approved": True, "scope": "workflow"} if sys.argv[3].startswith("tool") else "before crash", idempotency_key="answer"
            ))
        print(request.model_dump_json(), flush=True)
        await asyncio.Event().wait()

asyncio.run(main())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        str(tmp_path),
        str(answered),
        callable_name,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        async with asyncio.timeout(15):
            line = await process.stdout.readline()
        assert line
        request = mailbox.InputRequest.model_validate(json.loads(line))
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
    async with client.embedded(tmp_path) as connection:
        if not answered:
            assert (await pending(connection, request.execution_id))[0] == request
            answer = response(request)
            if callable_name.startswith("tool"):
                answer = answer.model_copy(
                    update={"response": {"approved": True, "scope": "workflow"}}
                )
            await connection.answer_input(answer)
        await completed(connection, request.execution_id)
        restored = await connection.input_request(
            mailbox.RequestRef(
                execution_id=request.execution_id, request_id=request.request_id
            )
        )
        assert restored.response == (
            {"approved": True, "scope": "workflow"}
            if callable_name.startswith("tool")
            else "before crash" if answered else "hello"
        )
