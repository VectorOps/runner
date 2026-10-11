import asyncio
from pathlib import Path

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import diagnostics
from vectorops.runner.api import llm
from vectorops.runner.authentication.models import AuthenticationError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError


class Arguments(pydantic.BaseModel):
    value: int = 0


async def test_tool_authentication_failure_fails_workflow(
    history: MemoryHistory,
) -> None:
    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        raise AuthenticationError("configure credentials before starting workflow")

    async def workflow() -> pydantic.JsonValue:
        await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute),
            connect.ToolCallBlock(id="call", name="test", arguments={}),
        )
        raise AssertionError("authentication failure was swallowed")

    completion = await replay.run(history, workflow)
    assert completion.outcome.kind == "failed"
    assert completion.outcome.error.type == "AuthenticationError"
    assert (
        await replay.run(history, workflow, completion.id)
    ).outcome == completion.outcome


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("exception_type", [RuntimeError, llm.ToolExecutionError])
async def test_tool_exceptions_are_recorded_logged_and_optionally_raised(
    history: MemoryHistory,
    tmp_path: Path,
    strict: bool,
    exception_type: type[Exception],
) -> None:
    calls = 0
    text = "original error\nwith details"

    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        nonlocal calls
        calls += 1
        raise exception_type(text)

    async def workflow() -> pydantic.JsonValue:
        tool = llm.Tool("test", "Test", Arguments, execute)
        request = connect.ToolCallBlock(id="call", name="test", arguments={})
        if strict:
            with pytest.raises(ActivityError) as error:
                await llm.invoke_tool(tool, request, raise_errors=True)
            assert error.value.error.message == text
        else:
            result = await llm.invoke_tool(tool, request)
            assert result.is_error
            assert result.content == [connect.TextBlock(text=text)]
        return None

    async with diagnostics.DiagnosticLog(tmp_path) as logs:
        with logs.activate():
            first = await replay.run(history, workflow)
            assert first.outcome.kind == "completed"
            assert (
                await replay.run(history, workflow, first.id)
            ).outcome == first.outcome
    assert calls == 1
    entries = [
        diagnostics.LogRecord.model_validate_json(line)
        for path in (tmp_path / "logs").glob("*.jsonl")
        for line in path.read_bytes().splitlines()
    ]
    failures = [entry for entry in entries if entry.event == "llm.tool_failed"]
    assert len(failures) == 1 and failures[0].step_id is not None
    assert failures[0].fields["error_type"] == exception_type.__name__
    assert "original error" not in failures[0].model_dump_json()


async def test_parallel_errors_and_successes_reach_model_in_call_order(
    history: MemoryHistory,
) -> None:
    calls: list[int] = []
    released = asyncio.Event()

    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        if arguments.value == 1:
            await released.wait()
            calls.append(1)
            raise RuntimeError("exact failure")
        calls.append(2)
        released.set()
        return llm.ToolResponse(content=[connect.TextBlock(text="success")])

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id=str(value), name="test", arguments={"value": value}
                    )
                    for value in (1, 2)
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("hello")
        await agent.run(tools=[llm.Tool("test", "Test", Arguments, execute)])
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert calls == [2, 1]
    results = [
        message for message in backend.requests[-1].messages if message.role == "tool"
    ]
    assert [message.tool_call_id for message in results] == ["1", "2"]
    assert [message.is_error for message in results] == [True, False]
    assert results[0].content == [connect.TextBlock(text="exact failure")]


async def test_tool_cancellation_never_becomes_error_response(
    history: MemoryHistory,
) -> None:
    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        raise asyncio.CancelledError

    async def workflow() -> pydantic.JsonValue:
        await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute),
            connect.ToolCallBlock(id="call", name="test", arguments={}),
        )
        return None

    with pytest.raises(asyncio.CancelledError):
        await replay.run(history, workflow)
