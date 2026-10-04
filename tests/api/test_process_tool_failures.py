import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.environment.test_processes import FakeDriver
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.environment.processes.manager import ProcessManager
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.mark.parametrize("operation", ["poll", "stdin", "eof", "kill"])
async def test_missing_process_error_reaches_model_and_replays(
    history: MemoryHistory, operation: str
) -> None:
    manager = ProcessManager(FakeDriver(), processes.ProcessConfig())
    calls = 0

    async def execute(
        context: ActivityExecutionContext, arguments: processes.SessionRequest
    ) -> llm.ToolResponse:
        nonlocal calls
        calls += 1
        if operation == "poll":
            await manager.poll(processes.PollRequest(process_id=arguments.process_id))
        elif operation == "kill":
            await manager.kill(arguments)
        else:
            await manager.write_stdin(
                processes.StdinRequest(
                    process_id=arguments.process_id,
                    data=b"input" if operation == "stdin" else b"",
                    eof=operation == "eof",
                )
            )
        raise AssertionError("missing process operation succeeded")

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="call", name=operation, arguments={"process_id": "stale-id"}
                    )
                ],
                finish_reason="tool_call",
            ),
            answer("start a new process"),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("continue the process")
        await agent.run(
            tools=[
                llm.Tool(
                    operation, "Process operation", processes.SessionRequest, execute
                )
            ]
        )
        return None

    try:
        first = await replay.run(history, workflow)
        assert first.outcome.kind == "completed"
        assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
        assert calls == 1
        results = [
            message
            for message in backend.requests[-1].messages
            if message.role == "tool"
        ]
        assert len(results) == 1 and results[0].is_error
        assert results[0].tool_call_id == "call"
        assert results[0].content == [
            connect.TextBlock(
                text="unknown or expired process ID 'stale-id'; "
                "process sessions do not survive environment restarts. "
                "This session cannot be continued. Start a new process with exec "
                "and use its new process ID."
            )
        ]
    finally:
        await manager.close()
