from collections.abc import AsyncGenerator

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import composition, inbox, llm
from vectorops.runner.api import chat_input
from vectorops.runner.runner import inbox as runtime
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem


def payload(text: str) -> chat_input.UserMessageInput:
    return chat_input.UserMessageInput(
        content=[chat_input.TextInput(type="text", text=text)]
    )


async def test_steering_before_final_return_and_replay(history: MemoryHistory) -> None:
    queues = runtime.Registry("execution", "test")

    class Steered(FakeProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            if not self.requests:
                queues.get(
                    runtime.Address(
                        execution_id="execution",
                        workflow_id="test",
                        conversation_id="chat",
                    )
                ).send(payload("change direction"))
            async for item in super().stream(context, settings, request):
                yield item

    backend = Steered([answer("initial"), answer("steered")])
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        messages = inbox.Inbox("chat")
        agent = llm.LLM("chat", model="test", backend=backend, inbox=messages)
        agent.add_user_message("hello")
        response = await agent.run()
        snapshots.append(agent.history)
        return response.model_dump(mode="json")

    first = await replay.run(history, workflow, inboxes=queues)
    assert first.outcome.kind == "completed", first.outcome
    assert len(backend.requests) == 2
    assert [message.role for message in backend.requests[1].messages] == [
        "user",
        "assistant",
        "user",
    ]
    assert backend.requests[1].messages[-1].content == [
        connect.TextBlock(text="change direction")
    ]
    restored = runtime.Registry("execution", "test")

    second = await replay.run(history, workflow, first.id, inboxes=restored)
    assert second == first
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 2
    assert restored.listing().mailboxes[0].queued_messages == 0


async def test_tool_results_precede_steering(history: MemoryHistory) -> None:
    queues = runtime.Registry("execution", "test")
    messages: inbox.Inbox

    class Arguments(pydantic.BaseModel):
        pass

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        queues.get(messages.address).send(payload("steer after tools"))
        return llm.ToolResponse(content=[connect.TextBlock(text="tool result")])

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="call", name="work", arguments={})],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        nonlocal messages
        messages = inbox.Inbox("chat")
        agent = llm.LLM("chat", model="test", backend=backend, inbox=messages)
        agent.add_user_message("hello")
        await agent.run(tools=[llm.Tool("work", "Work", Arguments, execute)])
        return None

    result = await replay.run(history, workflow, inboxes=queues)
    assert result.outcome.kind == "completed", result.outcome
    assert [message.role for message in backend.requests[-1].messages] == [
        "user",
        "assistant",
        "tool",
        "user",
    ]


async def test_recorded_empty_drain_leaves_new_input_queued(
    history: MemoryHistory,
) -> None:
    queues = runtime.Registry("execution", "test")
    prequeue = False

    async def workflow() -> pydantic.JsonValue:
        messages = inbox.Inbox("chat")
        if prequeue:
            queues.get(messages.address).send(payload("new input"))
        assert await messages.drain() == ()
        return None

    first = await replay.run(history, workflow, inboxes=queues)
    assert first.outcome.kind == "completed"
    queues = runtime.Registry("execution", "test")
    prequeue = True
    assert await replay.run(history, workflow, first.id, inboxes=queues) == first
    assert queues.listing().mailboxes[0].queued_messages == 1


async def test_round_limit_preserves_late_input(history: MemoryHistory) -> None:
    queues = runtime.Registry("execution", "test")
    address = runtime.Address(
        execution_id="execution", workflow_id="test", conversation_id="chat"
    )

    class Steered(FakeProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            queues.get(address).send(payload("next turn"))
            async for item in super().stream(context, settings, request):
                yield item

    backend = Steered([answer("done")])

    async def workflow() -> pydantic.JsonValue:
        messages = inbox.Inbox("chat")
        queues.get(messages.address).send(payload("before first round"))
        agent = llm.LLM("chat", model="test", backend=backend, inbox=messages)
        await agent.run(max_rounds=1)
        assert queues.get(messages.address).inspect().queued_messages == 1
        return None

    result = await replay.run(history, workflow, inboxes=queues)
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 1
    assert backend.requests[0].messages[0].content == [
        connect.TextBlock(text="before first round")
    ]


async def test_bound_inbox_rejects_other_workflow_context(
    history: MemoryHistory,
) -> None:
    queues = runtime.Registry("execution", "test")

    async def workflow() -> pydantic.JsonValue:
        messages = inbox.Inbox("chat")
        context = composition.workflow_context()
        token = composition._CURRENT_CONTEXT.set(None)
        try:
            with pytest.raises(RuntimeError):
                await messages.drain()
        finally:
            composition._CURRENT_CONTEXT.reset(token)
        assert composition.workflow_context() is context
        return None

    assert (
        await replay.run(history, workflow, inboxes=queues)
    ).outcome.kind == "completed"


async def test_unavailable_inbox_is_noop(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("done")])

    async def workflow() -> pydantic.JsonValue:
        messages = inbox.Inbox(
            "steer", workflow_id=composition.workflow_context().workflow.id
        )
        assert await messages.drain() == ()
        agent = llm.LLM("chat", model="test", backend=backend, inbox=messages)
        agent.add_user_message("hello")
        await agent.run()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert not any(
        step.operation.kind == "mailbox.drain" for step in history.steps.values()
    )
    assert await replay.run(history, workflow, first.id) == first
    assert len(backend.requests) == 1


async def test_addressed_handles_share_queue_without_cross_target_consumption(
    history: MemoryHistory,
) -> None:
    queues = runtime.Registry("execution", "test")

    async def workflow() -> pydantic.JsonValue:
        first = inbox.Inbox("steer", workflow_id="test")
        second = inbox.Inbox("steer", workflow_id="test")
        other = inbox.Inbox("steer", workflow_id="child")
        assert first.address == second.address
        queues.get(first.address).send(payload("root message"))
        assert await other.drain() == ()
        assert await second.drain() == (payload("root message"),)
        assert await first.drain() == ()
        return None

    result = await replay.run(history, workflow, inboxes=queues)
    assert result.outcome.kind == "completed", result.outcome
