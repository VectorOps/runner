import asyncio
from collections.abc import AsyncGenerator
from typing import Optional

import connect
import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models, tool_policy
from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm.models import RoundLimitError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.streaming import StreamItem


def answer(text: str) -> connect.AssistantMessage:
    return connect.AssistantMessage(
        content=[connect.TextBlock(text=text)],
        finish_reason="stop",
        usage=connect.Usage(input_tokens=10, output_tokens=2, total_tokens=12),
        protocol_state={"opaque": "continuation"},
    )


class FakeProvider:
    def __init__(self, responses: list[connect.AssistantMessage]) -> None:
        self.responses = responses
        self.requests: list[connect.GenerateRequest] = []
        self.settings: list[llm.Settings] = []
        self.step_ids: list[str] = []
        self.closed = 0
        self.started = asyncio.Event()
        self.release: Optional[asyncio.Event] = None

    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        assert c._CURRENT_CONTEXT.get() is None
        self.requests.append(request.model_copy(deep=True))
        self.settings.append(settings.model_copy(deep=True))
        self.step_ids.append(context.step_id)
        try:
            yield c.delta({"text": "partial"})
            self.started.set()
            if self.release is not None:
                await self.release.wait()
            yield c.final(self.responses.pop(0).model_dump(mode="json"))
        finally:
            self.closed += 1


async def test_history_reconstructs_without_growing_inputs(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("first"), answer("second")])
    snapshots: list[list[pydantic.JsonValue]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.set_system_prompt("system prompt")
        agent.add_user_message("large input " * 1000)
        first = await agent.generate()
        first.protocol_state.clear()
        snapshot = agent.history
        assert snapshot[-1].role == "assistant"
        snapshot[-1].protocol_state.clear()
        agent.add_user_message("continue")
        await agent.generate()
        snapshots.append([message.model_dump(mode="json") for message in agent.history])
        assert agent.usage.total_tokens == 24
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 2
    assert backend.requests[1].messages[1].role == "assistant"
    assert backend.requests[1].messages[1].protocol_state == {"opaque": "continuation"}
    inputs = [
        step.operation.input
        for step in history.path(first.id)
        if isinstance(step.operation, models.StepOperation)
        and step.operation.kind == "llm.generate"
    ]
    assert len(inputs) == 2
    assert all(set(item) == {"instance", "request_digest"} for item in inputs)
    assert all(len(str(item)) < 150 for item in inputs)


async def test_changed_prompt_replays_recorded_response(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("done")])
    prompt = "original"

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat", llm.Settings(model="test"), system_prompt=prompt, backend=backend
        )
        agent.add_user_message("hello")
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    prompt = "changed"
    second = await replay.run(history, workflow, first.id)
    assert second == first
    assert len(backend.requests) == 1


@pytest.mark.parametrize("gated", [False, True])
@pytest.mark.parametrize("removed", [False, True])
async def test_changed_tool_schema_and_version_replay_then_use_live_definition(
    history: MemoryHistory, gated: bool, removed: bool
) -> None:
    class OldArguments(pydantic.BaseModel):
        value: int

    class NewArguments(pydantic.BaseModel):
        replacement: str

    calls: list[int] = []

    async def old_execute(
        ctx: ActivityExecutionContext, args: OldArguments
    ) -> llm.ToolResponse:
        calls.append(args.value)
        return llm.ToolResponse(content=[connect.TextBlock(text="old result")])

    async def new_execute(
        ctx: ActivityExecutionContext, args: NewArguments
    ) -> llm.ToolResponse:
        raise AssertionError("cached arguments must fail current validation")

    call = connect.ToolCallBlock(id="a", name="echo", arguments={"value": 1})
    backend = FakeProvider(
        [
            connect.AssistantMessage(content=[call], finish_reason="tool_call"),
            answer("done"),
        ]
    )
    binding = llm.Tool("echo", "Old description", OldArguments, old_execute)
    bindings: list[llm.ToolBinding] = [binding]
    policy = tool_policy.ToolPolicy()
    policy.allow("echo")
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("hello")
        await agent.run(tools=bindings)
        snapshots.append(agent.history)
        return None

    first = await replay.run(
        history, workflow, tool_policy_version=1 if gated else None, policy=policy
    )
    assert first.outcome.kind == "completed", first.outcome
    bindings = (
        []
        if removed
        else [
            llm.Tool(
                "echo", "Changed description", NewArguments, new_execute, version=99
            )
        ]
    )
    before = dict(history.steps)
    second = await replay.run(
        history,
        workflow,
        first.id,
        tool_policy_version=1 if gated else None,
        policy=policy,
    )
    assert second == first
    assert snapshots[0] == snapshots[1]
    assert history.steps == before
    assert calls == [1]
    assert len(backend.requests) == 2

    async def live() -> pydantic.JsonValue:
        result = await llm.invoke_tool(bindings[0] if bindings else None, call)
        assert result.is_error
        return None

    assert (
        await replay.run(
            history, live, tool_policy_version=1 if gated else None, policy=policy
        )
    ).outcome.kind == "completed"
    assert calls == [1]


async def test_new_object_resets_history(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("one"), answer("two")])

    async def workflow() -> pydantic.JsonValue:
        for name in ("first", "second"):
            agent = llm.LLM(name, llm.Settings(model="test"), backend=backend)
            agent.add_user_message(name)
            await agent.generate()
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    assert [len(request.messages) for request in backend.requests] == [1, 1]


async def test_tool_loop_replays_results_in_order(history: MemoryHistory) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    executed: list[int] = []
    second_finished = asyncio.Event()

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        if args.value == 1:
            async with asyncio.timeout(2):
                await second_finished.wait()
        executed.append(args.value)
        if args.value == 2:
            second_finished.set()
        return llm.ToolResponse(content=[connect.TextBlock(text=str(args.value))])

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(id="a", name="echo", arguments={"value": 1}),
                    connect.ToolCallBlock(id="b", name="echo", arguments={"value": 2}),
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        result = await agent.run(tools=[llm.Tool("echo", "Echo", Arguments, execute)])
        snapshots.append(agent.history)
        return result.model_dump(mode="json")

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    await replay.run(history, workflow, result.id)
    assert executed == [2, 1]
    assert snapshots[0] == snapshots[1]
    assert [
        message.tool_call_id
        for message in backend.requests[1].messages
        if message.role == "tool"
    ] == ["a", "b"]
    assert [message.role for message in backend.requests[1].messages] == [
        "user",
        "assistant",
        "tool",
        "tool",
    ]


async def test_compaction_is_recorded_and_reconstructed(history: MemoryHistory) -> None:
    backend = FakeProvider(
        [answer("old answer"), answer("summary"), answer("new answer")]
    )
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            llm.Settings(model="test"),
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("old question")
        await agent.generate()
        agent.add_user_message("new question")
        await agent.generate()
        snapshots.append(agent.history)
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    await replay.run(history, workflow, result.id)
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 3
    assert [
        step.operation.kind
        for step in history.path(result.id)
        if step.operation.kind != "workflow.resume"
    ] == [
        "configuration.get",
        "llm.generate",
        "configuration.get",
        "llm.compact",
        "llm.generate",
        "workflow.complete",
    ]
    assert len(snapshots[0]) == 3
    assert snapshots[0][0].role == "user"
    assert snapshots[0][0].content == "Summary of earlier conversation:\nsummary"


async def test_overlap_rejected_and_partial_state_not_committed(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("done")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        task = asyncio.create_task(agent.generate())
        await backend.started.wait()
        assert len(agent.history) == 1
        with pytest.raises(RuntimeError, match="overlapping"):
            await agent.generate()
        with pytest.raises(RuntimeError, match="overlapping"):
            agent.set_system_prompt("changed")
        assert backend.release is not None
        backend.release.set()
        await task
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"


async def test_cancellation_restarts_same_round(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("done")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        return len(agent.history)

    task = asyncio.create_task(replay.run(history, workflow))
    await backend.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.closed == 1
    head = backend.step_ids[0]
    backend.release.set()
    result = await replay.run(history, workflow, head)
    assert result.outcome == models.StepCompleted(result=2)
    assert backend.step_ids == [head, head]
    assert backend.requests[0].session != backend.requests[1].session
    assert backend.requests[0].model_dump(exclude={"session"}) == backend.requests[
        1
    ].model_dump(exclude={"session"})


async def test_failed_response_does_not_mutate_history(history: MemoryHistory) -> None:
    response = answer("truncated")
    response.finish_reason = "length"
    backend = FakeProvider([response])
    lengths: list[int] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        with pytest.raises(ActivityError):
            await agent.generate()
        lengths.append(len(agent.history))
        return None

    result = await replay.run(history, workflow)
    await replay.run(history, workflow, result.id)
    assert lengths == [1, 1]
    assert len(backend.requests) == 1


async def test_round_limit_and_unknown_tool(history: MemoryHistory) -> None:
    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="a", name="unknown", arguments={})],
                finish_reason="tool_call",
            )
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        with pytest.raises(RoundLimitError):
            await agent.run(max_rounds=1)
        message = agent.history[-1]
        assert message.role == "tool" and message.is_error
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"


def test_constructor_requires_workflow_and_settings_are_validated() -> None:
    with pytest.raises(RuntimeError, match="active workflow"):
        llm.LLM("chat", llm.Settings(model="test"))
    for values in [{"model": " "}, {"model": "test", "timeout": -1}]:
        with pytest.raises(pydantic.ValidationError):
            llm.Settings.model_validate(values)


@pytest.mark.parametrize("invalid_arguments", [False, True])
async def test_tool_errors_are_model_visible_by_default(
    history: MemoryHistory, invalid_arguments: bool
) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    calls = 0

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        nonlocal calls
        calls += 1
        raise RuntimeError("infrastructure failure")

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="a",
                        name="test",
                        arguments={"value": "invalid" if invalid_arguments else 1},
                    )
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        bindings = [llm.Tool("test", "Test", Arguments, execute)]
        await agent.run(tools=bindings)
        result = agent.history[-2]
        assert result.role == "tool" and result.is_error
        if not invalid_arguments:
            assert result.content == [connect.TextBlock(text="infrastructure failure")]
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert calls == (0 if invalid_arguments else 1)


async def test_compaction_survives_failure_of_next_generation(
    history: MemoryHistory,
) -> None:
    failed = answer("partial")
    failed.finish_reason = "length"
    backend = FakeProvider([answer("old answer"), answer("summary"), failed])
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            llm.Settings(model="test"),
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("old question")
        await agent.generate()
        agent.add_user_message("new question")
        with pytest.raises(ActivityError):
            await agent.generate()
        snapshots.append(agent.history)
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert snapshots[0] == snapshots[1]
    assert len(snapshots[0]) == 2
    assert len(backend.requests) == 3


async def test_compaction_keeps_tool_call_and_results_together(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("summary"), answer("done")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            llm.Settings(model="test"),
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("hello")
        agent.add_message(
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(id="a", name="test", arguments={}),
                    connect.ToolCallBlock(id="b", name="test", arguments={}),
                ],
                finish_reason="tool_call",
            )
        )
        for identity in ("a", "b"):
            agent.add_message(
                connect.ToolResultMessage(
                    tool_call_id=identity,
                    tool_name="test",
                    content=[connect.TextBlock(text="ok")],
                )
            )
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert [message.role for message in backend.requests[-1].messages] == [
        "user",
        "assistant",
        "tool",
        "tool",
    ]


async def test_storage_growth_is_linear(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("reply") for _ in range(20)])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        for _ in range(20):
            agent.add_user_message("payload" * 1000)
            await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    inputs = [
        step.operation.input
        for step in history.path(first.id)
        if isinstance(step.operation, models.StepOperation)
        and step.operation.kind == "llm.generate"
    ]
    assert len(inputs) == 20
    assert len({len(str(value)) for value in inputs}) == 1
    assert len(backend.requests[-1].messages) == 39


async def test_branch_reconstructs_only_selected_prefix(history: MemoryHistory) -> None:
    backend = FakeProvider(
        [answer("first"), answer("old branch"), answer("new branch")]
    )
    user_input = "old branch"

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        agent.add_user_message(user_input)
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    user_input = "new branch"
    result = await replay.run(history, workflow, backend.step_ids[0])
    assert result.outcome.kind == "completed"
    assert len(backend.requests) == 3
    assert len(backend.requests[-1].messages) == 3
    assert backend.requests[-1].messages[-1] == connect.UserMessage(
        content="new branch"
    )


async def test_interrupt_after_compaction_reuses_summary(
    history: MemoryHistory,
) -> None:
    class InterruptingProvider(FakeProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            if len(self.requests) == 2:
                self.release = asyncio.Event()
            async for item in super().stream(context, settings, request):
                yield item

    backend = InterruptingProvider(
        [answer("old answer"), answer("summary"), answer("done")]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            llm.Settings(model="test"),
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("old question")
        await agent.generate()
        agent.add_user_message("new question")
        await agent.generate()
        return len(agent.history)

    task = asyncio.create_task(replay.run(history, workflow))
    async with asyncio.timeout(2):
        while len(backend.requests) < 3:
            await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.release is not None
    backend.release.set()
    result = await replay.run(history, workflow, backend.step_ids[-1])
    assert result.outcome == models.StepCompleted(result=3)
    assert len(backend.requests) == 4
    assert backend.requests[2].session != backend.requests[3].session
    assert backend.requests[2].model_dump(exclude={"session"}) == backend.requests[
        3
    ].model_dump(exclude={"session"})


async def test_cross_workflow_state_reuse_rejected(history: MemoryHistory) -> None:
    agents: list[llm.LLM] = []

    async def first() -> pydantic.JsonValue:
        agents.append(llm.LLM("chat", llm.Settings(model="test")))
        return None

    async def second() -> pydantic.JsonValue:
        with pytest.raises(RuntimeError, match="different workflow context"):
            agents[0].add_user_message("not allowed")
        return None

    await replay.run(history, first)
    result = await replay.run(MemoryHistory(), second)
    assert result.outcome.kind == "completed"


async def test_parallel_tool_failure_settles_siblings_before_return(
    history: MemoryHistory,
) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    failed = asyncio.Event()
    completed: list[int] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        if args.value == 1:
            failed.set()
            raise RuntimeError("broken tool")
        await failed.wait()
        await asyncio.sleep(0)
        completed.append(args.value)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

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
            )
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        agent.set_raise_tool_errors(True)
        with pytest.raises(ActivityError, match="broken tool"):
            await agent.run(tools=[llm.Tool("test", "Test", Arguments, execute)])
        assert completed == [2]
        assert len(agent.history) == 2
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    second = await replay.run(history, workflow, first.id)
    assert second.outcome.kind == "completed"
    assert completed == [2]


async def test_parallel_tool_cancellation_restarts_unfinished_calls(
    history: MemoryHistory,
) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    started = asyncio.Event()
    release = asyncio.Event()
    invocations: list[tuple[int, str]] = []
    closed: list[int] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        invocations.append((args.value, ctx.step_id))
        if len(invocations) == 2:
            started.set()
        try:
            await release.wait()
            return llm.ToolResponse(content=[connect.TextBlock(text="done")])
        finally:
            closed.append(args.value)

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
        agent = llm.LLM("chat", llm.Settings(model="test"), backend=backend)
        agent.add_user_message("hello")
        await agent.run(tools=[llm.Tool("test", "Test", Arguments, execute)])
        return len(agent.history)

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sorted(closed) == [1, 2]
    release.set()
    result = await replay.run(history, workflow, invocations[-1][1])
    assert result.outcome == models.StepCompleted(result=5)
    assert invocations[:2] == invocations[2:]
    assert len(backend.requests) == 2
