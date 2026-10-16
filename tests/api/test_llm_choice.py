import asyncio
from collections.abc import Sequence

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import llm
from vectorops.runner.api.llm.choice import FINISH_TOOL
from vectorops.runner.api.llm.models import RoundLimitError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import events


def finish(choice: str = "ready", text: str = "Completed.") -> connect.AssistantMessage:
    return connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(
                id="finish",
                name=FINISH_TOOL,
                arguments={"answer": text, "choice": choice},
            )
        ],
        finish_reason="tool_call",
    )


async def test_shared_history_prompt_restoration_and_replay(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("earlier"), finish(), answer("later")])
    snapshots: list[tuple[connect.Message, ...]] = []
    observed: list[events.StepEvent] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat", model="test", backend=backend, system_prompt="Original."
        )
        agent.add_user_message("First task")
        await agent.generate()
        agent.add_user_message("Now choose")
        result = await agent.run_with_choice(
            {"ready": "Proceed", "blocked": "Need input"}
        )
        assert result == llm.ChoiceResult(answer="Completed.", choice="ready")
        assert agent.history[-1].role == "tool"
        assert agent.history[-1].is_error is False
        snapshots.append(agent.history)
        agent.add_user_message("Continue")
        await agent.run()
        assert agent.usage.total_tokens == 24
        return result.model_dump(mode="json")

    first = await replay.run(history, workflow, event_sink=observed.append)
    assert first.outcome == models.StepCompleted(
        result={"answer": "Completed.", "choice": "ready"}
    )
    assert await replay.run(history, workflow, first.id) == first
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 3
    assert backend.requests[1].messages[1] == answer("earlier")
    assert str(backend.requests[1].system_prompt).startswith("Original.\n\n")
    assert backend.requests[2].system_prompt == "Original."
    assert backend.requests[0].session == backend.requests[2].session
    choice_events = [
        event for event in observed if event.step.id == backend.step_ids[1]
    ]
    assert "partial" not in str(choice_events)


@pytest.mark.parametrize(
    "invalid",
    [
        answer("Missing finish"),
        finish("unknown"),
        finish(text=" "),
        connect.AssistantMessage(
            content=[
                connect.ToolCallBlock(
                    id="a", name=FINISH_TOOL, arguments={"choice": "ready"}
                )
            ],
            finish_reason="tool_call",
        ),
        connect.AssistantMessage(
            content=[
                connect.ToolCallBlock(
                    id=str(i),
                    name=FINISH_TOOL,
                    arguments={"answer": "Done", "choice": "ready"},
                )
                for i in range(2)
            ],
            finish_reason="tool_call",
        ),
    ],
)
async def test_completion_repairs_and_replay(
    history: MemoryHistory, invalid: connect.AssistantMessage
) -> None:
    backend = FakeProvider([invalid, finish()])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("Choose")
        return (await agent.run_with_choice({"ready": "Proceed"})).model_dump(
            mode="json"
        )

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert await replay.run(history, workflow, first.id) == first
    assert len(backend.requests) == 2
    messages = backend.requests[-1].messages
    assert messages[1] == invalid
    assert "Completion was rejected" in str(messages[-1])
    assert all(message.is_error for message in messages if message.role == "tool")


@pytest.mark.parametrize("mixed", [False, True])
async def test_work_tools_not_repeated_during_repairs(
    history: MemoryHistory, mixed: bool
) -> None:
    class Arguments(pydantic.BaseModel):
        pass

    executed: list[str] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        executed.append(ctx.step_id)
        return llm.ToolResponse(content=[connect.TextBlock(text="Evidence")])

    work = connect.AssistantMessage(
        content=[connect.ToolCallBlock(id="work", name="work", arguments={})],
        finish_reason="tool_call",
    )
    invalid = (
        connect.AssistantMessage(
            content=[*finish().content, *work.content], finish_reason="tool_call"
        )
        if mixed
        else answer("Missing finish")
    )
    backend = FakeProvider([work, invalid, work, finish()])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("Do work and choose")
        result = await agent.run_with_choice(
            {"ready": "Proceed"}, tools=[llm.Tool("work", "Work", Arguments, execute)]
        )
        return result.model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert await replay.run(history, workflow, first.id) == first
    assert len(executed) == 1
    assert len(backend.requests) == 4
    assert [tool.name for tool in backend.requests[2].tools] == [FINISH_TOOL]
    results = [
        message for message in backend.requests[-1].messages if message.role == "tool"
    ]
    assert sum(not message.is_error for message in results) == 1
    assert results[-1].is_error


@pytest.mark.parametrize("retries", [0, 2])
async def test_retry_limit_restores_prompt(
    history: MemoryHistory, retries: int
) -> None:
    backend = FakeProvider(
        [finish("unknown") for _ in range(retries + 1)] + [answer("Next")]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, system_prompt="Original")
        agent.add_user_message("Choose")
        with pytest.raises(llm.ChoiceLimitError, match=f"max_retries={retries}"):
            await agent.run_with_choice({"ready": "Proceed"}, max_retries=retries)
        assert agent.history[-1].role == "tool"
        agent.add_user_message("Next task")
        await agent.run()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert await replay.run(history, workflow, first.id) == first
    assert backend.requests[-1].system_prompt == "Original"
    assert len(backend.requests) == retries + 2


async def test_round_budget_includes_repairs(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("Missing")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("Choose")
        with pytest.raises(RoundLimitError, match="max_rounds=1"):
            await agent.run_with_choice({"ready": "Proceed"}, max_rounds=1)
        return None

    assert (await replay.run(history, workflow)).outcome.kind == "completed"
    assert len(backend.requests) == 1


@pytest.mark.parametrize(
    "choices,retries",
    [
        ({}, 2),
        ({"bad name": "description"}, 2),
        ({"ok": ""}, -1),
        ({"ok": ""}, True),
        ({"ok": ""}, 21),
    ],
)
async def test_validation_before_generation(
    history: MemoryHistory, choices: dict[str, str], retries: int
) -> None:
    backend = FakeProvider([])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        with pytest.raises(ValueError):
            await agent.run_with_choice(choices, max_retries=retries)
        return None

    assert (await replay.run(history, workflow)).outcome.kind == "completed"
    assert not backend.requests


async def test_overlap_and_cancellation_restore_conversation(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("Next")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, system_prompt="Original")
        agent.add_user_message("Choose")
        task = asyncio.create_task(agent.run_with_choice({"ready": "Proceed"}))
        await asyncio.wait_for(backend.started.wait(), 2)
        with pytest.raises(RuntimeError, match="overlapping"):
            await agent.generate()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.closed == 1
        assert backend.release is not None
        backend.release.set()
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert backend.requests[-1].system_prompt == "Original"


async def test_interrupted_choice_replays_same_generation(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([finish()])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("Choose")
        return (await agent.run_with_choice({"ready": "Proceed"})).model_dump(
            mode="json"
        )

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(backend.started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.closed == 1
    backend.release.set()
    head = backend.step_ids[0]
    result = await replay.run(history, workflow, head)
    assert result.outcome == models.StepCompleted(
        result={"answer": "Completed.", "choice": "ready"}
    )
    assert backend.step_ids == [head, head]
    assert backend.requests[0].model_dump(exclude={"session"}) == backend.requests[
        1
    ].model_dump(exclude={"session"})


async def test_choice_compaction_and_replay(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("old answer"), answer("summary"), finish()])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("Old task")
        await agent.generate()
        agent.add_user_message("Choose")
        return (await agent.run_with_choice({"ready": "Proceed"})).model_dump(
            mode="json"
        )

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert await replay.run(history, workflow, first.id) == first
    assert len(backend.requests) == 3
    assert "summary" in str(backend.requests[-1].messages)


async def test_dynamic_parallel_tools_replay(history: MemoryHistory) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    executed: list[int] = []
    released = asyncio.Event()

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        if args.value == 1:
            await asyncio.wait_for(released.wait(), 2)
        executed.append(args.value)
        released.set()
        return llm.ToolResponse(content=[connect.TextBlock(text=str(args.value))])

    binding = llm.Tool("work", "Work", Arguments, execute)

    class DynamicTools:
        def __init__(self, completed: bool = False) -> None:
            self.bindings = [] if completed else [binding]

        def advance(
            self, results: Sequence[connect.ToolResultMessage]
        ) -> "DynamicTools":
            assert [result.tool_call_id for result in results] == ["1", "2"]
            return DynamicTools(True)

    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id=str(i), name="work", arguments={"value": i}
                    )
                    for i in (1, 2)
                ],
                finish_reason="tool_call",
            ),
            finish(),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("Work and choose")
        result = await agent.run_with_choice(
            {"ready": "Proceed"}, tool_set=DynamicTools()
        )
        return result.model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert await replay.run(history, workflow, first.id) == first
    assert executed == [2, 1]
    assert [tool.name for tool in backend.requests[-1].tools] == [FINISH_TOOL]


async def test_reserved_tool_rejected(history: MemoryHistory) -> None:
    class Arguments(pydantic.BaseModel):
        pass

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("reserved tool must not execute")

    backend = FakeProvider([])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        with pytest.raises(ValueError, match="reserved choice tool"):
            await agent.run_with_choice(
                {"ready": "Proceed"},
                tools=[llm.Tool(FINISH_TOOL, "Reserved", Arguments, execute)],
            )
        return None

    assert (await replay.run(history, workflow)).outcome.kind == "completed"
    assert not backend.requests
