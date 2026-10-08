import asyncio
import json
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Optional

import connect
import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm import compaction
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.streaming import StreamItem


def answer(text: str, tokens: int = 0, output: int = 10) -> connect.AssistantMessage:
    return connect.AssistantMessage(
        content=[connect.TextBlock(text=text)],
        finish_reason="stop",
        usage=connect.Usage(
            input_tokens=tokens, output_tokens=output, completeness="final"
        ),
    )


def overflow() -> connect.ContextLengthError:
    return connect.ContextLengthError(
        connect.ErrorInfo(code="context_length", message="too large")
    )


class Backend:
    def __init__(self, replies: Sequence[connect.AssistantMessage | Exception]) -> None:
        self.replies = list(replies)
        self.requests: list[connect.GenerateRequest] = []
        self.settings: list[llm.Settings] = []
        self.identities: list[str] = []
        self.pause_at: Optional[int] = None
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        self.requests.append(request.model_copy(deep=True))
        self.settings.append(settings.model_copy(deep=True))
        self.identities.append(context.step_id)
        if len(self.requests) == self.pause_at:
            self.started.set()
            await self.release.wait()
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        yield c.final(reply.model_dump(mode="json"))


def policy(**values: object) -> llm.Compaction:
    return llm.Compaction.model_validate(
        {"context_window": 1000, "keep_recent_tokens": 200, **values}
    )


async def test_post_response_repeated_compaction_and_replay(
    history: MemoryHistory,
) -> None:
    replies = [
        answer("first", 300),
        answer("second", 750),
        answer("summary-one", 400, 50),
    ]
    for index in range(8):
        replies.extend(
            [answer(f"reply-{index}", 800), answer(f"summary-{index}", 400, 50)]
        )
    backend = Backend(replies)
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        for index in range(10):
            agent.add_user_message(f"question-{index}")
            await agent.generate()
        snapshots.append(agent.history)
        assert agent.history[-1].role == "assistant"
        assert agent.history[-1].usage.input_tokens == 800
        return agent.usage.model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 19
    assert backend.requests[1].messages[-1] == connect.UserMessage(content="question-1")
    summaries = [request for request in backend.requests if request.system_prompt]
    initial = json.loads(str(summaries[0].messages[0].content))
    assert initial["previous_summary"] is None
    assert [message["content"] for message in initial["transcript"]] == ["question-0"]
    second = json.loads(str(summaries[1].messages[0].content))
    assert "summary-one" in second["previous_summary"]["content"]
    assert "question-0" not in str(second["transcript"])
    for step in history.path(first.id):
        if isinstance(
            step.operation, models.StepOperation
        ) and step.operation.kind.startswith("llm."):
            assert step.operation.version == 2
            assert len(str(step.operation.input)) < 150


@pytest.mark.parametrize("retry_fails", [False, True])
async def test_overflow_compacts_once_preserves_new_input(
    history: MemoryHistory, retry_fails: bool
) -> None:
    backend = Backend(
        [
            answer("first", 300),
            answer("second", 650),
            overflow(),
            answer("summary", 400, 50),
            overflow() if retry_fails else answer("recovered", 600),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate()
        agent.add_user_message("continue")
        await agent.generate()
        before = agent.history
        agent.add_user_message("large" * 10000)
        if retry_fails:
            with pytest.raises(ActivityError, match="ContextLengthError"):
                await agent.generate()
        else:
            await agent.generate()
        assert agent.history[1:4] == before[1:]
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert len(backend.requests) == 5
    assert backend.requests[-1].messages[1:] == backend.requests[2].messages[1:]
    assert "large" not in str(backend.requests[3].messages)


@pytest.mark.parametrize(
    "case",
    ["no_anchor", "already_compacted", "disabled", "missing_usage", "other_error"],
)
async def test_overflow_without_eligible_prefix_does_not_retry(
    history: MemoryHistory, case: str
) -> None:
    replies: list[connect.AssistantMessage | Exception] = []
    if case in {"already_compacted", "missing_usage"}:
        initial = answer("first", 300)
        second = answer("second", 750)
        if case == "missing_usage":
            initial.usage = second.usage = connect.Usage()
        replies.extend([initial, second])
        if case == "already_compacted":
            replies.append(answer("summary", 400, 50))
    replies.append(ValueError("bad") if case == "other_error" else overflow())
    backend = Backend(replies)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            compaction=None if case == "disabled" else policy(),
        )
        if case in {"already_compacted", "missing_usage"}:
            for _ in range(2):
                agent.add_user_message("old")
                await agent.generate()
        agent.add_user_message("large")
        before = agent.history
        with pytest.raises(ActivityError):
            await agent.generate()
        assert agent.history == before
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert not backend.replies
    assert len(backend.requests) == (
        4 if case == "already_compacted" else 3 if case == "missing_usage" else 1
    )


@pytest.mark.parametrize("failure", ["empty", "tool", "error", "length", "expanding"])
async def test_failed_summary_preserves_history_and_replays(
    history: MemoryHistory, failure: str
) -> None:
    summary: connect.AssistantMessage | Exception = answer("", 400)
    if failure == "tool":
        summary = connect.AssistantMessage(
            content=[connect.ToolCallBlock(id="bad", name="bad", arguments={})],
            finish_reason="tool_call",
        )
    elif failure == "error":
        summary = overflow()
    elif failure == "length":
        summary = answer("partial", 400)
        summary.finish_reason = "length"
    elif failure == "expanding":
        summary = answer("expanded", 400, 500)
    backend = Backend([answer("first", 300), answer("second", 750), summary])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate()
        agent.add_user_message("new")
        with pytest.raises(ActivityError):
            await agent.generate()
        assert len(agent.history) == 4
        assert agent.history[0] == connect.UserMessage(content="old")
        assert agent.history[-1] == answer("second", 750)
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert len(backend.requests) == 3


async def test_tool_results_survive_recovery_without_reexecution(
    history: MemoryHistory,
) -> None:
    class Args(pydantic.BaseModel):
        pass

    executed: list[str] = []

    async def execute(ctx: ActivityExecutionContext, args: Args) -> llm.ToolResponse:
        executed.append(ctx.step_id)
        return llm.ToolResponse(content=[connect.TextBlock(text="huge" * 10000)])

    calls = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(id=identity, name="read", arguments={})
            for identity in ("a", "b")
        ],
        finish_reason="tool_call",
        usage=connect.Usage(input_tokens=650, output_tokens=10, completeness="final"),
    )
    backend = Backend(
        [
            answer("first", 300),
            calls,
            overflow(),
            answer("summary", 400, 50),
            answer("done", 600),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        tool = llm.Tool("read", "Read", Args, execute)
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate(tools=[tool.definition])
        agent.add_user_message("read")
        await agent.run(tools=[tool])
        assert [
            message.tool_call_id for message in agent.history if message.role == "tool"
        ] == ["a", "b"]
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert len(executed) == 2
    assert backend.requests[2].messages[1:] == backend.requests[4].messages[1:]
    assert "huge" not in str(backend.requests[3].messages)


@pytest.mark.parametrize("pause_at", [3, 4])
async def test_interrupted_compaction_or_followup_reuses_commits(
    history: MemoryHistory, pause_at: int
) -> None:
    backend = Backend(
        [
            answer("first", 300),
            answer("second", 750),
            answer("summary", 400, 50),
            answer("done", 500),
        ]
    )
    backend.pause_at = pause_at

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        for text in ("old", "new", "last"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(backend.started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    backend.release.set()
    result = await replay.run(history, workflow, backend.identities[-1])
    assert result.outcome.kind == "completed"
    assert len(backend.requests) == 5
    assert backend.identities[pause_at - 1] == backend.identities[pause_at]
    assert backend.requests[pause_at - 1] == backend.requests[pause_at]


async def test_summary_settings_workspace_and_catalog_replay(
    history: MemoryHistory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = connect.ModelRegistry(
        [
            connect.ModelSpec(
                provider="test", model="main", api_family="test", context_window=1000
            )
        ]
    )
    monkeypatch.setattr(connect, "default_model_registry", registry)
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel="test/main"\n[components.llm.compaction]\nkeep_recent_tokens=200\nsummary_max_output_tokens=80\n[components.llm.compaction.summary_settings]\nmodel="summary"\nprovider="other"\ntemperature=0\n'
    )
    backend = Backend(
        [answer("first", 300), answer("second", 750), answer("summary", 400, 50)]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", backend=backend)
        for text in ("old", "new"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    monkeypatch.setattr(connect, "default_model_registry", connect.ModelRegistry([]))
    (tmp_path / "config.toml").write_text("")
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert backend.settings[-1].model == "summary"
    assert backend.settings[-1].provider == "other"
    assert backend.requests[-1].max_output_tokens == 80
    assert backend.requests[-1].temperature == 0
    assert backend.requests[-1].tools == []


@pytest.mark.parametrize(
    "values",
    [
        {"trigger_threshold_ratio": 0},
        {"trigger_threshold_ratio": 1.1},
        {"keep_recent_ratio": 0.8},
        {"keep_recent_tokens": 0},
        {"context_window": -1},
        {"summary_max_output_tokens": 0},
        {"keep_recent_ratio": float("nan")},
        {"keep_recent_tokens": True},
    ],
)
def test_invalid_policy(values: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        llm.Compaction.model_validate(values)


def test_policy_roundtrip() -> None:
    for value in (
        llm.Compaction(),
        policy(),
        llm.Compaction(max_context_characters=10),
    ):
        assert llm.Compaction.model_validate(value.model_dump()) == value


def test_rebased_coordinates_use_selected_prefix_not_retention_target() -> None:
    state = compaction.History()
    messages: list[connect.Message] = [connect.UserMessage(content="old")]
    for identifier, tokens in (("a", 300), ("b", 750)):
        response = answer(identifier, tokens)
        state.record(
            compaction.Generation(
                response=response, message_id=identifier, context_window=1000
            ),
            len(messages),
            "same",
        )
        messages.extend([response, connect.UserMessage(content="next")])
    selected = state.select(messages, "b", policy(), "same")
    assert selected == compaction.Selection(anchor_id="b", cut=1, prefix_tokens=300)
    state.commit(selected, 50)
    assert [
        (point.input_tokens, point.context_tokens) for point in state.checkpoints
    ] == [(300, 50), (750, 500)]
    assert state.select(messages, "b", policy(), "same", force=True) is None
    assert state.select(messages, "b", policy(), "different", force=True) is None


@pytest.mark.parametrize("completeness", ["none", "partial", "final"])
def test_measured_input_includes_cache_categories(completeness: str) -> None:
    usage = connect.Usage.model_validate(
        {
            "input_tokens": 100,
            "cache_read_tokens": 500,
            "cache_write_tokens": 100,
            "completeness": completeness,
        }
    )
    assert compaction.input_tokens(usage) == (700 if completeness == "final" else None)


@pytest.mark.parametrize("change", ["system", "model", "tools", "missing"])
async def test_incompatible_checkpoints_are_not_subtracted(
    history: MemoryHistory, change: str
) -> None:
    backend = Backend([answer("first", 300), answer("second", 750)])
    if change == "missing":
        response = backend.replies[0]
        assert isinstance(response, connect.AssistantMessage)
        response.usage = connect.Usage()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate()
        if change == "system":
            agent.set_system_prompt("changed")
        if change == "model":
            agent._overrides["model"] = "different"
        agent.add_user_message("new")
        tools = (
            [
                connect.ToolSpec(
                    name="new", description="New", input_schema={"type": "object"}
                )
            ]
            if change == "tools"
            else []
        )
        await agent.generate(tools=tools)
        assert len(agent.history) == 4
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    await replay.run(history, workflow, result.id)
    assert len(backend.requests) == 2


@pytest.mark.parametrize("known_limit", [False, True])
async def test_no_prediction_before_first_request(
    history: MemoryHistory, known_limit: bool
) -> None:
    backend = Backend([answer("first", 900)])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="unknown",
            backend=backend,
            compaction=policy() if known_limit else llm.Compaction(),
        )
        agent.add_user_message("large" * 100000)
        await agent.generate()
        assert len(agent.history) == 2
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    assert len(backend.requests) == 1


async def test_pending_tool_calls_are_retained_during_post_response_compaction(
    history: MemoryHistory,
) -> None:
    calls = connect.AssistantMessage(
        content=[connect.ToolCallBlock(id="a", name="read", arguments={})],
        finish_reason="tool_call",
        usage=connect.Usage(input_tokens=750, output_tokens=10, completeness="final"),
    )
    backend = Backend([answer("first", 300), calls, answer("summary", 400, 50)])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate()
        agent.add_user_message("new")
        await agent.generate()
        assert agent.history[-1] == calls
        agent.add_message(
            connect.ToolResultMessage(
                tool_call_id="a",
                tool_name="read",
                content=[connect.TextBlock(text="result")],
            )
        )
        assert agent.history[-2] == calls
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    await replay.run(history, workflow, result.id)
    assert "tool_call" not in str(backend.requests[-1].messages[0].content)


async def test_unknown_summary_usage_discards_rebased_checkpoints(
    history: MemoryHistory,
) -> None:
    summary = answer("summary", 400)
    summary.usage = connect.Usage()
    backend = Backend(
        [answer("first", 300), answer("second", 750), summary, overflow()]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        for text in ("old", "new"):
            agent.add_user_message(text)
            await agent.generate()
        assert len(agent.history) == 4
        assert not agent._compaction_history.checkpoints
        agent.add_user_message("large")
        with pytest.raises(ActivityError):
            await agent.generate()
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    await replay.run(history, workflow, result.id)
    assert len(backend.requests) == 4


@pytest.mark.parametrize("pause_at", [4, 5])
async def test_overflow_recovery_restart_replays_failure_and_summary(
    history: MemoryHistory, pause_at: int
) -> None:
    backend = Backend(
        [
            answer("first", 300),
            answer("second", 650),
            overflow(),
            answer("summary", 400, 50),
            answer("done", 600),
        ]
    )
    backend.pause_at = pause_at

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        for text in ("old", "recent", "oversized"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(backend.started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    backend.release.set()
    result = await replay.run(history, workflow, backend.identities[-1])
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 6
    assert backend.requests[pause_at - 1] == backend.requests[pause_at]
    assert backend.identities[pause_at - 1] == backend.identities[pause_at]
    assert (await replay.run(history, workflow, result.id)).outcome == result.outcome
    assert len(backend.requests) == 6


def test_selection_never_splits_parallel_tool_exchange() -> None:
    calls = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(id=identity, name="read", arguments={})
            for identity in ("a", "b")
        ],
        finish_reason="tool_call",
    )
    messages: list[connect.Message] = [
        connect.UserMessage(content="old"),
        calls,
        connect.ToolResultMessage(tool_call_id="a", tool_name="read", content=[]),
        connect.ToolResultMessage(tool_call_id="b", tool_name="read", content=[]),
        answer("done", 800),
    ]
    state = compaction.History()
    for identity, index, tokens in (
        ("start", 1, 300),
        ("unsafe", 3, 500),
        ("end", 4, 800),
    ):
        state.record(
            compaction.Generation(
                response=answer(identity, tokens),
                message_id=identity,
                context_window=1000,
            ),
            index,
            "same",
        )
    selected = state.select(messages, "end", policy(), "same")
    assert selected is not None and selected.cut == 1


async def test_ratio_retention_and_cached_input_trigger(history: MemoryHistory) -> None:
    second = answer("second", 100)
    second.usage.cache_read_tokens = 500
    second.usage.cache_write_tokens = 100
    backend = Backend([answer("first", 300), second, answer("summary", 400, 50)])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            compaction=llm.Compaction(context_window=1000),
        )
        for text in ("old", "new"):
            agent.add_user_message(text)
            await agent.generate()
        assert agent.history[-1].role == "assistant"
        assert agent.history[-1].usage == second.usage
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 3


async def test_branch_before_compaction_reconstructs_selected_prefix(
    history: MemoryHistory,
) -> None:
    backend = Backend(
        [
            answer("first", 300),
            answer("second", 750),
            answer("summary", 400, 50),
            answer("branch", 500),
        ]
    )
    text = "old branch"

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend, compaction=policy())
        agent.add_user_message("old")
        await agent.generate()
        agent.add_user_message(text)
        await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    text = "new branch"
    branch = await replay.run(history, workflow, backend.identities[0])
    assert branch.outcome.kind == "completed", branch.outcome
    assert len(backend.requests) == 4
    assert backend.requests[-1].messages == [
        connect.UserMessage(content="old"),
        answer("first", 300),
        connect.UserMessage(content="new branch"),
    ]
