import asyncio
import json
from typing import Optional

import connect
import pydantic
import pytest

from tests.api.test_llm_compaction import Backend, answer, overflow
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm
from vectorops.runner.api.llm import compaction, compaction_context, observability
from vectorops.runner.api.llm.models import CompactionCapacityError, Generation
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError

OLD = "User requires a runnable game, verified against original behavior. " * 150
RECENT = "Keep recent work unchanged."


def policy(**values: object) -> llm.Compaction:
    return llm.Compaction.model_validate(
        {
            "context_window": 10000,
            "keep_recent_tokens": 2000,
            "keep_recent_messages": 2,
            "summary_max_output_tokens": 128,
            **values,
        }
    )


def reasoning_reply(tokens: int) -> connect.AssistantMessage:
    opaque = "opaque-encrypted-payload-" * 100000
    response = answer("Investigated, not verified.", tokens)
    response.content.insert(
        0,
        connect.ReasoningBlock(
            text="Need original execution evidence.",
            signature=opaque,
            protocol_meta={"openai_encrypted_content": opaque},
            redacted=True,
        ),
    )
    return response


def test_projection_excludes_transport_not_business_data() -> None:
    original = reasoning_reply(2000)
    projected = json.dumps(compaction_context.project(original))
    assert "opaque-encrypted-payload" not in projected
    assert "Investigated, not verified." in projected
    assert "Need original execution evidence." in projected
    assert len(projected) < 500
    assert original.content[0].type == "reasoning"
    assert original.content[0].signature is not None
    call = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(
                id="a", name="read", arguments={"signature": "business", "usage": 12}
            )
        ]
    )
    assert '"signature": "business"' in json.dumps(compaction_context.project(call))
    tool = connect.ToolResultMessage(
        tool_call_id="a",
        tool_name="read",
        is_error=True,
        content=[connect.TextBlock(text='{"protocol_meta": "business"}')],
    )
    projected_tool = compaction_context.project(tool)
    assert isinstance(projected_tool, dict)
    assert projected_tool["is_error"] is True
    assert "business" in str(projected_tool)


async def test_projection_reference_context_injection_and_replay(
    history: MemoryHistory,
) -> None:
    original = reasoning_reply(2000)
    backend = Backend(
        [
            original,
            answer("Continue integration", 5000),
            answer("Current work", 8500),
            answer(
                "User wants a runnable game. Original execution remains unverified.",
                3000,
                30,
            ),
            answer("Next", 4000),
        ]
    )
    snapshots: list[tuple[connect.Message, ...]] = []
    tool = connect.ToolSpec(
        name="read", description="Read evidence", input_schema={"type": "object"}
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            system_prompt="Fixed task: finish the game; never weaken acceptance.",
            compaction=policy(additional_instructions="Keep file paths."),
            backend=backend,
        )
        for text in (OLD, "Continue the original request", RECENT):
            agent.add_user_message(text)
            await agent.generate(tools=[tool])
        snapshots.append(agent.history)
        await agent.generate(tools=[tool])
        return None

    first = await replay.run(history, workflow, compaction_version=1)
    assert first.outcome.kind == "completed", first.outcome
    assert (
        await replay.run(history, workflow, first.id, compaction_version=1)
    ).outcome == first.outcome
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 5
    summary = backend.requests[3]
    payload = json.loads(str(summary.messages[0].content))
    assert payload["reference"]["system_prompt"] == backend.requests[0].system_prompt
    assert payload["reference"]["tools"][0]["name"] == "read"
    assert "opaque-encrypted-payload" not in str(summary.messages)
    assert OLD in str(payload["transcript"])
    assert RECENT in str(payload["recent_context"])
    assert summary.tools == []
    assert "Keep file paths." in str(summary.system_prompt)
    assert "ALL user requests" in str(summary.system_prompt)
    assert backend.requests[-1].system_prompt == backend.requests[0].system_prompt
    assert backend.requests[-1].tools == backend.requests[0].tools
    assert snapshots[0][0].role == "user"
    assert str(snapshots[0][0].content).startswith(compaction_context.ENVELOPE)
    assert list(snapshots[0][1:]) == backend.requests[2].messages[3:] + [
        answer("Current work", 8500)
    ]
    assert backend.requests[1].messages[1] == original
    compact_steps = [
        step for step in history.path(first.id) if step.operation.kind == "llm.compact"
    ]
    assert len(compact_steps) == 1
    assert compact_steps[0].metrics is not None
    metric = observability.Metrics.model_validate(compact_steps[0].metrics["llm"])
    assert metric.compaction is not None
    assert metric.compaction.saved_tokens_estimate is not None
    assert metric.compaction.saved_tokens_estimate > 0


def make_summary(
    messages: list[connect.Message],
    *,
    window: int = 10000,
    has_summary: bool = False,
    selected: Optional[llm.Compaction] = None,
) -> compaction_context.Summary:
    return compaction_context.prepare(
        connect.GenerateRequest(
            system_prompt="Preserve the user's requirements.",
            messages=messages + [connect.UserMessage(content=RECENT)],
        ),
        len(messages),
        selected or policy(context_window=window),
        llm.Settings(model="test"),
        has_summary,
    )


def test_summary_cap_is_authoritative() -> None:
    summary = make_summary(
        [connect.UserMessage(content=OLD)],
        selected=policy(
            summary_settings=llm.models.SettingsOverrides(max_output_tokens=9000)
        ),
    )
    assert summary.request.max_output_tokens == 128


def test_oversized_payload_reports_capacity_without_chunking() -> None:
    with pytest.raises(CompactionCapacityError, match="context window"):
        make_summary([connect.UserMessage(content=OLD)], window=1000)


def test_percentage_target_is_soft_and_does_not_inherit_generation_cap() -> None:
    summary = compaction_context.prepare(
        connect.GenerateRequest(messages=[connect.UserMessage(content=OLD)]),
        1,
        llm.Compaction(summary_target_ratio=0.1),
        llm.Settings(model="test", max_output_tokens=32),
        False,
    )
    assert summary.request.max_output_tokens is None
    assert "soft target" in str(summary.request.system_prompt)
    assert "approximately" in str(summary.request.system_prompt)


@pytest.mark.parametrize("output_limit", [6000, 20000])
def test_summary_output_uses_catalog_and_available_context(
    monkeypatch: pytest.MonkeyPatch, output_limit: int
) -> None:
    monkeypatch.setattr(
        connect,
        "default_model_registry",
        connect.ModelRegistry(
            [
                connect.ModelSpec(
                    provider="other",
                    model="summary",
                    api_family="test",
                    context_window=10000,
                    max_output_tokens=output_limit,
                )
            ]
        ),
    )
    summary = make_summary(
        [connect.UserMessage(content=OLD)],
        selected=policy(
            summary_max_output_tokens=None,
            summary_settings=llm.SettingsOverrides(model="other/summary"),
        ),
    )
    size = observability.input_tokens(summary.request)
    assert size is not None
    assert summary.request.max_output_tokens == min(output_limit, 10000 - size)
    assert summary.request.max_output_tokens > 4096


def test_preservation_prompt_keeps_original_intent_and_evidence_rules() -> None:
    summary = make_summary([connect.UserMessage(content=OLD)])
    instruction = str(summary.request.system_prompt)
    assert "Later user corrections supersede earlier requests" in instruction
    assert "Do not turn planned work into completed work" in instruction
    assert (
        "Correct previous-summary errors supported by original context" in instruction
    )
    assert "intent is absent because it is in reference instructions" in instruction
    assert "chunk" not in instruction
    message = compaction_context.replacement([], 0, "User requires verification.")[0]
    assert "does not replace current system instructions" in str(message.content)


async def test_summary_overflow_is_not_retried_and_preserves_history(
    history: MemoryHistory,
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 4000),
            answer("c", 6000),
            answer("d", 8500),
            overflow(),
        ]
    )
    seen: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            compaction=policy(keep_recent_tokens=1000),
            backend=backend,
        )
        for text in (OLD, OLD, "Continue"):
            agent.add_user_message(text)
            await agent.generate()
        agent.add_user_message(RECENT)
        before = agent.history
        with pytest.raises(ActivityError):
            await agent.generate()
        assert agent.history == (*before, answer("d", 8500))
        seen.append(agent.history)
        return None

    first = await replay.run(history, workflow, compaction_version=1)
    assert first.outcome.kind == "completed", first.outcome
    assert (
        await replay.run(history, workflow, first.id, compaction_version=1)
    ).outcome == first.outcome
    assert seen[0] == seen[1]
    assert len(backend.requests) == 5


@pytest.mark.parametrize("failure", ["empty", "tool", "length", "expanded", "cap"])
async def test_failed_summary_does_not_replace_history(
    history: MemoryHistory, failure: str
) -> None:
    result = answer("", output=10)
    if failure == "tool":
        result = connect.AssistantMessage(
            content=[connect.ToolCallBlock(id="bad", name="bad", arguments={})],
            finish_reason="tool_call",
        )
    elif failure == "length":
        result = answer("truncated")
        result.finish_reason = "length"
    elif failure == "expanded":
        result = answer(OLD * 2)
    elif failure == "cap":
        result = answer("summary", output=129)
    backend = Backend([answer("a", 2000), answer("b", 8500), result])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", compaction=policy(), backend=backend)
        agent.add_user_message(OLD)
        await agent.generate()
        agent.add_user_message(RECENT)
        with pytest.raises(ActivityError):
            await agent.generate()
        assert agent.history == (
            connect.UserMessage(content=OLD),
            answer("a", 2000),
            connect.UserMessage(content=RECENT),
            answer("b", 8500),
        )
        return None

    first = await replay.run(history, workflow, compaction_version=1)
    assert first.outcome.kind == "completed", first.outcome
    assert (
        await replay.run(history, workflow, first.id, compaction_version=1)
    ).outcome == first.outcome


@pytest.mark.parametrize("pause_at", [3, 4])
async def test_interrupted_summary_or_continuation_replays(
    history: MemoryHistory, pause_at: int
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 8500),
            answer("Requirements preserved", output=20),
            answer("next", 4000),
        ]
    )
    backend.pause_at = pause_at

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", compaction=policy(), backend=backend)
        for text in (OLD, RECENT, "Next"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    task = asyncio.create_task(replay.run(history, workflow, compaction_version=1))
    await asyncio.wait_for(backend.started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    backend.release.set()
    result = await replay.run(
        history, workflow, backend.identities[-1], compaction_version=1
    )
    assert result.outcome.kind == "completed", result.outcome
    assert backend.identities[pause_at - 1] == backend.identities[pause_at]
    assert len(backend.requests) == 5


def test_defaults_and_legacy_policy_resolution() -> None:
    current = llm.Compaction()
    assert current.trigger_threshold_ratio == 0.8
    assert current.keep_recent_messages == 8
    legacy = current.for_execution(False)
    assert legacy.trigger_threshold_ratio == 0.7
    assert legacy.keep_recent_messages == 4
    explicit = llm.Compaction(trigger_threshold_ratio=0.9, keep_recent_messages=12)
    assert explicit.for_execution(False) == explicit


def test_retention_threshold_output_headroom_and_images() -> None:
    state = compaction.History()
    messages: list[connect.Message] = []
    for index, tokens in enumerate((2000, 4000, 7900)):
        messages.append(connect.UserMessage(content=OLD))
        response = answer(str(index), tokens)
        state.record(
            Generation(
                response=response,
                message_id=str(index),
                context_window=10000,
            ),
            len(messages),
            "same",
        )
        messages.append(response)
    assert state.select(messages, "2", policy(), "same", modern=True) is None
    state.checkpoints[-1].input_tokens = 8000
    selected = state.select(messages, "2", policy(), "same", modern=True)
    assert selected is not None and len(messages) - selected.cut >= 2
    assert (
        state.select(messages, "2", policy(keep_recent_messages=8), "same", modern=True)
        is None
    )
    state.checkpoints[-1].input_tokens = 7900
    state.checkpoints[-1].output_budget = 2500
    assert state.select(messages, "2", policy(), "same", modern=True) is not None
    messages[0] = connect.UserMessage(
        content=[connect.ImageBlock(mime_type="image/png", data="aGVsbG8=")]
    )
    assert state.select(messages, "2", policy(), "same", modern=True) is None
    with pytest.raises(CompactionCapacityError, match="image"):
        compaction_context.project(messages[0])


def test_chunk_boundaries_keep_parallel_tool_exchanges_together() -> None:
    messages: list[connect.Message] = [
        connect.UserMessage(content=OLD),
        connect.AssistantMessage(
            content=[
                connect.ToolCallBlock(id=identity, name="read", arguments={})
                for identity in ("a", "b")
            ]
        ),
        connect.ToolResultMessage(tool_call_id="a", tool_name="read", content=[]),
        connect.ToolResultMessage(tool_call_id="b", tool_name="read", content=[]),
    ]
    assert compaction.safe_boundaries(messages) == {0, 1, 4}


def test_prior_summary_is_separate_and_framing_is_not_accumulated() -> None:
    previous = compaction_context.replacement([], 0, "The user prohibits commits.")[0]
    summary = make_summary(
        [previous, connect.UserMessage(content=OLD)], has_summary=True
    )
    payload = json.loads(str(summary.request.messages[0].content))
    assert "prohibits commits" in str(payload["previous_summary"])
    assert len(payload["transcript"]) == 1
    assert compaction_context.ENVELOPE not in str(payload["transcript"])


async def test_repeated_compaction_keeps_one_summary_and_user_corrections(
    history: MemoryHistory,
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 8500),
            answer("User requires equivalence and prohibits commits.", output=20),
            answer("c", 4000),
            answer("d", 8500),
            answer(
                "User correction: compare behavior, not just unit tests. No commits.",
                output=20,
            ),
            answer("e", 4000),
            answer("f", 8500),
            answer("Behavioral comparison remains unverified. No commits.", output=20),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", compaction=policy(), backend=backend)
        for text in (
            OLD,
            "Do not commit.",
            "Correction: compare behavior, not just unit tests. " + OLD,
            "Continue",
            OLD,
            "Continue",
        ):
            agent.add_user_message(text)
            await agent.generate()
        assert (
            sum(
                compaction_context.ENVELOPE in str(message.content)
                for message in agent.history
            )
            == 1
        )
        assert "unverified" in str(agent.history[0].content)
        return [message.model_dump(mode="json") for message in agent.history]

    first = await replay.run(history, workflow, compaction_version=1)
    assert first.outcome.kind == "completed", first.outcome
    assert (
        await replay.run(history, workflow, first.id, compaction_version=1)
    ).outcome == first.outcome
    second_summary = json.loads(str(backend.requests[5].messages[0].content))
    assert "prohibits commits" in str(second_summary["previous_summary"])
    assert "Correction: compare behavior" in str(second_summary["transcript"])
    assert len(backend.requests) == 9


async def test_summary_replays_without_catalog(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = connect.ModelRegistry(
        [
            connect.ModelSpec(
                provider="other",
                model="summary",
                api_family="test",
                context_window=6500,
            )
        ]
    )
    monkeypatch.setattr(connect, "default_model_registry", registry)
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 4000),
            answer("c", 6000),
            answer("d", 8500),
            answer("All requests preserved", output=20),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            compaction=policy(
                keep_recent_tokens=1000,
                summary_settings=llm.SettingsOverrides(model="other/summary"),
            ),
            backend=backend,
        )
        for text in (OLD, "Continue", "Continue", RECENT):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    first = await replay.run(history, workflow, compaction_version=1)
    assert first.outcome.kind == "completed", first.outcome
    monkeypatch.setattr(connect, "default_model_registry", connect.ModelRegistry([]))
    assert (
        await replay.run(history, workflow, first.id, compaction_version=1)
    ).outcome == first.outcome
    assert len(backend.requests) == 5
    assert all(value.model == "other/summary" for value in backend.settings[4:])


@pytest.mark.parametrize("interrupt", [False, True])
async def test_summary_failure_or_interruption_preserves_originals(
    history: MemoryHistory, interrupt: bool
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 4000),
            answer("c", 6000),
            answer("d", 8500),
            (
                answer("All requests preserved", output=20)
                if interrupt
                else RuntimeError("provider unavailable")
            ),
        ]
    )
    if interrupt:
        backend.pause_at = 5

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            compaction=policy(keep_recent_tokens=1000),
            backend=backend,
        )
        for text in (OLD, OLD, "Continue"):
            agent.add_user_message(text)
            await agent.generate()
        agent.add_user_message(RECENT)
        before = agent.history
        if interrupt:
            try:
                await agent.generate()
            except asyncio.CancelledError:
                assert agent.history == (*before, answer("d", 8500))
                raise
        else:
            with pytest.raises(ActivityError, match="provider unavailable"):
                await agent.generate()
            assert agent.history == (*before, answer("d", 8500))
        return None

    if interrupt:
        task = asyncio.create_task(replay.run(history, workflow, compaction_version=1))
        await asyncio.wait_for(backend.started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        backend.release.set()
        result = await replay.run(
            history, workflow, backend.identities[-1], compaction_version=1
        )
        assert result.outcome.kind == "completed", result.outcome
        assert len(backend.requests) == 6
        assert backend.identities[-1] == backend.identities[-2]
    else:
        result = await replay.run(history, workflow, compaction_version=1)
        assert result.outcome.kind == "completed", result.outcome
        await replay.run(history, workflow, result.id, compaction_version=1)
        assert len(backend.requests) == 5


async def test_generation_overflow_retains_tool_results_without_reexecution(
    history: MemoryHistory,
) -> None:
    class Args(pydantic.BaseModel):
        pass

    executions: list[str] = []

    async def execute(ctx: ActivityExecutionContext, args: Args) -> llm.ToolResponse:
        executions.append(ctx.step_id)
        return llm.ToolResponse(
            content=[connect.TextBlock(text="large result " * 10000)]
        )

    calls = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(id=identity, name="read", arguments={})
            for identity in ("a", "b")
        ],
        finish_reason="tool_call",
        usage=connect.Usage(input_tokens=7000, output_tokens=10, completeness="final"),
    )
    backend = Backend(
        [
            answer("first", 2000),
            calls,
            overflow(),
            answer("Requirements preserved", output=20),
            answer("done", 6000),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        tool = llm.Tool("read", "Read", Args, execute)
        agent = llm.LLM("worker", model="test", backend=backend, compaction=policy())
        agent.add_user_message(OLD)
        await agent.generate(tools=[tool.definition])
        agent.add_user_message("Read evidence")
        await agent.run(tools=[tool])
        assert [
            message.tool_call_id for message in agent.history if message.role == "tool"
        ] == ["a", "b"]
        return None

    result = await replay.run(history, workflow, compaction_version=1)
    assert result.outcome.kind == "completed", result.outcome
    await replay.run(history, workflow, result.id, compaction_version=1)
    assert len(executions) == 2
    assert backend.requests[2].messages[1:] == backend.requests[4].messages[1:]
    assert "large result" not in str(backend.requests[3].messages)
