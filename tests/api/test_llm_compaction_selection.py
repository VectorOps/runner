import asyncio
from typing import Literal, Optional

import connect
import pydantic
import pytest

from tests.api.test_llm_compaction import Backend, answer, overflow
from tests.api.test_llm_compaction_context import OLD, policy
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from tests.mcp.test_discovery import catalog
from vectorops.runner import models as runner_models
from vectorops.runner.api import llm
from vectorops.runner.api.llm import compaction, compaction_context, observability
from vectorops.runner.runner import streaming
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.mark.parametrize(
    ("tokens", "expected"),
    [
        ((2000, 6000, 7000, 8500), 1),
        ((2000, 5500, 6200, 8500), 2),
        ((2000, 6100, 7000, 8500), 1),
        ((2000, 5900, 6300, 8500), 1),
        ((6000, 4000, 6500, 8500), 0),
        ((8000, 8100, 8200, 8500), 0),
    ],
)
def test_nearest_boundary_uses_only_provider_counts(
    tokens: tuple[int, ...], expected: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(request: connect.GenerateRequest) -> Optional[int]:
        raise AssertionError("compaction must not estimate tokens")

    monkeypatch.setattr(observability, "input_tokens", forbidden)
    state = compaction.History()
    messages: list[connect.Message] = []
    for index, count in enumerate(tokens):
        messages.append(connect.UserMessage(content=OLD * (index + 1)))
        response = answer(str(index), count, 100)
        state.record(
            compaction.Generation(
                response=response, message_id=str(index), context_window=10000
            ),
            len(messages),
            f"different-tools-{index}",
        )
        messages.append(response)
    selected = state.select(
        messages,
        "3",
        llm.Compaction(context_window=10000, keep_recent_messages=2),
        "another-tool-set",
        modern=True,
    )
    assert selected == compaction.Selection(
        anchor_id="3", cut=2 * expected + 1, prefix_tokens=tokens[expected]
    )


@pytest.mark.parametrize("force", [False, True])
def test_large_context_with_fragmented_tool_history_can_compact(force: bool) -> None:
    state = compaction.History()
    messages: list[connect.Message] = []
    counts = [
        100000,
        200000,
        300000,
        400000,
        500000,
        600000,
        650000,
        700000,
        740000,
        800000,
        820000,
        825000 if force else 844751,
    ]
    for index, count in enumerate(counts):
        messages.append(connect.UserMessage(content="continue"))
        response = answer(str(index), 1, 952)
        response.usage.cache_read_tokens = count - 1
        state.record(
            compaction.Generation(
                response=response, message_id=str(index), context_window=1050000
            ),
            len(messages),
            f"tools-{index}",
        )
        messages.append(response)
    selected = state.select(
        messages,
        "11",
        llm.Compaction(),
        "tools-after-discovery",
        modern=True,
        force=force,
    )
    assert selected == compaction.Selection(
        anchor_id="11", cut=11, prefix_tokens=600000
    )
    state.commit(selected, None)
    assert not state.checkpoints
    assert state.select(messages, "11", llm.Compaction(), force=True) is None


@pytest.mark.parametrize("completeness", ["none", "partial"])
def test_missing_provider_usage_is_not_replaced_with_estimates(
    completeness: str,
) -> None:
    state = compaction.History()
    response = answer(OLD * 100, 9000)
    response.usage = connect.Usage.model_validate(
        {"input_tokens": 9000, "completeness": completeness}
    )
    messages: list[connect.Message] = [connect.UserMessage(content=OLD), response]
    state.record(
        compaction.Generation(
            response=response, message_id="unknown", context_window=10000
        ),
        1,
    )
    assert not state.checkpoints
    assert state.select(messages, "unknown", policy(), force=True) is None


def test_closest_boundary_does_not_split_parallel_tool_results() -> None:
    calls = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(id=identifier, name="read", arguments={})
            for identifier in ("a", "b")
        ]
    )
    messages: list[connect.Message] = [
        connect.UserMessage(content=OLD),
        calls,
        connect.ToolResultMessage(tool_call_id="a", tool_name="read", content=[]),
        connect.ToolResultMessage(tool_call_id="b", tool_name="read", content=[]),
        answer("done", 8500),
    ]
    state = compaction.History()
    for identifier, index, count in (
        ("old", 1, 2000),
        ("unsafe", 3, 6000),
        ("end", 4, 8500),
    ):
        state.record(
            compaction.Generation(
                response=answer(identifier, count),
                message_id=identifier,
                context_window=10000,
            ),
            index,
        )
    assert state.select(messages, "end", policy(), modern=True) == compaction.Selection(
        anchor_id="end", cut=1, prefix_tokens=2000
    )


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("interrupt", [False, True])
async def test_discovery_changes_compact_and_replay_without_repeating_tools(
    history: MemoryHistory, force: bool, interrupt: bool
) -> None:
    selected = catalog("alpha", "beta")
    name = selected.bindings[0].definition.name
    replies: list[connect.AssistantMessage | Exception] = []
    for index, (query, count) in enumerate(
        zip(("alpha", "beta", "alpha", "beta"), (1000, 4000, 5800, 7000))
    ):
        response = answer("", count)
        response.content = [
            connect.ToolCallBlock(
                id=f"search-{index}", name=name, arguments={"query": query}
            )
        ]
        response.finish_reason = "tool_call"
        replies.append(response)
    replies.extend(
        [
            overflow() if force else answer("done", 8500),
            answer("User intent and evidence preserved", output=20),
        ]
    )
    if force:
        replies.append(answer("recovered", 6000))
    backend = Backend(replies)
    if interrupt:
        backend.pause_at = 6

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "dynamic",
            model="test",
            backend=backend,
            compaction=policy(keep_recent_tokens=2500),
        )
        agent.add_user_message(OLD)
        await agent.run(tool_set=selected)
        assert compaction_context.ENVELOPE in str(agent.history[0].content)
        return [message.model_dump(mode="json") for message in agent.history]

    async def execute(head: Optional[str] = None) -> runner_models.WorkflowStep:
        return await replay.run(
            history,
            workflow,
            head,
            compaction_version=3,
            tool_policy_version=1,
            rules=[replay.allow(name)],
        )

    if interrupt:
        task = asyncio.create_task(execute())
        await asyncio.wait_for(backend.started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        backend.release.set()
        result = await execute(backend.identities[-1])
        assert backend.identities[5] == backend.identities[6]
    else:
        result = await execute()
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 6 + force + interrupt
    assert [len(request.tools) for request in backend.requests[:5]] == [1, 2, 3, 3, 3]
    before = dict(history.steps)
    assert (await execute(result.id)).outcome == result.outcome
    assert history.steps == before
    assert len(backend.requests) == 6 + force + interrupt
    tool_steps = [
        step for step in history.path(result.id) if step.operation.kind == "llm.tool"
    ]
    assert len(tool_steps) == 4
    compact_steps = [
        step for step in history.path(result.id) if step.operation.kind == "llm.compact"
    ]
    assert len(compact_steps) == 1
    step = compact_steps[0]
    assert isinstance(step.operation, runner_models.StepOperation)
    debug = step.operation.input["compaction"]
    assert isinstance(debug, dict)
    assert debug["trigger"] == ("overflow" if force else "threshold")
    assert debug["measured_prefix_tokens"] == (4000 if force else 5800)
    assert debug["source_tokens_estimate"] is None
    assert debug["packet_tokens_estimate"] is None
    assert len(selected.bindings) == 1


@pytest.mark.parametrize("version", [None, 1, 2, 3])
async def test_tool_changes_compact_without_a_workflow_version_gate(
    history: MemoryHistory, version: Optional[Literal[1, 2, 3]]
) -> None:
    backend = Backend(
        [answer("first", 2000), answer("second", 8500), answer("Summary", output=20)]
    )
    tool = connect.ToolSpec(
        name="new", description="New tool", input_schema={"type": "object"}
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", compaction=policy(), backend=backend)
        agent.add_user_message(OLD)
        await agent.generate()
        agent.add_user_message("Continue")
        await agent.generate(tools=[tool])
        return [message.model_dump(mode="json") for message in agent.history]

    first = await replay.run(history, workflow, compaction_version=version)
    assert first.outcome.kind == "completed", first.outcome
    before = dict(history.steps)
    assert (
        await replay.run(history, workflow, first.id, compaction_version=version)
    ).outcome == first.outcome
    assert history.steps == before
    assert len(backend.requests) == 3


async def test_discovery_reset_between_user_turns_does_not_block_compaction(
    history: MemoryHistory,
) -> None:
    selected = catalog("alpha", "beta")
    name = selected.bindings[0].definition.name
    searches = []
    for index, (query, tokens) in enumerate((("alpha", 4500), ("beta", 8500))):
        response = answer("", tokens)
        response.content = [
            connect.ToolCallBlock(
                id=f"search-{index}", name=name, arguments={"query": query}
            )
        ]
        response.finish_reason = "tool_call"
        searches.append(response)
    backend = Backend(
        [
            answer("prior", 2000),
            searches[0],
            answer("done", 6500),
            searches[1],
            answer("User intent preserved", output=20),
            answer("continued", 4000),
        ]
    )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "dynamic",
            model="test",
            backend=backend,
            compaction=policy(keep_recent_tokens=2500),
        )
        agent.add_user_message(OLD)
        await agent.generate()
        for text in ("First turn", "Next turn"):
            agent.add_user_message(text)
            await agent.run(tool_set=selected)
        return [message.model_dump(mode="json") for message in agent.history]

    first = await replay.run(
        history,
        workflow,
        compaction_version=3,
        tool_policy_version=1,
        rules=[replay.allow(name)],
    )
    assert first.outcome.kind == "completed", first.outcome
    before = dict(history.steps)
    assert (
        await replay.run(
            history,
            workflow,
            first.id,
            compaction_version=3,
            tool_policy_version=1,
            rules=[replay.allow(name)],
        )
    ).outcome == first.outcome
    assert history.steps == before
    assert [len(request.tools) for request in backend.requests] == [0, 1, 2, 1, 0, 2]
    assert (
        len(
            [
                step
                for step in history.path(first.id)
                if step.operation.kind == "llm.tool"
            ]
        )
        == 2
    )
    compact_step = next(
        step for step in history.path(first.id) if step.operation.kind == "llm.compact"
    )
    assert isinstance(compact_step.operation, runner_models.StepOperation)
    debug = compact_step.operation.input["compaction"]
    assert isinstance(debug, dict)
    assert debug["measured_prefix_tokens"] == 6500


@pytest.mark.parametrize("summary_model", ["test", "other/summary"])
def test_summary_uses_measured_tokens_without_character_estimates(
    monkeypatch: pytest.MonkeyPatch, summary_model: str
) -> None:
    def forbidden(request: connect.GenerateRequest) -> Optional[int]:
        raise AssertionError("compaction must not estimate tokens")

    monkeypatch.setattr(observability, "input_tokens", forbidden)
    summary = compaction_context.prepare(
        connect.GenerateRequest(messages=[connect.UserMessage(content=OLD * 100)]),
        1,
        policy(
            summary_max_output_tokens=None,
            summary_settings=llm.SettingsOverrides(model=summary_model),
        ),
        llm.Settings(model="test"),
        False,
        prefix_tokens=2000,
    )
    assert "approximately 200 tokens" in str(summary.request.system_prompt)
    assert summary.metadata.source_tokens_estimate is None
    assert summary.metadata.packet_tokens_estimate is None
    assert summary.metadata.summary_target_tokens == 200
    assert summary.request.max_output_tokens is None
    response = answer(OLD * 200, output=20)
    result = summary.finalize(
        streaming.ActivityFinal(
            response.model_dump(mode="json"),
            {"llm": observability.Metrics().model_dump(mode="json")},
        ),
        ActivityExecutionContext(None, "workflow", "summary"),
    )
    assert result.value == response.model_dump(mode="json")
    if summary_model == "test":
        response.usage.output_tokens = 2000
        with pytest.raises(llm.models.LLMResponseError, match="did not reduce"):
            summary.finalize(
                streaming.ActivityFinal(response.model_dump(mode="json")),
                ActivityExecutionContext(None, "workflow", "summary"),
            )
