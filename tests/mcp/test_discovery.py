import asyncio
from collections.abc import Sequence
from pathlib import Path

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import mcp_dynamic_workflows as fixture, replay
from tests.fixtures.history import MemoryHistory
from tests.mcp.test_replay import configure
from vectorops.runner import tool_policy
from vectorops.runner.api import llm
from vectorops.runner.mcp.discovery import Discovery, Match, SearchResult
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.workflow import WorkflowRunner


class Arguments(pydantic.BaseModel):
    pass


async def execute(ctx: ActivityExecutionContext, args: Arguments) -> llm.ToolResponse:
    return llm.ToolResponse(content=[connect.TextBlock(text="executed")])


def catalog(*names: str, max_active: int = 32) -> Discovery:
    return Discovery(
        [llm.Tool(name, f"Find {name}", Arguments, execute) for name in names],
        max_active=max_active,
    )


async def search(
    selected: Discovery, query: str, call_id: str = "search"
) -> connect.ToolResultMessage:
    tool = selected.bindings[0]
    result = await tool.execute(
        ActivityExecutionContext(None, "workflow", "step"), {"query": query}
    )
    return connect.ToolResultMessage(
        tool_call_id=call_id, tool_name=tool.definition.name, content=result.content
    )


async def test_search_activation_is_immutable_bounded_and_ordered() -> None:
    selected = catalog("alpha", "beta", "gamma", max_active=1)
    first = await search(selected, "alpha")
    second = await search(selected, "beta", "other")
    advanced = selected.advance([first, second])
    assert len(selected.bindings) == 1
    assert advanced.bindings[-1].definition.name == "beta"
    assert selected.advance([second, first]).bindings[-1].definition.name == "alpha"
    assert advanced.advance([second]).bindings[-1].definition.name == "beta"
    empty = await search(selected, "not-present")
    assert len(selected.advance([empty]).bindings) == 1
    capped = await search(selected, "")
    assert len(selected.advance([capped]).bindings) == 2
    denied = first.model_copy(update={"is_error": True})
    assert len(selected.advance([denied]).bindings) == 1


async def test_activation_rejects_corrupt_or_foreign_results() -> None:
    selected = catalog("alpha")
    result = await search(selected, "alpha")
    block = result.content[0]
    assert isinstance(block, connect.TextBlock)
    value = SearchResult.model_validate_json(block.text)
    foreign = value.model_copy(
        update={"tools": [Match(name="foreign", description="bad")]}
    )
    with pytest.raises(ValueError, match="unavailable"):
        selected.advance(
            [
                result.model_copy(
                    update={
                        "content": [connect.TextBlock(text=foreign.model_dump_json())]
                    }
                )
            ]
        )
    with pytest.raises(ValueError, match="mismatch"):
        selected.advance(
            [
                result.model_copy(
                    update={
                        "content": [
                            connect.TextBlock(
                                text=value.model_copy(
                                    update={"catalog_digest": "wrong"}
                                ).model_dump_json()
                            )
                        ]
                    }
                )
            ]
        )
    assert len(selected.bindings) == 1
    with pytest.raises(ValueError):
        catalog("alpha", max_active=0)


async def test_same_round_hidden_call_is_unknown_then_activated_on_replay(
    history: MemoryHistory,
) -> None:
    selected = catalog("alpha")
    name = selected.bindings[0].definition.name
    provider = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="search", name=name, arguments={"query": "alpha"}
                    ),
                    connect.ToolCallBlock(id="hidden", name="alpha", arguments={}),
                ],
                finish_reason="tool_call",
            ),
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(id="active", name="alpha", arguments={})
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )
    policy = tool_policy.ToolPolicy()
    policy.allow(name)
    policy.allow("alpha")

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("dynamic", model="test", backend=provider)
        agent.add_user_message("hello")
        return (await agent.run(tool_set=selected)).model_dump(mode="json")

    first = await replay.run(history, workflow, tool_policy_version=1, policy=policy)
    assert first.outcome.kind == "completed"
    assert len(provider.requests[0].tools) == 1
    assert len(provider.requests[1].tools) == 2
    hidden = [
        item
        for item in provider.requests[1].messages
        if item.role == "tool" and item.tool_call_id == "hidden"
    ][0]
    assert hidden.role == "tool" and hidden.is_error
    assert (
        await replay.run(
            history, workflow, first.id, tool_policy_version=1, policy=policy
        )
    ).outcome == first.outcome
    assert len(provider.requests) == 3 and len(selected.bindings) == 1


async def test_strict_failed_round_does_not_advance(history: MemoryHistory) -> None:
    selected = catalog("alpha")
    search_name = selected.bindings[0].definition.name

    class Watched:
        @property
        def bindings(self) -> Sequence[llm.ToolBinding]:
            return selected.bindings

        def advance(self, results: Sequence[connect.ToolResultMessage]) -> llm.ToolSet:
            raise AssertionError("failed round advanced selection")

    provider = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(id="search", name=search_name, arguments={}),
                    connect.ToolCallBlock(id="bad", name="unknown", arguments={}),
                ],
                finish_reason="tool_call",
            )
        ]
    )
    policy = tool_policy.ToolPolicy()
    policy.allow(search_name)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "dynamic", model="test", backend=provider, raise_tool_errors=True
        )
        agent.add_user_message("hello")
        with pytest.raises(ActivityError):
            await agent.run(tool_set=Watched())
        assert len(agent.history) == 2
        return None

    assert (
        await replay.run(history, workflow, tool_policy_version=1, policy=policy)
    ).outcome.kind == "completed"


@pytest.mark.parametrize("excluded", [True, False])
async def test_sqlite_dynamic_catalog_and_activation_replay_offline(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, excluded: bool
) -> None:
    configure(workspace)
    monkeypatch.setattr(fixture, "probe", fixture.Probe())
    runner = WorkflowRunner(workspace)
    first = await runner.run(
        "python:tests.fixtures.mcp_dynamic_workflows:workflow",
        input={"excluded": excluded},
    )
    assert len(fixture.probe.requests) == (2 if excluded else 3)
    identifier = runner.last_execution_id
    assert identifier is not None

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("completed replay performed I/O")

    monkeypatch.setattr(MCPRuntime, "discover", forbidden)
    monkeypatch.setattr(MCPRuntime, "call", forbidden)
    (workspace / "config.toml").write_text("")
    assert await WorkflowRunner(workspace).resume(identifier) == first
    assert len(fixture.probe.requests) == (2 if excluded else 3)


async def test_discovery_approval_does_not_grant_concrete_tool(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure(workspace, approve=False)
    monkeypatch.setattr(fixture, "probe", fixture.Probe())
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("python:tests.fixtures.mcp_dynamic_workflows:workflow")
    )
    names: list[str] = []
    answered: set[str] = set()
    try:
        async with asyncio.timeout(5):
            while len(answered) < 2:
                identifier = runner.last_execution_id
                if identifier is not None:
                    page = await runner.pending_inputs(
                        mailbox.RequestQuery(execution_id=identifier)
                    )
                    for request in page.requests:
                        if request.request_id in answered:
                            continue
                        assert isinstance(request.payload, dict)
                        call = connect.ToolCallBlock.model_validate(
                            request.payload["call"]
                        )
                        names.append(call.name)
                        answered.add(request.request_id)
                        await runner.answer_input(
                            mailbox.Answer(
                                execution_id=identifier,
                                request_id=request.request_id,
                                response={
                                    "approved": len(answered) == 1,
                                    "scope": (
                                        "workflow" if len(answered) == 1 else "call"
                                    ),
                                },
                                idempotency_key=request.request_id,
                            )
                        )
                await asyncio.sleep(0.01)
        await task
        assert names[0].startswith("mcp_discover_") and names[1] != names[0]
        last = fixture.probe.requests[-1].messages[-1]
        assert last.role == "tool" and last.is_error
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_interrupted_generation_reconstructs_activation(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure(workspace)
    monkeypatch.setattr(fixture, "probe", fixture.Probe(release=asyncio.Event()))
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("python:tests.fixtures.mcp_dynamic_workflows:workflow")
    )
    try:
        await asyncio.wait_for(fixture.probe.activated.wait(), 5)
        runner.stop()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert fixture.probe.release is not None
        fixture.probe.release.set()
        assert runner.last_execution_id is not None
        await WorkflowRunner(workspace).resume(runner.last_execution_id)
        assert len(fixture.probe.requests) == 4
        assert fixture.probe.requests[1] == fixture.probe.requests[2]
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
