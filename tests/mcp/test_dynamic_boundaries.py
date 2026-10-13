from typing import Optional

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from tests.mcp.test_discovery import Arguments, catalog, execute
from vectorops.runner import tool_policy
from vectorops.runner.api import llm


async def test_mixed_static_bindings_and_catalog_change_replays_recorded_response(
    history: MemoryHistory,
) -> None:
    selected = catalog("alpha")
    backend = FakeProvider([answer("done")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("dynamic", model="test", backend=backend)
        agent.add_user_message("hello")
        return (
            await agent.run(
                tools=[llm.Tool("static", "Static tool", Arguments, execute)],
                tool_set=selected,
            )
        ).model_dump(mode="json")

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    assert [item.name for item in backend.requests[0].tools] == [
        "static",
        selected.bindings[0].definition.name,
    ]
    selected = catalog("beta")
    assert await replay.run(history, workflow, result.id) == result
    assert len(backend.requests) == 1


async def test_activation_collision_rejects_round_without_committing_results(
    history: MemoryHistory,
) -> None:
    selected = catalog("alpha")
    name = selected.bindings[0].definition.name
    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="search", name=name, arguments={})],
                finish_reason="tool_call",
            )
        ]
    )
    policy = tool_policy.ToolPolicy()
    policy.allow(name)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("dynamic", model="test", backend=backend)
        agent.add_user_message("hello")
        with pytest.raises(ValueError, match="duplicate"):
            await agent.run(
                tools=[llm.Tool("alpha", "Static alpha", Arguments, execute)],
                tool_set=selected,
            )
        assert len(agent.history) == 2
        assert len(selected.bindings) == 1
        return None

    result = await replay.run(history, workflow, tool_policy_version=1, policy=policy)
    assert result.outcome.kind == "completed"
    assert (
        await replay.run(
            history, workflow, result.id, tool_policy_version=1, policy=policy
        )
    ).outcome.kind == "completed"


async def test_successful_search_cannot_activate_a_denied_tool(
    history: MemoryHistory,
) -> None:
    selected = catalog("alpha")
    name = selected.bindings[0].definition.name
    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="search", name=name, arguments={})],
                finish_reason="tool_call",
            ),
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="alpha", name="alpha", arguments={})],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )
    policy = tool_policy.ToolPolicy()
    policy.allow(name)

    async def deny(call: tool_policy.ToolCall) -> Optional[str]:
        return "not allowed" if call.name == "alpha" else None

    policy.register_check("deny-alpha", deny)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("dynamic", model="test", backend=backend)
        agent.add_user_message("hello")
        await agent.run(tool_set=selected)
        message = agent.history[-2]
        assert message.role == "tool" and message.is_error
        return None

    result = await replay.run(history, workflow, tool_policy_version=1, policy=policy)
    assert result.outcome.kind == "completed"


async def test_parallel_llms_share_catalog_without_activation_leaks(
    history: MemoryHistory,
) -> None:
    import asyncio

    selected = catalog("alpha")
    name = selected.bindings[0].definition.name
    searching = FakeProvider(
        [
            connect.AssistantMessage(
                content=[connect.ToolCallBlock(id="search", name=name, arguments={})],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )
    other = FakeProvider([answer("done")])
    policy = tool_policy.ToolPolicy()
    policy.allow(name)

    async def workflow() -> pydantic.JsonValue:
        first = llm.LLM("first", model="test", backend=searching)
        second = llm.LLM("second", model="test", backend=other)
        first.add_user_message("search")
        second.add_user_message("answer")
        await asyncio.gather(
            first.run(tool_set=selected), second.run(tool_set=selected)
        )
        assert len(selected.bindings) == 1
        return None

    result = await replay.run(history, workflow, tool_policy_version=1, policy=policy)
    assert result.outcome.kind == "completed"
    assert len(searching.requests[-1].tools) == 2 and len(other.requests[0].tools) == 1


@pytest.mark.parametrize(
    "arguments",
    [
        {"limit": 0},
        {"limit": 21},
        {"limit": True},
        {"source": "forbidden"},
        {"query": "x" * 513},
    ],
)
def test_search_inputs_are_bounded(arguments: dict[str, pydantic.JsonValue]) -> None:
    with pytest.raises(pydantic.ValidationError):
        catalog("alpha").bindings[0].validate(arguments)
