import asyncio
from collections.abc import AsyncGenerator

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c, llm
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.streaming import StreamItem


@pytest.mark.parametrize("measured", [False, True])
@pytest.mark.parametrize("failed", [False, True])
async def test_recorded_generation_does_not_prepare_request_or_digest(
    history: MemoryHistory,
    monkeypatch: pytest.MonkeyPatch,
    measured: bool,
    failed: bool,
) -> None:
    response = answer("recorded")
    if failed:
        response.finish_reason = "length"
    backend = FakeProvider([response])
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            compaction=llm.Compaction(context_window=100000) if measured else None,
        )
        agent.add_user_message("hello")
        if failed:
            with pytest.raises(ActivityError, match="length"):
                await agent.generate()
        else:
            await agent.generate()
        snapshots.append(agent.history)
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    before = dict(history.steps)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("completed replay must not prepare a provider request")

    monkeypatch.setattr(llm.LLM, "_request", forbidden)
    monkeypatch.setattr(llm.LLM, "_input", forbidden)
    assert await replay.run(history, workflow, first.id) == first
    assert snapshots[0] == snapshots[1]
    assert history.steps == before
    assert len(backend.requests) == 1


async def test_request_reuses_owned_messages_before_provider_boundary(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test")
        agent.add_user_message("hello")
        await agent._configure()
        tool = connect.ToolSpec(
            name="echo", description="Echo", input_schema={"type": "object"}
        )

        def forbidden(*args: object, **kwargs: object) -> None:
            raise AssertionError("request assembly must not deepcopy history")

        with monkeypatch.context() as scoped:
            scoped.setattr(connect.UserMessage, "__deepcopy__", forbidden)
            scoped.setattr(connect.ToolSpec, "__deepcopy__", forbidden)
            request = agent._request([tool])
            snapshot = agent._request([tool], snapshot=True)
        for prepared in (request, snapshot):
            assert prepared.messages is not agent._messages
            assert prepared.messages[0] is agent._messages[0]
            assert prepared.tools[0] is tool
        agent.add_user_message("later")
        assert len(request.messages) == len(snapshot.messages) == 1
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome


async def test_provider_mutations_do_not_escape_attempt_or_corrupt_history(
    history: MemoryHistory,
) -> None:
    class MutatingProvider:
        def __init__(self) -> None:
            self.requests: list[connect.GenerateRequest] = []

        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            self.requests.append(request.model_copy(deep=True))
            assert request.tools[0].input_schema == tool.input_schema
            assert request.messages[0].protocol_meta == {
                "nested": {"value": "original"}
            }
            request.messages[0].protocol_meta.clear()
            request.tools[0].input_schema.clear()
            request.messages.clear()
            if len(self.requests) == 1:
                raise TimeoutError("retry")
            yield c.final(answer("done").model_dump(mode="json"))

    backend = MutatingProvider()
    tool = connect.ToolSpec(
        name="echo", description="Echo", input_schema={"type": "object"}
    )
    schema = dict(tool.input_schema)
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            retry=llm.RetryPolicy(max_retries=1, initial_delay=0),
        )
        agent.add_message(
            connect.UserMessage(
                content="hello", protocol_meta={"nested": {"value": "original"}}
            )
        )
        await agent.generate(tools=[tool])
        agent.add_user_message("continue")
        await agent.generate(tools=[tool])
        assert agent.settings.model == "test"
        snapshots.append(agent.history)
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert len(backend.requests) == 3
    assert backend.requests[0] == backend.requests[1]
    assert len(backend.requests[2].messages) == 3
    assert tool.input_schema == schema
    assert await replay.run(history, workflow, first.id) == first
    assert snapshots[0] == snapshots[1]
    assert len(backend.requests) == 3


async def test_branch_prepares_only_new_generation(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = FakeProvider([answer("first"), answer("old"), answer("new")])
    prepared: list[int] = []
    original = llm.LLM._request

    def tracked(
        self: llm.LLM,
        definitions: list[connect.ToolSpec],
        *,
        snapshot: bool = False,
    ) -> connect.GenerateRequest:
        prepared.append(len(self._messages))
        return original(self, definitions, snapshot=snapshot)

    monkeypatch.setattr(llm.LLM, "_request", tracked)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        agent.add_user_message("continue")
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert prepared == [1, 3]
    branch = await replay.run(history, workflow, backend.step_ids[0])
    assert branch.outcome.kind == "completed", branch.outcome
    assert prepared == [1, 3, 3]


async def test_interrupted_generation_rebuilds_request_but_preserves_input(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = FakeProvider([answer("done")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", model="test", backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        return None

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(backend.started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    head = backend.step_ids[0]
    operation = history.steps[head].operation
    assert isinstance(operation, models.StepOperation)
    assert set(operation.input) == {"instance", "request_digest"}

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("resuming an existing activity must preserve its input")

    monkeypatch.setattr(llm.LLM, "_input", forbidden)
    backend.release.set()
    result = await replay.run(history, workflow, head)
    assert result.outcome.kind == "completed", result.outcome
    assert history.steps[head].operation == operation
    assert backend.requests[0].model_dump(exclude={"session"}) == backend.requests[
        1
    ].model_dump(exclude={"session"})
