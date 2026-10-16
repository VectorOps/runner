import connect
from connect.providers.chatgpt import ChatGPTProvider
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm


async def test_session_lasts_for_object_not_workflow_or_identity(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("one"), answer("two"), answer("three")])
    sessions: list[str] = []

    async def workflow() -> pydantic.JsonValue:
        first = llm.LLM("same", model="test", backend=backend)
        first.add_user_message("hello")
        await first.generate()
        sessions.append(first._session_id)
        first.add_user_message("continue")
        await first.generate()
        sessions.append(first._session_id)
        second = llm.LLM("same", model="test", backend=backend)
        second.add_user_message("new conversation")
        await second.generate()
        sessions.append(second._session_id)
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    assert sessions[0] == sessions[1] != sessions[2]
    assert all(sessions)
    for sent, identifier in zip(backend.requests, sessions):
        assert sent.session == connect.SessionHints(session_id=identifier)
    model = connect.default_model_registry.resolve("gpt-6.1-sol", "chatgpt")
    adapter = ChatGPTProvider()
    options = connect.RequestOptions()
    assert (
        adapter.build_headers(model, backend.requests[0], options)["session_id"]
        == sessions[0]
    )
    assert (
        adapter.build_payload(model, backend.requests[0], options)["prompt_cache_key"]
        == sessions[0]
    )
    assert await replay.run(history, workflow, result.id) == result
    assert len(backend.requests) == 3
    assert sessions[3] == sessions[4] != sessions[5]
    assert set(sessions[:3]).isdisjoint(sessions[3:])


async def test_compaction_and_generation_share_object_session(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("summary"), answer("done")])

    async def workflow() -> pydantic.JsonValue:
        conversation = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        conversation.add_user_message("old context")
        conversation.add_message(answer("old answer"))
        conversation.add_user_message("new question")
        await conversation.generate()
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 2
    assert backend.requests[0].session is not None
    assert backend.requests[0].session == backend.requests[1].session
