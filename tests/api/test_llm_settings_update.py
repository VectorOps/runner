import asyncio

import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import llm


async def test_replace_settings_preserves_history_and_inherits_omitted_fields(
    history: MemoryHistory,
) -> None:
    backend = FakeProvider([answer("first"), answer("second")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "update", model="first", temperature=0.5, compaction=None, backend=backend
        )
        agent.add_user_message("hello")
        await agent.generate()
        selected = llm.SettingsOverrides(model="second")
        agent.set_settings(selected)
        assert agent.settings.model == "first"
        agent.add_user_message("continue")
        await agent.generate()
        assert agent.settings.model == "second"
        assert agent.settings.temperature is None
        assert len(agent.history) == 4
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert [settings.model for settings in backend.settings] == ["first", "second"]
    assert [message.role for message in backend.requests[1].messages] == [
        "user",
        "assistant",
        "user",
    ]
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert len(backend.requests) == 2


async def test_settings_cannot_change_during_generation(history: MemoryHistory) -> None:
    backend = FakeProvider([answer("done")])
    backend.release = asyncio.Event()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("busy", model="first", backend=backend)
        agent.add_user_message("hello")
        task = asyncio.create_task(agent.generate())
        try:
            await asyncio.wait_for(backend.started.wait(), 2)
            with pytest.raises(RuntimeError, match="overlapping mutations"):
                agent.set_settings(llm.SettingsOverrides(model="second"))
        finally:
            assert backend.release is not None
            backend.release.set()
            await task
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed", result.outcome
    assert backend.settings[0].model == "first"
