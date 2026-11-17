import asyncio
from collections.abc import Sequence
from typing import Literal, Optional

import connect
import pydantic
import pytest

from tests.api.test_llm_compaction import Backend, answer
from tests.api.test_llm_compaction_context import OLD, policy
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import llm
from vectorops.runner.api.llm import compaction, compaction_context
from vectorops.runner.runner.errors import ReplayMismatchError


def old_compaction_inputs(history: MemoryHistory, *, metadata: bool = True) -> None:
    """Project saved compaction inputs into their pre-refactor format."""
    for identifier, step in list(history.steps.items()):
        operation = step.operation
        if (
            not isinstance(operation, models.StepOperation)
            or operation.kind != "llm.compact"
        ):
            continue
        values = dict(operation.input)
        debug = values.get("compaction")
        if metadata:
            assert isinstance(debug, dict)
            values["compaction"] = {
                key: value for key, value in debug.items() if key != "selection"
            }
        else:
            values.pop("compaction", None)
        history.steps[identifier] = step.model_copy(
            update={"operation": operation.model_copy(update={"input": values})}
        )


def old_prepare(
    before: connect.GenerateRequest,
    cut: int,
    selected: llm.Compaction,
    generation: llm.Settings,
    has_summary: bool,
    *,
    prefix_tokens: Optional[int] = None,
) -> compaction_context.Summary:
    return original_prepare(before, cut, selected, generation, has_summary)


original_prepare = compaction_context.prepare


@pytest.mark.parametrize("version", [None, 1, 2, 3])
async def test_recorded_absence_is_replayed_and_live_frontier_uses_new_selector(
    history: MemoryHistory,
    monkeypatch: pytest.MonkeyPatch,
    version: Optional[Literal[1, 2, 3]],
) -> None:
    backend = Backend([answer("first", 2000), answer("second", 8500)])
    continuing = False

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", backend=backend, compaction=policy())
        for text in (OLD, "Continue"):
            agent.add_user_message(text)
            await agent.generate()
        if continuing:
            agent.add_user_message("New work")
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    with monkeypatch.context() as patch:
        patch.setattr(compaction.History, "select", lambda *args, **kwargs: None)
        first = await replay.run(history, workflow, compaction_version=version)
    assert first.outcome.kind == "completed", first.outcome
    before = dict(history.steps)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("replay must follow recorded compaction decisions")

    with monkeypatch.context() as patch:
        patch.setattr(compaction.History, "select", forbidden)
        assert (
            await replay.run(history, workflow, first.id, compaction_version=version)
        ).outcome == first.outcome
    assert history.steps == before
    assert len(backend.requests) == 2
    continuing = True
    backend.replies.extend(
        [answer("New continuation summary", output=20), answer("new", 4000)]
    )
    result = await replay.run(
        history, workflow, backend.identities[1], compaction_version=version
    )
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 4
    assert "New continuation summary" in str(backend.requests[-1].messages[0].content)
    assert history.workflows["test"].compaction_version == version


@pytest.mark.parametrize("record_format", ["current", "old", "v3"])
async def test_recorded_boundary_is_used_without_running_selector_or_preparing_requests(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch, record_format: str
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 5000),
            answer("c", 8500),
            answer("Summary", output=20),
            answer("d", 4000),
        ]
    )
    target = 6000
    snapshots: list[tuple[connect.Message, ...]] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            backend=backend,
            compaction=policy(keep_recent_tokens=target),
        )
        for text in (OLD, "Continue", "More", "Next"):
            agent.add_user_message(text)
            await agent.generate()
        snapshots.append(agent.history)
        return [message.model_dump(mode="json") for message in agent.history]

    with monkeypatch.context() as patch:
        if record_format == "old":
            patch.setattr(compaction_context, "prepare", old_prepare)
        first = await replay.run(history, workflow, compaction_version=2)
    assert first.outcome.kind == "completed", first.outcome
    if record_format != "current":
        old_compaction_inputs(history)
    before = dict(history.steps)
    target = 2500

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError(
            "completed compaction must use recorded boundary and response"
        )

    monkeypatch.setattr(compaction.History, "select", forbidden)
    monkeypatch.setattr(llm.LLM, "_request", forbidden)
    assert (
        await replay.run(history, workflow, first.id, compaction_version=2)
    ).outcome == first.outcome
    assert snapshots[0] == snapshots[1]
    assert history.steps == before
    assert len(backend.requests) == 5


@pytest.mark.parametrize("provider_tokens", [False, True])
async def test_old_interrupted_summary_resumes_with_its_recorded_boundary_and_request(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch, provider_tokens: bool
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 8500),
            answer("Summary", output=20),
            answer("c", 4000),
        ]
    )
    backend.pause_at = 3

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            backend=backend,
            compaction=policy(keep_recent_tokens=6000),
        )
        for text in (OLD, "Continue", "Next"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    with monkeypatch.context() as patch:
        if not provider_tokens:
            patch.setattr(compaction_context, "prepare", old_prepare)
        task = asyncio.create_task(replay.run(history, workflow, compaction_version=2))
        await asyncio.wait_for(backend.started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    old_compaction_inputs(history)
    backend.release.set()
    result = await replay.run(
        history, workflow, backend.identities[-1], compaction_version=2
    )
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 5
    assert backend.identities[2] == backend.identities[3]
    assert backend.requests[2].model_dump(exclude={"session"}) == backend.requests[
        3
    ].model_dump(exclude={"session"})


async def test_old_interrupted_generation_is_not_preceded_by_new_compaction(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = Backend(
        [
            answer("a", 2000),
            answer("b", 8500),
            answer("c", 9000),
            answer("Summary", output=20),
        ]
    )
    backend.pause_at = 3

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("worker", model="test", backend=backend, compaction=policy())
        for text in (OLD, "Continue", "Next"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    with monkeypatch.context() as patch:
        patch.setattr(compaction.History, "select", lambda *args, **kwargs: None)
        task = asyncio.create_task(replay.run(history, workflow, compaction_version=2))
        await asyncio.wait_for(backend.started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    backend.release.set()
    result = await replay.run(
        history, workflow, backend.identities[-1], compaction_version=2
    )
    assert result.outcome.kind == "completed", result.outcome
    assert len(backend.requests) == 5
    assert backend.identities[2] == backend.identities[3]
    assert backend.requests[2].model_dump(exclude={"session"}) == backend.requests[
        3
    ].model_dump(exclude={"session"})


@pytest.mark.parametrize(
    "metadata", [[], {}, {"selection": {}}, {"anchor_id": "missing"}]
)
def test_invalid_recorded_compaction_metadata_is_not_reselected(
    metadata: pydantic.JsonValue,
) -> None:
    operation = models.StepOperation(
        kind="llm.compact",
        name="_compact",
        input={"instance": "worker", "compaction": metadata},
    )
    with pytest.raises(ReplayMismatchError, match="compaction"):
        compaction.recorded_compaction(operation, "worker")


async def test_old_unmarked_compaction_without_metadata_keeps_its_original_cut(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = Backend(
        [answer("a", 300), answer("b", 750), answer("Summary", output=50)]
    )
    select = compaction.History.select
    legacy_calls: list[bool] = []

    def old_select(
        self: compaction.History,
        messages: Sequence[connect.Message],
        message_id: str,
        selected: llm.Compaction,
        signature: str = "",
        *,
        force: bool = False,
        modern: bool = False,
        allow_images: bool = False,
        legacy: bool = False,
    ) -> Optional[compaction.Selection]:
        legacy_calls.append(legacy)
        return select(
            self,
            messages,
            message_id,
            selected,
            signature,
            force=force,
            modern=modern,
            allow_images=allow_images,
            legacy=True,
        )

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "worker",
            model="test",
            backend=backend,
            compaction=llm.Compaction(context_window=1000, keep_recent_tokens=200),
        )
        for text in ("old", "new"):
            agent.add_user_message(text)
            await agent.generate()
        return [message.model_dump(mode="json") for message in agent.history]

    monkeypatch.setattr(compaction.History, "select", old_select)
    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    old_compaction_inputs(history, metadata=False)
    legacy_calls.clear()
    before = dict(history.steps)
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert legacy_calls == [True]
    assert history.steps == before
    assert len(backend.requests) == 3
