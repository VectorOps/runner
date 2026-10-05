import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing
from pathlib import Path

import aiohttp
import connect
import pydantic
import pytest

from tests.api.test_llm import answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import diagnostics, models
from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm import retry
from vectorops.runner.api.llm.models import LLMResponseError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.streaming import StreamItem


class FlakyProvider:
    def __init__(self, errors: list[Exception]) -> None:
        self.errors = errors
        self.identities: list[ActivityExecutionContext] = []
        self.closed = 0
        self.fail_after_final = False
        self.empty = False
        self.response = answer("success")

    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        self.identities.append(context)
        assert request.messages[0] == connect.UserMessage(content="hello")
        request.messages.clear()
        try:
            if self.errors:
                yield c.delta({"content": [{"type": "text", "text": "discard"}]})
                if self.fail_after_final:
                    yield c.final(answer("discarded final").model_dump(mode="json"))
                raise self.errors.pop(0)
            if not self.empty:
                yield c.final(self.response.model_dump(mode="json"))
        finally:
            self.closed += 1


def request() -> connect.GenerateRequest:
    return connect.GenerateRequest(messages=[connect.UserMessage(content="hello")])


@pytest.mark.parametrize(
    "error,expected",
    [
        (TimeoutError("timeout"), True),
        (ConnectionResetError("reset"), True),
        (aiohttp.ServerDisconnectedError(), True),
        (aiohttp.ClientPayloadError("truncated"), True),
        (
            connect.RateLimitError(
                connect.ErrorInfo(code="rate_limit", message="busy", status_code=429)
            ),
            True,
        ),
        (
            connect.TransientProviderError(
                connect.ErrorInfo(code="server", message="busy", status_code=503)
            ),
            True,
        ),
        (
            connect.ConnectError(
                connect.ErrorInfo(code="busy", message="busy", retryable=True)
            ),
            True,
        ),
        (
            connect.ConnectError(
                connect.ErrorInfo(code="server", message="busy", status_code=500)
            ),
            True,
        ),
        (
            connect.AuthenticationError(
                connect.ErrorInfo(code="auth", message="no", retryable=True)
            ),
            False,
        ),
        (
            connect.ContextLengthError(
                connect.ErrorInfo(code="context", message="large", retryable=True)
            ),
            False,
        ),
        (
            connect.PermanentProviderError(
                connect.ErrorInfo(code="bad", message="bad", status_code=400)
            ),
            False,
        ),
        (ValueError("bad request"), False),
        (LLMResponseError("length"), False),
    ],
)
def test_transient_classification(error: Exception, expected: bool) -> None:
    assert retry.is_transient(error) is expected


@pytest.mark.parametrize("after_final", [False, True])
async def test_retry_resets_partial_stream_and_commits_one_final(
    after_final: bool,
) -> None:
    backend = FlakyProvider([TimeoutError("once")])
    backend.fail_after_final = after_final
    original = request()
    context = ActivityExecutionContext("exec", "workflow", "step")
    items = [
        item
        async for item in retry.stream(
            backend,
            context,
            llm.Settings(model="test"),
            original,
            llm.RetryPolicy(initial_delay=0),
        )
    ]
    assert items == [
        c.delta({"content": [{"type": "text", "text": "discard"}]}),
        c.delta({"content": []}),
        c.final(answer("success").model_dump(mode="json")),
    ]
    assert backend.identities == [context, context]
    assert backend.closed == 2
    assert original == request()


async def test_backoff_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(retry.asyncio, "sleep", sleep)
    backend = FlakyProvider([TimeoutError() for _ in range(3)])
    async for _ in retry.stream(
        backend,
        ActivityExecutionContext(None, "w", "s"),
        llm.Settings(model="test"),
        request(),
        llm.RetryPolicy(initial_delay=1, max_delay=2),
    ):
        pass
    assert delays == [1, 2, 2]


async def test_retry_exhaustion_is_recorded_and_replay_never_retries(
    history: MemoryHistory, tmp_path: Path
) -> None:
    backend = FlakyProvider([TimeoutError("failure") for _ in range(5)])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            retry=llm.RetryPolicy(max_retries=2, initial_delay=0),
        )
        agent.add_user_message("hello")
        with pytest.raises(ActivityError, match="failure"):
            await agent.generate()
        assert len(agent.history) == 1
        return None

    async with diagnostics.DiagnosticLog(tmp_path) as logs:
        with logs.activate():
            first = await replay.run(history, workflow)
            assert first.outcome.kind == "completed"
            assert (
                await replay.run(history, workflow, first.id)
            ).outcome == first.outcome
    assert len(backend.identities) == 3
    entries = [
        diagnostics.LogRecord.model_validate_json(line)
        for path in (tmp_path / "logs").glob("*.jsonl")
        for line in path.read_bytes().splitlines()
    ]
    assert [
        entry.event for entry in entries if entry.event.startswith("llm.request")
    ] == ["llm.request_retry", "llm.request_retry", "llm.request_failed"]
    assert all(
        entry.step_id == backend.identities[0].step_id
        for entry in entries
        if entry.event.startswith("llm.request")
    )


async def test_successful_retry_is_one_replayable_activity(
    history: MemoryHistory,
) -> None:
    backend = FlakyProvider([TimeoutError("once")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            retry=llm.RetryPolicy(initial_delay=0),
        )
        agent.add_user_message("hello")
        await agent.generate()
        assert agent.usage.total_tokens == 12
        assert len(agent.history) == 2
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert len(backend.identities) == 2
    assert (
        len(
            [
                step
                for step in history.path(first.id)
                if step.operation.kind == "llm.generate"
            ]
        )
        == 1
    )


async def test_cancel_during_backoff_propagates_and_closes_attempt() -> None:
    backend = FlakyProvider([TimeoutError()])
    async with aclosing(
        retry.stream(
            backend,
            ActivityExecutionContext(None, "w", "s"),
            llm.Settings(model="test"),
            request(),
            llm.RetryPolicy(initial_delay=30),
        )
    ) as stream:
        await anext(stream)
        assert await anext(stream) == c.delta({"content": []})
        task = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(backend.identities) == backend.closed == 1


@pytest.mark.parametrize("empty", [False, True])
async def test_terminal_validation_does_not_retry(empty: bool) -> None:
    backend = FlakyProvider([])
    backend.empty = empty
    backend.response.finish_reason = "length"
    with pytest.raises(LLMResponseError):
        async for _ in retry.stream(
            backend,
            ActivityExecutionContext(None, "w", "s"),
            llm.Settings(model="test"),
            request(),
            llm.RetryPolicy(initial_delay=0),
        ):
            pass
    assert len(backend.identities) == 1


async def test_workspace_retry_policy_is_recorded(
    history: MemoryHistory, tmp_path: Path
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "test"\n[components.llm.retry]\nmax_retries = 1\ninitial_delay = 0\n'
    )
    backend = FlakyProvider([TimeoutError("once")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM("chat", backend=backend)
        agent.add_user_message("hello")
        await agent.generate()
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text(
        '[components.llm]\nmodel = "changed"\n[components.llm.retry]\nmax_retries = 0\n'
    )
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert len(backend.identities) == 2


async def test_compaction_retries_inside_its_activity(history: MemoryHistory) -> None:
    backend = FlakyProvider([TimeoutError("once")])

    class SummaryProvider(FlakyProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            request = request.model_copy(
                update={"messages": [connect.UserMessage(content="hello")]}
            )
            async for item in backend.stream(context, settings, request):
                yield item

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=SummaryProvider([]),
            retry=llm.RetryPolicy(initial_delay=0),
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("old")
        agent.add_message(answer("old answer"))
        agent.add_user_message("new")
        await agent.generate()
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert len(backend.identities) == 3
    assert backend.identities[0] == backend.identities[1]
    assert backend.identities[1] != backend.identities[2]
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert len(backend.identities) == 3


@pytest.mark.parametrize("after_provider", [False, True])
async def test_storage_failure_is_not_retried(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch, after_provider: bool
) -> None:
    backend = FlakyProvider([TimeoutError("provider")])
    original = history.update_step

    async def update(step: models.WorkflowStep) -> None:
        if step.operation.kind == "llm.generate" and (
            not after_provider or backend.identities
        ):
            raise ConnectionError("storage unavailable")
        await original(step)

    monkeypatch.setattr(history, "update_step", update)

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test",
            backend=backend,
            retry=llm.RetryPolicy(initial_delay=0),
        )
        agent.add_user_message("hello")
        await agent.generate()
        return None

    with pytest.raises(ConnectionError, match="storage unavailable"):
        await replay.run(history, workflow)
    assert len(backend.identities) == int(after_provider)


@pytest.mark.parametrize(
    "values",
    [
        {"max_retries": -1},
        {"max_retries": True},
        {"initial_delay": -1},
        {"initial_delay": float("nan")},
        {"max_delay": 0},
        {"max_retries": 21},
    ],
)
def test_invalid_retry_settings(values: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        llm.RetryPolicy.model_validate(values)
