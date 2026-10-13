import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, patch

import connect
import pydantic
import pytest

from tests.api.test_llm import answer
from tests.api.test_llm_provider import Client
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import diagnostics
from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm import deadlines, retry, snapshots
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.streaming import StreamItem


class StuckProvider:
    def __init__(
        self, *, started: bool = False, metadata: bool = False, recover: bool = False
    ) -> None:
        self.started = started
        self.metadata = metadata
        self.recover = recover
        self.calls = 0
        self.closed = 0
        self.ready = asyncio.Event()

    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        self.calls += 1
        try:
            if self.recover and self.calls > 1:
                yield c.final(answer("recovered").model_dump(mode="json"))
                return
            content: list[pydantic.JsonValue] = (
                [{"type": "text", "text": "partial"}] if self.started else []
            )
            if self.started:
                yield c.delta({"content": content})
            self.ready.set()
            while True:
                if not self.metadata:
                    await asyncio.Event().wait()
                await asyncio.sleep(0.005)
                yield c.delta({"content": content, "usage": {"output_tokens": 99}})
        finally:
            self.closed += 1


def request() -> connect.GenerateRequest:
    return connect.GenerateRequest(messages=[connect.UserMessage(content="hello")])


def stream(
    backend: StuckProvider, *, retries: int = 0
) -> AsyncGenerator[StreamItem, None]:
    return retry.stream(
        backend,
        ActivityExecutionContext(None, "w", "s"),
        llm.Settings(model="test", first_token_timeout=0.05, stream_stall_timeout=0.03),
        request(),
        llm.RetryPolicy(max_retries=retries, initial_delay=0),
    )


@pytest.mark.parametrize("started", [False, True])
@pytest.mark.parametrize("metadata", [False, True])
async def test_hang_times_out_and_closes_stream(started: bool, metadata: bool) -> None:
    backend = StuckProvider(started=started, metadata=metadata)
    async with asyncio.timeout(2):
        with pytest.raises(deadlines.StreamTimeoutError) as error:
            async for _ in stream(backend):
                pass
    assert error.value.phase == ("stream_stall" if started else "first_token")
    assert backend.calls == backend.closed == 1


@pytest.mark.parametrize(
    "kind,field",
    [("text", "text"), ("reasoning", "text"), ("tool_call", "arguments_text")],
)
async def test_healthy_progress_has_no_total_deadline(kind: str, field: str) -> None:
    async def healthy() -> AsyncGenerator[StreamItem, None]:
        for index in range(12):
            await asyncio.sleep(0.01)
            yield c.delta({"content": [{"type": kind, field: "x" * (index + 1)}]})
        yield c.final(answer("done").model_dump(mode="json"))

    settings = llm.Settings(
        model="test", first_token_timeout=0.06, stream_stall_timeout=0.06
    )
    items = [item async for item in deadlines.stream(healthy(), settings)]
    assert items[-1] == c.final(answer("done").model_dump(mode="json"))
    assert len(items) == 13


@pytest.mark.parametrize("new_blocks", [False, True])
async def test_redacted_reasoning_refreshes_deadline(new_blocks: bool) -> None:
    async def healthy() -> AsyncGenerator[StreamItem, None]:
        accumulator = snapshots.Accumulator()
        for index in range(12):
            await asyncio.sleep(0.01)
            event = connect.ReasoningEndEvent(
                index=index if new_blocks else 0,
                text="",
                signature=f"opaque-{index}",
                redacted=True,
            )
            yield c.delta(accumulator.update(event))
        yield c.final(answer("done").model_dump(mode="json"))

    settings = llm.Settings(
        model="test", first_token_timeout=0.06, stream_stall_timeout=0.06
    )
    items = [item async for item in deadlines.stream(healthy(), settings)]
    assert items[-1] == c.final(answer("done").model_dump(mode="json"))
    assert len(items) == 13


@pytest.mark.parametrize("signature", [None, "", "opaque"])
async def test_unchanged_reasoning_does_not_refresh_deadline(
    signature: Optional[str],
) -> None:
    async def unchanged() -> AsyncGenerator[StreamItem, None]:
        while True:
            yield c.delta(
                {
                    "content": [
                        {
                            "type": "reasoning",
                            "text": "",
                            "signature": signature,
                            "redacted": True,
                        }
                    ]
                }
            )
            await asyncio.sleep(0.005)

    settings = llm.Settings(
        model="test", first_token_timeout=0.05, stream_stall_timeout=0.03
    )
    async with asyncio.timeout(2):
        with pytest.raises(deadlines.StreamTimeoutError) as error:
            async for _ in deadlines.stream(unchanged(), settings):
                pass
    assert error.value.phase == ("stream_stall" if signature else "first_token")


async def test_metadata_does_not_reset_first_output_budget() -> None:
    async def metadata() -> AsyncGenerator[StreamItem, None]:
        for index in range(100):
            await asyncio.sleep(0.005)
            yield c.delta(
                {"content": [{"type": "text", "text": ""}], "response_id": str(index)}
            )

    with pytest.raises(deadlines.StreamTimeoutError, match="first_token"):
        async for _ in deadlines.stream(
            metadata(), llm.Settings(model="test", first_token_timeout=0.04)
        ):
            pass


async def test_immediate_metadata_cannot_evade_deadline(tmp_path: Path) -> None:
    async def metadata() -> AsyncGenerator[StreamItem, None]:
        while True:
            yield c.delta({"content": []})

    async with diagnostics.DiagnosticLog(tmp_path) as logs:
        with logs.activate():
            with pytest.raises(deadlines.StreamTimeoutError, match="first_token"):
                async for _ in deadlines.stream(
                    metadata(), llm.Settings(model="test", first_token_timeout=0.01)
                ):
                    pass
    records = [
        diagnostics.LogRecord.model_validate_json(line)
        for path in (tmp_path / "logs").glob("*.jsonl")
        for line in path.read_bytes().splitlines()
    ]
    assert any(record.event == "llm.request_timeout" for record in records)


async def test_persistence_time_is_not_counted() -> None:
    async def healthy() -> AsyncGenerator[StreamItem, None]:
        yield c.delta({"content": []})
        yield c.delta({"content": [{"type": "text", "text": "output"}]})
        yield c.final(answer("done").model_dump(mode="json"))

    async for _ in deadlines.stream(
        healthy(),
        llm.Settings(model="test", first_token_timeout=0.02, stream_stall_timeout=0.02),
    ):
        await asyncio.sleep(0.04)


async def test_timeout_retries_with_fresh_budget_and_clears_partial_output() -> None:
    backend = StuckProvider(started=True, recover=True)
    items = [item async for item in stream(backend, retries=1)]
    assert items[1] == c.delta({"content": []})
    assert items[-1] == c.final(answer("recovered").model_dump(mode="json"))
    assert backend.calls == backend.closed == 2


async def test_cancellation_is_not_timeout_or_retry() -> None:
    backend = StuckProvider()
    async with aclosing(stream(backend, retries=3)) as items:
        task = asyncio.create_task(anext(items))
        await backend.ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert backend.calls == backend.closed == 1


async def test_exhausted_deadline_is_recorded_and_not_retried_on_replay(
    history: MemoryHistory,
) -> None:
    backend = StuckProvider()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "stuck",
            model="test",
            backend=backend,
            first_token_timeout=0.02,
            retry=llm.RetryPolicy(max_retries=1, initial_delay=0),
        )
        agent.add_user_message("hello")
        with pytest.raises(ActivityError, match="first_token"):
            await agent.generate()
        assert len(agent.history) == 1
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert backend.calls == backend.closed == 2


async def test_final_only_response_is_success() -> None:
    async def final_only() -> AsyncGenerator[StreamItem, None]:
        yield c.final(answer("done").model_dump(mode="json"))

    assert (
        len(
            [
                item
                async for item in deadlines.stream(
                    final_only(), llm.Settings(model="test")
                )
            ]
        )
        == 1
    )


async def test_wait_after_final_is_bounded() -> None:
    async def stuck_close() -> AsyncGenerator[StreamItem, None]:
        yield c.final(answer("done").model_dump(mode="json"))
        await asyncio.Event().wait()

    with pytest.raises(deadlines.StreamTimeoutError, match="stream_stall"):
        async for _ in deadlines.stream(
            stuck_close(), llm.Settings(model="test", stream_stall_timeout=0.02)
        ):
            pass


async def test_interactive_authentication_is_excluded() -> None:
    async def transport(*args: object) -> connect.TransportAuth:
        await asyncio.sleep(0.06)
        return connect.NoAuth()

    client = Client([connect.ResponseEndEvent(response=answer("done"))])
    backend = llm.ConnectProvider(client_factory=lambda: client)
    with patch.object(
        AuthenticationService, "transport", new=AsyncMock(side_effect=transport)
    ):
        items = [
            item
            async for item in retry.stream(
                backend,
                ActivityExecutionContext(None, "w", "s"),
                llm.Settings(model="test", first_token_timeout=0.02),
                request(),
                llm.RetryPolicy(max_retries=0),
            )
        ]
    assert items[-1] == c.final(answer("done").model_dump(mode="json"))


@pytest.mark.parametrize("method", ["resolve", "refresh"])
async def test_transport_authentication_pauses_deadline(method: str) -> None:
    from vectorops.runner.api.llm import provider

    class Auth:
        async def resolve(
            self, context: Optional[connect.AuthContext] = None
        ) -> connect.ResolvedAuth:
            await asyncio.sleep(0.06)
            return connect.ResolvedAuth()

        async def refresh(self, context: Optional[connect.AuthContext] = None) -> bool:
            await asyncio.sleep(0.06)
            return True

    async def authenticated() -> AsyncGenerator[StreamItem, None]:
        auth = provider._Authentication(Auth())
        if method == "resolve":
            await auth.resolve()
        else:
            await auth.refresh()
        yield c.final(answer("done").model_dump(mode="json"))

    assert (
        len(
            [
                item
                async for item in deadlines.stream(
                    authenticated(),
                    llm.Settings(model="test", first_token_timeout=0.02),
                )
            ]
        )
        == 1
    )


@pytest.mark.parametrize("field", ["first_token_timeout", "stream_stall_timeout"])
@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), None])
def test_invalid_deadline_settings(field: str, value: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        llm.Settings.model_validate({"model": "test", field: value})


async def test_configuration_overrides_and_replay(
    history: MemoryHistory, tmp_path: Path
) -> None:
    settings = llm.Settings(model="test")
    assert (settings.first_token_timeout, settings.stream_stall_timeout) == (180, 120)
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.llm]\nmodel = "test"\nfirst_token_timeout = 240\nstream_stall_timeout = 90\n'
    )

    async def workflow() -> pydantic.JsonValue:
        inherited = await llm.resolve_settings()
        assert (inherited.first_token_timeout, inherited.stream_stall_timeout) == (
            240,
            90,
        )
        overridden = await llm.resolve_settings(
            llm.SettingsOverrides(first_token_timeout=200), stream_stall_timeout=60
        )
        assert (overridden.first_token_timeout, overridden.stream_stall_timeout) == (
            200,
            60,
        )
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text('[components.llm]\nmodel = "changed"\nfirst_token_timeout = 1\n')
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
