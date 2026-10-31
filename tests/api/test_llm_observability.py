import asyncio
from collections.abc import AsyncGenerator
from decimal import Decimal
import json
from pathlib import Path

import connect
import pydantic
import pytest

from tests.api.test_llm import answer, FakeProvider
from tests.api.test_llm_retry import FlakyProvider, request
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm import observability as obs
from vectorops.runner.runner import events, execution_state, history_query
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.streaming import StreamItem


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> connect.ModelSpec:
    model = connect.ModelSpec(
        provider="test",
        model="small",
        api_family="test",
        context_window=1000,
        max_output_tokens=200,
        pricing=connect.ModelPricing(
            input_per_million=Decimal("2"),
            output_per_million=Decimal("8"),
            cache_read_per_million=Decimal("0.5"),
            cache_write_per_million=Decimal("3"),
        ),
    )
    monkeypatch.setattr(
        connect, "default_model_registry", connect.ModelRegistry([model])
    )
    return model


def metrics(step: models.WorkflowStep) -> obs.Metrics:
    assert step.metrics is not None
    return obs.Metrics.model_validate(step.metrics["llm"])


def test_budget_cost_and_usage_are_separate(catalog: connect.ModelSpec) -> None:
    generation = request().model_copy(update={"max_output_tokens": 100})
    observation = obs.Observation(llm.Settings(model="test/small"), generation)
    response = answer("test")
    response.usage = connect.Usage(
        input_tokens=100,
        output_tokens=20,
        reasoning_tokens=10,
        cache_read_tokens=40,
        cache_write_tokens=30,
        completeness="final",
    )
    result = obs.Metrics.model_validate(
        observation.snapshot(response.model_dump(mode="json"), 2)["llm"]
    )
    assert result.catalog_model == "small"
    assert result.catalog_provider == "test"
    assert result.input_budget == 900
    assert result.output_budget == 100
    assert result.context_window == 1000
    assert result.input_tokens_estimate is not None
    assert result.context_remaining_estimate == 1000 - result.input_tokens_estimate
    assert result.remaining_output_headroom_estimate == 80
    assert result.cost_estimate_usd == Decimal("0.00047")
    assert result.cost_basis == "provider_usage"
    assert result.provider_usage == response.usage
    assert result.attempt == 2 and result.retries == 1
    assert result.context_overflow_estimate == 0


def test_estimates_include_system_and_tools_not_metadata(
    catalog: connect.ModelSpec,
) -> None:
    original = request()
    initial = obs.input_tokens(original)
    original.system_prompt = "system prompt " * 40
    original.tools = [
        connect.ToolSpec(
            name="test",
            description="description " * 20,
            input_schema={
                "type": "object",
                "properties": {"message": {"type": "string"}},
            },
        )
    ]
    larger = obs.input_tokens(original)
    assert initial is not None and larger is not None and larger > initial
    original.messages[0].provider_meta["credential"] = "secret" * 10000
    assert obs.input_tokens(original) == larger
    observation = obs.Observation(
        llm.Settings(model="test/small", credential_profile="secret"),
        original,
    )
    value: dict[str, pydantic.JsonValue] = {
        "content": [{"type": "text", "text": "private response"}],
        "usage": {
            "input_tokens": 20,
            "completeness": "partial",
            "provider_meta": {"api_key": "secret"},
        },
        "provider_meta": {"authorization": "secret"},
    }
    snapshot = observation.snapshot(value, 1)
    serialized = json.dumps(snapshot)
    for secret in (
        "secret",
        "private response",
        "system prompt",
        "description",
        "credential",
        "api_key",
        "authorization",
    ):
        assert secret not in serialized
    parsed = obs.Metrics.model_validate(snapshot["llm"])
    assert parsed.provider_usage is not None
    assert parsed.provider_usage.completeness == "partial"
    assert parsed.cost_basis == "provider_usage"


def test_unknown_models_missing_rates_and_usage(
    catalog: connect.ModelSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    value: dict[str, pydantic.JsonValue] = {
        "content": [{"type": "text", "text": "hello"}]
    }
    known = obs.Observation(llm.Settings(model="small"), request())
    estimate = obs.Metrics.model_validate(known.snapshot(value, 1)["llm"])
    assert estimate.provider_usage is None
    assert estimate.cost_basis == "token_estimate"
    assert estimate.cost_estimate_usd is not None
    assert estimate.output_budget == 200
    unknown = obs.Observation(llm.Settings(model="missing"), request())
    result = obs.Metrics.model_validate(unknown.snapshot(value, 1)["llm"])
    assert result.context_window is None and result.cost_estimate_usd is None
    assert result.remaining_output_headroom_estimate is None
    known.model = catalog.model_copy(
        update={"pricing": connect.ModelPricing(input_per_million=Decimal("2"))}
    )
    assert (
        obs.Metrics.model_validate(known.snapshot(value, 1)["llm"]).cost_estimate_usd
        is None
    )
    monkeypatch.setattr(
        connect,
        "default_model_registry",
        connect.ModelRegistry(
            [catalog, catalog.model_copy(update={"provider": "other"})]
        ),
    )
    assert (
        obs.Observation(llm.Settings(model="small"), request()).base.context_window
        is None
    )


def test_overflow_and_multimodal_are_not_false_precision(
    catalog: connect.ModelSpec,
) -> None:
    large = connect.GenerateRequest(messages=[connect.UserMessage(content="x" * 8000)])
    result = obs.Metrics.model_validate(
        obs.Observation(llm.Settings(model="small"), large).snapshot(
            {"content": []}, 1
        )["llm"]
    )
    assert result.remaining_output_headroom_estimate == 0
    assert (
        result.context_overflow_estimate is not None
        and result.context_overflow_estimate > 0
    )
    image = connect.GenerateRequest(
        messages=[
            connect.UserMessage(
                content=[connect.ImageBlock(mime_type="image/png", data="YQ==")]
            )
        ]
    )
    result = obs.Metrics.model_validate(
        obs.Observation(llm.Settings(model="small"), image).snapshot(
            {"content": []}, 1
        )["llm"]
    )
    assert result.input_tokens_estimate is None
    assert result.context_remaining_estimate is None
    assert result.cost_estimate_usd is None


def test_multimodal_context_uses_measured_usage_without_changing_metrics(
    catalog: connect.ModelSpec,
) -> None:
    image = connect.GenerateRequest(
        messages=[
            connect.UserMessage(
                content=[connect.ImageBlock(mime_type="image/png", data="YQ==")]
            )
        ]
    )
    response = answer("done")
    response.usage = connect.Usage(
        input_tokens=100,
        cache_read_tokens=500,
        cache_write_tokens=100,
        output_tokens=20,
        completeness="final",
    )
    metrics = obs.Metrics.model_validate(
        obs.Observation(llm.Settings(model="small"), image).snapshot(
            response.model_dump(mode="json"), 1
        )["llm"]
    )
    before = metrics.model_dump()
    context = metrics.context("image-call")
    assert context is not None
    assert context.input_tokens_estimate == 700
    assert context.output_tokens_estimate == 20
    assert context.context_window == 1000
    assert context.input_tokens_source == "provider_usage"
    assert context.context_remaining_estimate == 300
    assert context.remaining_output_headroom_estimate == 180
    assert metrics.input_tokens_estimate is None
    assert metrics.model_dump() == before


@pytest.mark.parametrize("fail", [False, True])
async def test_retry_events_persistence_and_replay(
    history: MemoryHistory, catalog: connect.ModelSpec, tmp_path: Path, fail: bool
) -> None:
    backend = FlakyProvider([TimeoutError("retry")] * (3 if fail else 1))
    received: list[events.StepEvent] = []

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="test/small",
            backend=backend,
            retry=llm.RetryPolicy(max_retries=2, initial_delay=0),
        )
        agent.add_user_message("hello")
        return (await agent.generate()).model_dump(mode="json")

    first = await replay.run(history, workflow, event_sink=received.append)
    step = next(
        step for step in history.path(first.id) if step.operation.kind == "llm.generate"
    )
    assert step.outcome.kind == ("failed" if fail else "completed")
    assert metrics(step).attempt == (3 if fail else 2)
    assert isinstance(step.operation, models.StepOperation)
    assert set(step.operation.input) == {"instance", "request_digest"}
    streamed = [
        event.step
        for event in received
        if event.step.id == step.id and event.step.metrics is not None
    ]
    assert streamed[-1].metrics == step.metrics
    assert {metrics(item).attempt for item in streamed} == set(
        range(1, metrics(step).attempt + 1)
    )
    reset = [
        item
        for item in streamed
        if item.outcome.kind == "streaming" and item.outcome.state == {"content": []}
    ]
    assert all(metrics(item).output_tokens_estimate == 0 for item in reset)
    assert all(metrics(item).provider_usage is None for item in reset)
    connect.default_model_registry.register(
        catalog.model_copy(update={"context_window": 9999, "pricing": None})
    )
    original = step.metrics
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert history.steps[step.id].metrics == original
    assert len(backend.identities) == (3 if fail else 2)

    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(history.workflows["test"])
    await database.save_execution(
        execution_state.ExecutionRecord(id="exec", root_workflow_id="test")
    )
    for item in history.path(first.id):
        if item.id == step.id:
            await database.append_step(
                item.model_copy(
                    update={"outcome": models.StepStreaming(), "metrics": None}
                )
            )
            await database.update_step(item)
        else:
            await database.append_step(item)
    reopened = SQLiteHistory(tmp_path / "history.sqlite3")
    page = await reopened.query(
        history_query.HistoryQuery(execution_id="exec", step_ids=[step.id])
    )
    assert page.items[0].step.metrics == original
    output = tmp_path / "export.jsonl"
    await reopened.export_execution(output)
    exported = [json.loads(line) for line in output.read_text().splitlines()]
    assert (
        next(item for item in exported if item.get("id") == step.id)["metrics"]
        == original
    )


@pytest.mark.parametrize("summary", ["short", "long " * 2000])
async def test_compaction_savings_and_legacy_replay(
    history: MemoryHistory, catalog: connect.ModelSpec, summary: str
) -> None:
    backend = FakeProvider([answer(summary), answer("done")])

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="small",
            backend=backend,
            system_prompt="instructions",
            compaction=llm.Compaction(max_context_characters=1, keep_recent_messages=1),
        )
        agent.add_user_message("old " * 200)
        agent.add_user_message("recent")
        return (await agent.generate()).model_dump(mode="json")

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    compact = next(
        step for step in history.path(first.id) if step.operation.kind == "llm.compact"
    )
    generation = next(
        step for step in history.path(first.id) if step.operation.kind == "llm.generate"
    )
    savings = metrics(compact).compaction
    assert savings is not None
    assert (
        savings.after_input_tokens_estimate == metrics(generation).input_tokens_estimate
    )
    assert savings.saved_tokens_estimate is not None
    assert (savings.saved_tokens_estimate > 0) == (summary == "short")
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    for identifier, step in history.steps.items():
        payload = step.model_dump(mode="json")
        payload.pop("metrics")
        history.steps[identifier] = models.WorkflowStep.model_validate(payload)
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    assert all(step.metrics is None for step in history.steps.values())


async def test_cancelled_attempt_restarts_metric_budget(history: MemoryHistory) -> None:
    started = asyncio.Event()

    class Backend:
        calls = 0

        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError()
            if self.calls == 2:
                started.set()
                await asyncio.Event().wait()
            yield c.final(answer("done").model_dump(mode="json"))

    backend = Backend()

    async def workflow() -> pydantic.JsonValue:
        agent = llm.LLM(
            "chat",
            model="unknown",
            backend=backend,
            retry=llm.RetryPolicy(initial_delay=0),
        )
        agent.add_user_message("hello")
        return (await agent.generate()).model_dump(mode="json")

    task = asyncio.create_task(replay.run(history, workflow))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    step = next(
        step for step in history.steps.values() if step.operation.kind == "llm.generate"
    )
    assert metrics(step).attempt == 2
    assert (
        await replay.run(history, workflow, history.heads["test"])
    ).outcome.kind == "completed"
    assert metrics(history.steps[step.id]).attempt == 1
    assert backend.calls == 3
