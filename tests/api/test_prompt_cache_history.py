"""Exact prompt-prefix checks, including opt-in read-only production-history replay."""

import asyncio
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import sqlite3
from typing import Optional, Union

import connect
from connect.providers.chatgpt import ChatGPTProvider
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition, llm
from vectorops.runner.api.llm import invocation
from vectorops.runner.runner import context, inbox, project
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.workflows.agent import agent


def encoded(value: pydantic.JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


@dataclass
class Prompt:
    label: str
    settings: str
    items: list[str]
    recorded_usage: dict[str, pydantic.JsonValue] = field(default_factory=dict)

    @classmethod
    def from_request(
        cls, label: str, settings: llm.Settings, request: connect.GenerateRequest
    ) -> "Prompt":
        model = connect.default_model_registry.resolve(settings.model)
        assert model.provider == "chatgpt", "This audit targets ChatGPT serialization"
        payload = ChatGPTProvider().build_payload(
            model, request, connect.RequestOptions()
        )
        value = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_python(
            payload
        )
        items = value.pop("input")
        assert isinstance(items, list)
        return cls(label, encoded(value), [encoded(item) for item in items])


def assert_prefix(previous: Prompt, current: Prompt) -> None:
    """Compare serialized items without normalizing order or exposing prompt contents."""
    transition = f"{previous.label} -> {current.label}"
    if previous.settings != current.settings:
        pytest.fail(f"{transition}: prompt settings changed")
    assert len(current.items) >= len(previous.items), f"{transition}: input truncated"
    for index, old in enumerate(previous.items):
        new = current.items[index]
        if old != new:
            offset = next(
                (i for i, (a, b) in enumerate(zip(old, new)) if a != b),
                min(len(old), len(new)),
            )
            pytest.fail(
                f"{transition}: input[{index}] changed at JSON character {offset}"
            )


def capture(monkeypatch: pytest.MonkeyPatch) -> list[Prompt]:
    prompts: list[Prompt] = []
    original = invocation.invoke

    async def invoke(
        call: composition.ActivityCall,
        input: Union[
            dict[str, pydantic.JsonValue], Callable[[], dict[str, pydantic.JsonValue]]
        ],
        backend: llm.Provider,
        settings: llm.Settings,
        request: Union[connect.GenerateRequest, Callable[[], connect.GenerateRequest]],
        policy: llm.RetryPolicy,
        finalize: Optional[invocation.Finalizer] = None,
    ) -> pydantic.JsonValue:
        prepared = request() if callable(request) else request
        metadata = input() if callable(input) else input
        prompts.append(Prompt.from_request(str(len(prompts) + 1), settings, prepared))
        return await original(
            call, metadata, backend, settings, prepared, policy, finalize
        )

    monkeypatch.setattr(invocation, "invoke", invoke)
    return prompts


@pytest.mark.parametrize("change", ["text", "order", "truncate", "tools", "reasoning"])
def test_prefix_check_detects_changes(change: str) -> None:
    previous = Prompt("round-1", '{"tools":[],"reasoning":null}', ['"a"', '"b"'])
    current = Prompt("round-2", previous.settings, [*previous.items, '"c"'])
    assert_prefix(previous, current)
    if change == "text":
        current.items[0] = '"changed"'
    elif change == "order":
        current.items[:2] = reversed(current.items[:2])
    elif change == "truncate":
        current.items = current.items[:1]
    else:
        current.settings = encoded({change: "changed"})
    with pytest.raises(
        (AssertionError, pytest.fail.Exception), match="round-1 -> round-2"
    ):
        assert_prefix(previous, current)


async def test_replayed_tool_rounds_preserve_json_prefix(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Arguments(pydantic.BaseModel):
        value: int

    finished = asyncio.Event()
    execution_order: list[int] = []

    async def execute(
        ctx: ActivityExecutionContext, arguments: Arguments
    ) -> llm.ToolResponse:
        if arguments.value == 1:
            await asyncio.wait_for(finished.wait(), 2)
        execution_order.append(arguments.value)
        finished.set()
        return llm.ToolResponse(content=[connect.TextBlock(text=str(arguments.value))])

    response = connect.AssistantMessage(
        content=[
            connect.ToolCallBlock(id=str(i), name="echo", arguments={"value": i})
            for i in (1, 2)
        ],
        finish_reason="tool_call",
    )
    backend = FakeProvider([response, answer("done"), answer("continued")])
    prompts = capture(monkeypatch)

    async def workflow() -> pydantic.JsonValue:
        conversation = llm.LLM(
            "chat", model="chatgpt/gpt-6.1-sol", system_prompt="Stable", backend=backend
        )
        conversation.add_user_message("Start")
        tools = [llm.Tool("echo", "Echo", Arguments, execute)]
        await conversation.run(tools=tools)
        conversation.add_user_message("Continue")
        await conversation.run(tools=tools)
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed", first.outcome
    assert execution_order == [2, 1]
    assert len(prompts) == 3
    for previous, current in zip(prompts, prompts[1:]):
        assert_prefix(previous, current)
    results = [
        json.loads(item)["call_id"]
        for item in prompts[1].items
        if json.loads(item).get("type") == "function_call_output"
    ]
    assert results == ["1", "2"]
    replayed = await replay.run(history, workflow, first.id)
    assert replayed == first
    assert len(prompts) == 6
    for original, rebuilt in zip(prompts[:3], prompts[3:]):
        old_settings, new_settings = json.loads(original.settings), json.loads(
            rebuilt.settings
        )
        assert old_settings.pop("prompt_cache_key") != new_settings.pop(
            "prompt_cache_key"
        )
        assert old_settings == new_settings
        assert original.items == rebuilt.items
    assert execution_order == [2, 1]
    assert len(backend.requests) == 3


class HistoryExhausted(BaseException):
    pass


class RecordedActivities:
    """Return saved results only; never invoke activity callbacks or child workflows."""

    def __init__(
        self, steps: Sequence[models.WorkflowStep], prompts: list[Prompt]
    ) -> None:
        self.steps = iter(steps)
        self.prompts = prompts
        self.generations = 0

    async def submit(
        self, request: context.OperationRequest
    ) -> context.OperationResponse:
        assert isinstance(
            request, (context.ActivityRequest, context.StreamingActivityRequest)
        )
        step = next(self.steps, None)
        if step is None or step.outcome.kind != "completed":
            raise HistoryExhausted
        assert isinstance(step.operation, models.StepOperation)
        assert request.operation.matches_replay(step.operation), step.id
        if step.operation.kind == "llm.generate":
            self.generations += 1
            assert (
                request.operation.input == step.operation.input
            ), f"{step.id}: reconstructed request digest differs from saved history"
            self.prompts[-1].label = step.id
            metrics = (step.metrics or {}).get("llm")
            if isinstance(metrics, dict) and isinstance(
                metrics.get("provider_usage"), dict
            ):
                self.prompts[-1].recorded_usage = pydantic.TypeAdapter(
                    dict[str, pydantic.JsonValue]
                ).validate_python(metrics["provider_usage"])
            if len(self.prompts) > 1:
                assert_prefix(self.prompts[-2], self.prompts[-1])
        return context.ActivityResponse(step.outcome)


async def test_saved_agent_history_json_prefix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Set PROMPT_CACHE_HISTORY to a history.sqlite3; private data stays outside the repo."""
    supplied = os.environ.get("PROMPT_CACHE_HISTORY")
    if supplied is None:
        pytest.skip("Set PROMPT_CACHE_HISTORY to audit a saved builtin:agent history")
    await reconstruct_history(Path(supplied), monkeypatch, tmp_path)


async def reconstruct_history(
    path: Path, monkeypatch: pytest.MonkeyPatch, workspace: Path
) -> list[Prompt]:
    """Rebuild and verify saved root-agent requests without executing recorded activities."""
    path = path.expanduser().resolve()
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as connection:
        connection.execute("BEGIN")
        root_id = connection.execute(
            "SELECT json_extract(payload, '$.root_workflow_id') FROM execution"
        ).fetchone()[0]
        workflow = models.Workflow.model_validate_json(
            connection.execute(
                "SELECT payload FROM workflows WHERE id = ?", (root_id,)
            ).fetchone()[0]
        )
        records = {
            step.id: step
            for (payload,) in connection.execute(
                "SELECT payload FROM steps WHERE workflow_id = ?", (root_id,)
            )
            for step in [models.WorkflowStep.model_validate_json(payload)]
        }
        head = connection.execute(
            "SELECT step_id FROM workflow_heads WHERE workflow_id = ?", (root_id,)
        ).fetchone()[0]
    assert workflow.definition_ref == "builtin:agent"
    selected: list[models.WorkflowStep] = []
    while head is not None:
        step = records[head]
        selected.append(step)
        head = step.previous_step_id
    selected.reverse()
    assert not any(
        step.operation.kind == "llm.compact" for step in selected
    ), "Compaction intentionally changes prefixes; audit uncompacted history separately"
    kinds = {
        "configuration.validated",
        "builtin.context",
        "configuration.get",
        "workflow.describe",
        "mailbox.input",
        "mailbox.drain",
        "llm.generate",
        "llm.tool",
    }
    steps = [step for step in selected if step.operation.kind in kinds]
    prompts = capture(monkeypatch)
    submitter = RecordedActivities(steps, prompts)
    ctx = context.WorkflowExecutionContext(
        project.RunnerProject(workspace, project.RunnerConfig()),
        workflow,
        submitter,
        inboxes=inbox.Registry("audit", workflow.id),
    )
    token = composition._CURRENT_CONTEXT.set(ctx)
    try:
        with pytest.raises(HistoryExhausted):
            await asyncio.wait_for(agent.func(ctx), 30)
    finally:
        composition._CURRENT_CONTEXT.reset(token)
    expected = sum(
        step.operation.kind == "llm.generate" and step.outcome.kind == "completed"
        for step in steps
    )
    assert submitter.generations == expected >= 2
    assert len(prompts) == expected
    for prompt in prompts:
        settings = json.loads(prompt.settings)
        settings.pop("prompt_cache_key", None)
        prompt.settings = encoded(settings)
    return prompts
