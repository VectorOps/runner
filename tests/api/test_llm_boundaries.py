import json
from pathlib import Path
import subprocess
import sys

import connect
import pydantic
import pytest

from tests.fixtures import replay, sources
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import chat_input, composition, plan
from vectorops.runner.api import tool_contracts, user_input
from vectorops.runner.api.builtin_tools.patch import apply_patch_tool
from vectorops.runner.api.llm import compaction, models, snapshots
from vectorops.runner.api.llm import input as legacy_input
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.mark.parametrize(
    "target, expected",
    [
        ("llm", []),
        ("environment", []),
        ("read_file", ["read_file"]),
        ("subagent_tool", ["run_agent", "subworkflow"]),
    ],
)
def test_import_registration_boundaries(target: str, expected: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, str(sources.path("import_registration.py")), target],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(result.stdout) == expected


def test_explicit_bootstrap_is_idempotent() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(sources.path("bootstrap.py")),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


async def test_patch_binding_rejects_environment_reuse(tmp_path: Path) -> None:
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(first_root))
    )
    second = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(second_root))
    )
    binding = apply_patch_tool()
    arguments: dict[str, pydantic.JsonValue] = {
        "text": "*** Begin Patch\n*** Add File: result.txt\n+first\n*** End Patch\n"
    }
    try:
        response = await binding.execute(
            ActivityExecutionContext(None, "workflow", "first", environment=first),
            arguments,
        )
        assert not response.is_error
        with pytest.raises(
            tool_contracts.ToolExecutionError, match="target environments"
        ):
            await binding.execute(
                ActivityExecutionContext(
                    None, "workflow", "second", environment=second
                ),
                arguments,
            )
        assert (first_root / "result.txt").read_text() == "first\n"
        assert not (second_root / "result.txt").exists()
    finally:
        await first.close()
        await second.close()


def test_compaction_request_and_replacement_contract() -> None:
    settings = models.Settings(model="test", temperature=0.2, max_output_tokens=100)
    policy = models.Compaction(instruction="Custom instruction")
    messages: list[connect.Message] = [
        connect.UserMessage(content="old"),
        connect.UserMessage(content="retained"),
    ]
    before = connect.GenerateRequest(messages=messages)
    history = compaction.History()
    selection = compaction.Selection(anchor_id="anchor", cut=1, prefix_tokens=200)
    summary = history.prepare(messages, selection, policy, settings, before)
    expected = connect.GenerateRequest(
        system_prompt="Custom instruction\nUpdate the previous summary with the transcript. Preserve task intent, "
        "constraints, decisions, and unfinished work; remove superseded details. "
        "Use sections: Goal, Constraints, Progress, Decisions, Next steps, Critical context.",
        messages=[
            connect.UserMessage(
                content=json.dumps(
                    {
                        "previous_summary": None,
                        "transcript": [messages[0].model_dump(mode="json")],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        ],
        temperature=0.2,
        max_output_tokens=4096,
    )
    assert summary.request.model_dump(mode="json") == expected.model_dump(mode="json")
    replaced = compaction.replacement(messages, 1, "summary")
    assert replaced[0] == connect.UserMessage(
        content="Summary of earlier conversation:\nsummary"
    )
    assert replaced[1] == messages[1]
    history.commit(selection, 10)
    repeated = history.prepare(replaced, selection, policy, settings, before)
    expected.messages = [
        connect.UserMessage(
            content=json.dumps(
                {
                    "previous_summary": replaced[0].model_dump(mode="json"),
                    "transcript": [],
                },
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    ]
    assert repeated.request == expected
    legacy = compaction.legacy_summary(
        messages, 1, policy.instruction, settings, before
    )
    assert legacy.request.system_prompt == policy.instruction
    assert legacy.request.max_output_tokens == 100


def test_configuration_projection_ignores_additive_policy_fields() -> None:
    class ExtendedConfiguration(models.Configuration):
        extra_policy: bool = True

    configuration = ExtendedConfiguration(model="test", max_rounds=2)
    assert configuration.generation_settings() == models.Settings(model="test")


def test_chat_adapter_preserves_legacy_specification() -> None:
    request = chat_input.UserMessageRequest(prompt="Question")
    legacy = legacy_input.UserMessageRequest(prompt="Question")
    assert user_input.input_spec(request) == legacy.input_spec()
    assert "input_spec" not in chat_input.UserMessageRequest.__dict__
    assert "execute" not in tool_contracts.WorkflowToolBinding.__dict__


async def test_plan_owner_replays_without_tool_binding(history: MemoryHistory) -> None:
    async def run() -> pydantic.JsonValue:
        state = plan.Plan()
        context = composition.workflow_context()
        await state.update(
            context,
            plan.PlanArguments(
                todos=[plan.Todo(id="task", title="Work", status="in_progress")]
            ),
        )
        before = state.todos
        with pytest.raises(plan.PlanError, match="Only one"):
            await state.update(
                context,
                plan.PlanArguments(
                    todos=[plan.Todo(id="other", title="Other", status="in_progress")]
                ),
            )
        assert state.todos == before
        updated = await state.update(
            context,
            plan.PlanArguments(todos=[plan.Todo(id="task", status="completed")]),
        )
        return [todo.model_dump(mode="json") for todo in updated]

    first = await replay.run(history, run)
    second = await replay.run(history, run, first.id)
    assert first.outcome == second.outcome
    assert first.outcome.kind == "completed"


def test_snapshot_progress_and_estimation_share_output_contract() -> None:
    progress = snapshots.Progress()
    assert not progress.update({"model": "test", "content": []})
    value: pydantic.JsonValue = {
        "content": [
            {
                "type": "tool_call",
                "id": "call",
                "name": "tool",
                "arguments_text": "{",
            }
        ]
    }
    assert progress.update(value)
    assert not progress.update(value)
    assert isinstance(value, dict)
    assert snapshots.content(value["content"]) == value["content"]
    assert snapshots.content([{"type": "image", "data": "AAAA"}]) is None
