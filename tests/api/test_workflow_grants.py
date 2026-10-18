import asyncio
from pathlib import Path
from typing import Optional

import connect
import pydantic
import pytest

from tests.api.test_tools import ApprovalStore, approve
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import tool_policy
from vectorops.runner.api import llm
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext


class Arguments(pydantic.BaseModel):
    value: int


async def test_workflow_grant_cannot_bypass_mandatory_checks(
    history: MemoryHistory, tmp_path: Path
) -> None:
    policy = tool_policy.ToolPolicy()
    checks: list[int] = []
    calls: list[int] = []
    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def check(call: tool_policy.ToolCall) -> Optional[str]:
        assert call.workflow_id == "test"
        value = call.arguments["value"]
        assert isinstance(value, int)
        checks.append(value)
        return "blocked" if value == 2 else None

    policy.register_check("required", check)

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        calls.append(args.value)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        results = []
        for value in (1, 2, 3):
            result = await llm.invoke_tool(
                llm.Tool("test", "Test", Arguments, execute),
                connect.ToolCallBlock(
                    id=str(value), name="test", arguments={"value": value}
                ),
            )
            results.append(result)
        assert [result.is_error for result in results] == [False, True, False]
        assert results[1].content == [connect.TextBlock(text="blocked")]
        return None

    task = asyncio.create_task(
        replay.run(
            history,
            workflow,
            mailbox=box,
            workspace=tmp_path,
            tool_policy_version=1,
            policy=policy,
        )
    )
    await asyncio.wait_for(store.registered.wait(), 2)
    await approve(
        box,
        next(iter(store.requests.values())),
        {"decision": "approve", "scope": "workflow"},
    )
    first = await asyncio.wait_for(task, 2)
    assert first.outcome.kind == "completed"
    assert checks == [1, 2, 3] and calls == [1, 3]
    assert len(store.requests) == 1
    assert {step.operation.kind for step in history.path(first.id)} == {
        "llm.tool",
        "workflow.complete",
    }
    assert (
        await replay.run(
            history, workflow, first.id, workspace=tmp_path, tool_policy_version=1
        )
    ).outcome == first.outcome
    assert checks == [1, 2, 3] and calls == [1, 3]


@pytest.mark.parametrize(
    "value",
    [
        {"approved": False, "scope": "workflow"},
        {"approved": True, "scope": "execution"},
        {"approved": True, "scope": "workflow", "tool_name": "other"},
    ],
)
def test_invalid_scope_response(value: dict[str, pydantic.JsonValue]) -> None:
    with pytest.raises(pydantic.ValidationError):
        llm.ToolApprovalResponse.model_validate(value)


def test_scope_defaults_to_call() -> None:
    assert llm.ToolApprovalResponse(decision="approve").scope == "call"
