import asyncio
from pathlib import Path
from unittest.mock import patch

import connect
import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import tool_policy
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.resolver import WorkflowResolver
from vectorops.runner.runner.workflow import WorkflowRunner

CHILD = """
from vectorops.runner.api.composition import workflow

@workflow(description="Double a number")
async def double(ctx, value: int) -> int:
    if value < 0:
        raise ValueError("negative input")
    return value * 2
"""

PARENT = """
import connect
from vectorops.runner.api import llm

async def main(ctx, reference: str = "child:double", value: int = 3):
    tool = await llm.subworkflow_tool(["child:double"])
    result = await llm.invoke_tool(tool, connect.ToolCallBlock(
        id="call", name="subworkflow", arguments={"reference": reference, "input": {"value": value}}
    ))
    return result.model_dump(mode="json")
"""


def prepare(workspace: Path, *, allow: bool = True) -> None:
    (workspace / "workflows/child.py").write_text(CHILD)
    (workspace / "workflows/parent.py").write_text(PARENT)
    if allow:
        (workspace / "config.toml").write_text(
            "[components.tools.approval]\napprove_all = true\n"
        )


async def test_subworkflow_history_and_descriptor_replay(
    workspace: Path, history: MemoryHistory
) -> None:
    prepare(workspace)
    runner = WorkflowRunner(workspace, history)
    result = connect.ToolResultMessage.model_validate(
        await runner.run("parent:main", workflow_id="root")
    )
    assert not result.is_error
    assert result.content == [connect.TextBlock(text="6")]
    path = history.path(history.heads["root"])
    kinds = [step.operation.kind for step in path]
    assert kinds.count("workflow.describe") == 1
    assert kinds.count("llm.tool") == 1
    assert kinds.count("workflow.spawn") == 1
    assert kinds.count("workflow.join") == 1
    before = list(history.appended)
    (workspace / "workflows/child.py").unlink()
    with (
        patch.object(
            WorkflowResolver, "describe", side_effect=AssertionError("live discovery")
        ),
        patch.object(
            tool_policy.ToolPolicy,
            "evaluate",
            side_effect=AssertionError("live policy"),
        ),
    ):
        replayed = await runner.run("parent:main", continue_from_step_id=path[-1].id)
    assert replayed == result.model_dump(mode="json")
    assert history.appended == before


@pytest.mark.parametrize("input", [{"reference": "other:main"}, {"value": "wrong"}])
async def test_rejects_unavailable_workflow_and_bad_input(
    workspace: Path, history: MemoryHistory, input: dict[str, pydantic.JsonValue]
) -> None:
    prepare(workspace)
    with patch.object(
        tool_policy.ToolPolicy,
        "evaluate",
        side_effect=AssertionError("policy before validation"),
    ):
        result = connect.ToolResultMessage.model_validate(
            await WorkflowRunner(workspace, history).run("parent:main", input=input)
        )
    assert result.is_error
    assert len(history.workflows) == 1


async def test_child_error_becomes_tool_error(
    workspace: Path, history: MemoryHistory
) -> None:
    prepare(workspace)
    result = connect.ToolResultMessage.model_validate(
        await WorkflowRunner(workspace, history).run("parent:main", input={"value": -1})
    )
    assert result.is_error
    assert result.content == [connect.TextBlock(text="negative input")]


@pytest.mark.parametrize("approved", [False, True])
async def test_approval_precedes_child_spawn_and_survives_restart(
    workspace: Path, approved: bool
) -> None:
    prepare(workspace, allow=False)
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("parent:main"))
    try:
        async with asyncio.timeout(5):
            while True:
                identifier = runner.last_execution_id
                if identifier is not None:
                    pending = await runner.pending_inputs(
                        mailbox.RequestQuery(execution_id=identifier)
                    )
                    if pending.requests:
                        break
                await asyncio.sleep(0.01)
        assert (await runner.inspect_execution(identifier)).stats.workflows == 1
        request = pending.requests[0]
        await runner.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=request.request_id,
                response={"approved": approved},
                idempotency_key="answer",
            )
        )
        result = connect.ToolResultMessage.model_validate(
            await asyncio.wait_for(task, 5)
        )
        assert result.is_error is not approved
        assert (await runner.inspect_execution(identifier)).stats.workflows == (
            2 if approved else 1
        )
        assert await WorkflowRunner(workspace).resume(identifier) == result.model_dump(
            mode="json"
        )
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_dynamic_factory_refresh(workspace: Path, history: MemoryHistory) -> None:
    prepare(workspace)
    (workspace / "workflows/parent.py").write_text("""
import asyncio
from pathlib import Path
from vectorops.runner.api import llm
from vectorops.runner.api.composition import activity

@activity()
async def generate(ctx, directory: str):
    await asyncio.to_thread((Path(directory) / "generated.py").write_text,
        "async def main(ctx, text: str): return text")

async def main(ctx):
    first = await llm.subworkflow_tool(["child:double"])
    await generate(str(ctx.project.workflows_path))
    second = await llm.subworkflow_tool(["child:double", "generated:main"])
    return [first.definition.description, second.definition.description]
""")
    result = await WorkflowRunner(workspace, history).run("parent:main")
    assert isinstance(result, list)
    assert "generated:main" not in str(result[0])
    assert "generated:main" in str(result[1])


async def test_mandatory_denial_prevents_child(
    workspace: Path, history: MemoryHistory
) -> None:
    prepare(workspace)
    with patch.object(
        tool_policy.ToolPolicy,
        "evaluate",
        return_value=tool_policy.Decision(action="deny", reason="blocked"),
    ):
        result = connect.ToolResultMessage.model_validate(
            await WorkflowRunner(workspace, history).run("parent:main")
        )
    assert result.is_error
    assert result.content == [connect.TextBlock(text="blocked")]
    assert len(history.workflows) == 1


async def test_pending_child_resumes_after_cancellation(workspace: Path) -> None:
    prepare(workspace)
    (workspace / "workflows/child.py").write_text("""
from vectorops.runner.api.composition import workflow
from vectorops.runner.api.mailbox import request_input

@workflow()
async def double(ctx, value: int) -> int:
    await request_input("continue", "Ready?")
    return value * 2
""")
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("parent:main"))
    try:
        async with asyncio.timeout(5):
            while True:
                identifier = runner.last_execution_id
                if identifier is not None:
                    pending = await runner.pending_inputs(
                        mailbox.RequestQuery(execution_id=identifier)
                    )
                    if pending.requests:
                        break
                await asyncio.sleep(0.01)
        assert runner.suspend_inputs(identifier)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        restarted = WorkflowRunner(workspace)
        await restarted.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=pending.requests[0].request_id,
                response="yes",
                idempotency_key="continue",
            )
        )
        result = connect.ToolResultMessage.model_validate(
            await asyncio.wait_for(restarted.resume(identifier), 5)
        )
        assert result.content == [connect.TextBlock(text="6")]
        assert not result.is_error
        assert (await restarted.inspect_execution(identifier)).stats.workflows == 2
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_llm_tool_loop_with_parallel_subworkflows(
    workspace: Path, history: MemoryHistory
) -> None:
    prepare(workspace)
    (workspace / "workflows/parent.py").write_text("""
import connect
from vectorops.runner.api import llm
from vectorops.runner.api.composition import final

class Provider:
    def __init__(self):
        self.calls = 0

    async def stream(self, ctx, settings, request):
        self.calls += 1
        if self.calls == 1:
            content = [connect.ToolCallBlock(id=str(value), name="subworkflow",
                arguments={"reference": "child:double", "input": {"value": value}})
                for value in (3, 4)]
        else:
            content = [connect.TextBlock(text="done")]
        yield final(connect.AssistantMessage(content=content,
            finish_reason="tool_call" if self.calls == 1 else "stop").model_dump(mode="json"))

async def main(ctx):
    tool = await llm.subworkflow_tool(["child:double"])
    conversation = llm.LLM("agent", model="test", backend=Provider())
    conversation.add_user_message("Double 3 and 4")
    answer = await conversation.run(tools=[tool])
    return answer.model_dump(mode="json")
""")
    runner = WorkflowRunner(workspace, history)
    result = await runner.run("parent:main", workflow_id="root")
    assert len(history.workflows) == 3
    before = list(history.appended)
    assert (
        await runner.run("parent:main", continue_from_step_id=history.heads["root"])
        == result
    )
    assert history.appended == before
