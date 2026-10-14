import asyncio
from pathlib import Path
from unittest.mock import patch

import connect
import pydantic
import pytest

from tests.api.test_workflow_tool_results import Arguments, WorkflowTool
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import tool_contracts as tools, tool_execution
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import AuthorizationPersistenceError
from vectorops.runner.runner.sqlite_history import SQLiteHistory


@pytest.mark.parametrize("workflow_tool", [False, True])
async def test_authorized_pending_call_resumes_without_reauthorization(
    history: MemoryHistory, tmp_path: Path, workflow_tool: bool
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        step = await history.get_step(ctx.step_id)
        assert step.authorized is True
        assert step.outcome.kind == "pending"
        started.set()
        await release.wait()
        return tools.ToolResponse(content=[connect.TextBlock(text="done")])

    class WaitingTool(WorkflowTool):
        async def execute_workflow(
            self,
            ctx: WorkflowExecutionContext,
            arguments: dict[str, pydantic.JsonValue],
        ) -> tools.ToolResponse:
            step = next(
                step
                for step in history.steps.values()
                if step.operation.kind == "llm.tool"
            )
            assert step.authorized is True
            started.set()
            await release.wait()
            return tools.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        binding = (
            WaitingTool()
            if workflow_tool
            else tools.Tool("test", "Test", Arguments, execute)
        )
        return (
            await tool_execution.invoke_tool(
                binding, connect.ToolCallBlock(id="call", name="test", arguments={})
            )
        ).model_dump(mode="json")

    task = asyncio.create_task(
        replay.run(history, workflow, tool_policy_version=1, workspace=tmp_path)
    )
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    step = next(
        step for step in history.steps.values() if step.operation.kind == "llm.tool"
    )
    assert step.outcome.kind == "pending" and step.authorized is True
    release.set()
    with patch.object(
        tool_execution.tool_approval,
        "authorize",
        side_effect=AssertionError("approval repeated"),
    ):
        completion = await replay.run(
            history, workflow, step.id, tool_policy_version=1, workspace=tmp_path
        )
    assert completion.outcome.kind == "completed"
    assert history.steps[step.id].authorized is True
    assert not any(
        record.operation.kind == "tool.authorization"
        for record in history.steps.values()
    )


async def test_authorization_update_is_durable_and_monotonic(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    step = models.WorkflowStep(
        id="call",
        workflow_id="root",
        operation=models.StepOperation(kind="llm.tool", name="invoke_tool"),
        outcome=models.StepPending(),
        deferred_delivery=True,
    )
    async with CachedHistory(database, flush_interval=60) as cache:
        await cache.save_workflow(
            models.Workflow(id="root", definition_ref="test:main")
        )
        await cache.append_step(step)
        authorized = step.authorize()
        await cache.update_step(authorized)
        assert (await SQLiteHistory(database.path).get_step(step.id)).authorized is True
        with pytest.raises(ValueError, match="revoked"):
            await cache.update_step(step)
        with pytest.raises(ValueError, match="revoked"):
            await database.update_step(step)
        await cache.update_step(
            authorized.model_copy(
                update={"outcome": models.StepCompleted(result="done")}
            )
        )
        assert (await SQLiteHistory(database.path).get_step(step.id)).authorized is True


@pytest.mark.parametrize("workflow_tool", [False, True])
async def test_authorization_write_failure_prevents_execution(
    history: MemoryHistory, tmp_path: Path, workflow_tool: bool
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    executed = False

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        nonlocal executed
        executed = True
        raise AssertionError("execution after failed authorization write")

    class ForbiddenTool(WorkflowTool):
        async def execute_workflow(
            self,
            ctx: WorkflowExecutionContext,
            arguments: dict[str, pydantic.JsonValue],
        ) -> tools.ToolResponse:
            nonlocal executed
            executed = True
            raise AssertionError("execution after failed authorization write")

    async def workflow() -> pydantic.JsonValue:
        await tool_execution.invoke_tool(
            (
                ForbiddenTool()
                if workflow_tool
                else tools.Tool("test", "Test", Arguments, execute)
            ),
            connect.ToolCallBlock(id="call", name="test", arguments={}),
        )
        return None

    history.fail_update = True
    with pytest.raises(AuthorizationPersistenceError):
        await replay.run(history, workflow, tool_policy_version=1, workspace=tmp_path)
    assert not executed


async def test_workflow_reconstruction_returns_original_response(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    text = "original"
    binding = WorkflowTool()

    class ChangingTool(WorkflowTool):
        async def execute_workflow(
            self,
            ctx: WorkflowExecutionContext,
            arguments: dict[str, pydantic.JsonValue],
        ) -> tools.ToolResponse:
            await binding.work("success")
            binding.rebuilt += 1
            return tools.ToolResponse(content=[connect.TextBlock(text=text)])

    async def workflow() -> pydantic.JsonValue:
        return (
            await tool_execution.invoke_tool(
                ChangingTool(),
                connect.ToolCallBlock(id="call", name="test", arguments={}),
            )
        ).model_dump(mode="json")

    first = await replay.run(
        history, workflow, tool_policy_version=1, workspace=tmp_path
    )
    assert first.outcome.kind == "completed"
    text = "changed"
    before = dict(history.steps)
    with patch.object(
        tool_execution.tool_approval,
        "authorize",
        side_effect=AssertionError("approval during replay"),
    ):
        assert (
            await replay.run(
                history, workflow, first.id, tool_policy_version=1, workspace=tmp_path
            )
            == first
        )
    assert binding.executed == 1 and binding.rebuilt == 2
    assert history.steps == before
