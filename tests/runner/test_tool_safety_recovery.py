import asyncio
from pathlib import Path
from typing import Union
from unittest.mock import AsyncMock, patch

import connect
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from tests.control.test_client_commands import pending, settled
from tests.runner.test_authorization_model import tool_step
from tests.runner import test_tool_safety as safety
from tests.runner.test_usage import seed
from vectorops.runner import models
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import history_query, mailbox
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.errors import AuthorizationPersistenceError
from vectorops.runner.runner.llm import RunnerLLM
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def approve(
    message: connect.UserMessage, *, system_prompt: str, **options: object
) -> connect.AssistantMessage:
    return safety.response(safety.token(system_prompt))


@pytest.mark.parametrize("sqlite", [False, True])
@pytest.mark.parametrize("workflow_tool", [False, True])
async def test_runner_safety_assessment_fresh_execution_and_completed_replay(
    workspace: Path, execution_probe: ExecutionProbe, sqlite: bool, workflow_tool: bool
) -> None:
    history: Union[MemoryHistory, SQLiteHistory] = (
        SQLiteHistory(workspace / "history.sqlite3") if sqlite else MemoryHistory()
    )
    runner = WorkflowRunner(workspace, history)
    project = await runner.project()
    await project.configuration.set("tools.safety.enabled", True)
    generate = AsyncMock(side_effect=approve)
    try:
        with patch.object(RunnerLLM, "generate", generate):
            result = await runner.run(
                "safety:main",
                workflow_id="root",
                input={"workflow_tool": workflow_tool},
            )
        assert connect.ToolResultMessage.model_validate(result).is_error is False
        assert execution_probe.calls == [1]
        context = execution_probe.contexts[0]
        steps = [entry async for entry in history.replay_path("root", context.step_id)]
        step = next(entry for entry in steps if entry.operation.kind == "llm.tool")
        assert step.safety_assessment is not None and step.safety_assessment.approved
        assert step.authorized is True and step.authorization_rule_id is None
        assert step.outcome.kind == "completed"
        assert context.authorized is False
        assert generate.await_count == 1
        head = context.step_id
        before = [entry async for entry in history.replay_path("root", head)]
        await project.configuration.set("tools.safety", {"enabled": False})
        with patch.object(
            RunnerLLM, "generate", side_effect=AssertionError("model during replay")
        ):
            assert await runner.run("safety:main", continue_from_step_id=head) == result
        assert execution_probe.calls == [1]
        assert [entry async for entry in history.replay_path("root", head)] == before
    finally:
        if isinstance(history, SQLiteHistory):
            await history.aclose()


@pytest.mark.parametrize("sqlite", [False, True])
@pytest.mark.parametrize("workflow_tool", [False, True])
async def test_completed_assessment_survives_interruption_before_authorization(
    workspace: Path, execution_probe: ExecutionProbe, sqlite: bool, workflow_tool: bool
) -> None:
    history: Union[MemoryHistory, SQLiteHistory] = (
        SQLiteHistory(workspace / "history.sqlite3") if sqlite else MemoryHistory()
    )
    runner = WorkflowRunner(workspace, history)
    project = await runner.project()
    await project.configuration.set("tools.safety.enabled", True)
    persisted = asyncio.Event()
    saved: list[models.WorkflowStep] = []
    original = history.update_step

    async def update(step: models.WorkflowStep) -> None:
        await original(step)
        if step.safety_assessment is not None and step.authorized is None:
            saved.append(step)
            persisted.set()
            await asyncio.Event().wait()

    generate = AsyncMock(side_effect=approve)
    try:
        with (
            patch.object(history, "update_step", update),
            patch.object(RunnerLLM, "generate", generate),
        ):
            task = asyncio.create_task(
                runner.run(
                    "safety:main",
                    workflow_id="root",
                    input={"workflow_tool": workflow_tool},
                )
            )
            try:
                await asyncio.wait_for(persisted.wait(), 5)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        assert execution_probe.calls == []
        step = saved[0]
        assert step.authorized is None and step.outcome.kind == "pending"
        if isinstance(history, SQLiteHistory):
            path = history.path
            await history.aclose()
            history = SQLiteHistory(path)
        restored = await history.get_step(step.id)
        assert restored.safety_assessment == step.safety_assessment
        await project.configuration.set("tools.safety", {"enabled": False})
        restarted = WorkflowRunner(workspace, history, project=project)
        with patch.object(
            RunnerLLM, "generate", side_effect=AssertionError("assessment repeated")
        ):
            result = await restarted.run("safety:main", continue_from_step_id=step.id)
        assert connect.ToolResultMessage.model_validate(result).is_error is False
        assert execution_probe.calls == [1]
        assert (
            await history.get_step(step.id)
        ).safety_assessment == step.safety_assessment
        assert generate.await_count == 1
    finally:
        if isinstance(history, SQLiteHistory):
            await history.aclose()


@pytest.mark.parametrize("human_approves", [True, False])
async def test_model_abstention_and_pending_human_confirmation_survive_restart(
    workspace: Path, execution_probe: ExecutionProbe, human_approves: bool
) -> None:
    generate = AsyncMock(return_value=safety.response("UNSAFE"))
    with patch.object(RunnerLLM, "generate", generate):
        async with client.embedded(workspace) as connection:
            await connection.set_configuration(
                "tools.safety.enabled", True, permanent=True
            )
            accepted = await connection.start(
                protocol.Start(reference="safety:main", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            page = await connection.history(
                history_query.HistoryQuery(execution_id=accepted.execution_id)
            )
            tool = next(
                item.step
                for item in page.items
                if item.step.operation.kind == "llm.tool"
            )
            assert (
                tool.safety_assessment is not None
                and not tool.safety_assessment.approved
            )
            assert tool.authorized is None
            assert execution_probe.calls == []
        assert generate.await_count == 1
    with patch.object(
        RunnerLLM,
        "generate",
        side_effect=AssertionError("model during pending approval"),
    ):
        async with client.embedded(workspace) as connection:
            restored = await pending(connection, accepted.execution_id)
            assert restored.request_id == request.request_id
            await connection.set_configuration("tools.safety.prompt", "Changed policy")
            await connection.answer_input(
                mailbox.Answer(
                    execution_id=accepted.execution_id,
                    request_id=request.request_id,
                    response={"decision": "approve" if human_approves else "deny"},
                    idempotency_key="human",
                )
            )
            await settled(connection, accepted.execution_id)
            page = await connection.history(
                history_query.HistoryQuery(execution_id=accepted.execution_id)
            )
            tool = next(
                item.step
                for item in page.items
                if item.step.operation.kind == "llm.tool"
            )
            assert (
                tool.safety_assessment is not None
                and not tool.safety_assessment.approved
            )
            assert tool.outcome.kind == "completed"
            result = connect.ToolResultMessage.model_validate(tool.outcome.result)
            assert result.is_error is not human_approves
            assert execution_probe.calls == ([1] if human_approves else [])


async def test_failed_assessment_write_prevents_tool_execution(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    project = await runner.project()
    await project.configuration.set("tools.safety.enabled", True)
    original = history.update_step

    async def update(step: models.WorkflowStep) -> None:
        if step.safety_assessment is not None:
            raise OSError("assessment write failed")
        await original(step)

    with (
        patch.object(history, "update_step", update),
        patch.object(RunnerLLM, "generate", side_effect=approve),
    ):
        with pytest.raises(AuthorizationPersistenceError):
            await runner.run("safety:main", workflow_id="root")
    assert execution_probe.calls == []


@pytest.mark.parametrize("approved", [True, False])
async def test_assessment_sqlite_cache_durability_and_monotonicity(
    tmp_path: Path, approved: bool
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    step = tool_step()
    try:
        async with CachedHistory(database, flush_interval=60) as cache:
            await cache.save_workflow(
                models.Workflow(id="root", definition_ref="test:main")
            )
            await cache.append_step(step)
            assessed = step.assess_safety(safety.assessment(approved))
            await cache.update_step(assessed)
            reader = SQLiteHistory(database.path)
            try:
                assert (
                    await reader.get_step(step.id)
                ).safety_assessment == assessed.safety_assessment
            finally:
                await reader.aclose()
            with pytest.raises(ValueError, match="removed"):
                await cache.update_step(step)
            with pytest.raises(ValueError, match="removed"):
                await database.update_step(step)
            changed = assessed.model_copy(
                update={"safety_assessment": safety.assessment(not approved)}
            )
            with pytest.raises(ValueError, match="unassessed pending"):
                await cache.update_step(changed)
            await cache.update_step(assessed.authorize())
            await cache.update_step(
                assessed.authorize().model_copy(
                    update={"outcome": models.StepCompleted(result="done")}
                )
            )
            assert (
                await database.get_step(step.id)
            ).safety_assessment == assessed.safety_assessment
    finally:
        await database.aclose()


async def test_assessment_updates_keep_summary_projection_consistent(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    try:
        await seed(history)
        step = tool_step()
        await history.append_step(step)
        before = await history.execution_summary()
        await history.update_step(step.assess_safety(safety.assessment(False)))
        assert await history.execution_summary() == before
        assert await project.summaries.get("execution") == before
    finally:
        await history.aclose()
