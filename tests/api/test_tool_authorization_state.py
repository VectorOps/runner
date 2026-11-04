import asyncio
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Literal, Optional, Union
from unittest.mock import patch

import connect
import pydantic
import pytest

from tests.api.test_workflow_tool_results import Arguments, WorkflowTool
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models, tool_policy
from vectorops.runner import tool_authorization, tool_authorization_store
from vectorops.runner.api import (
    composition as c,
    tool_contracts as tools,
    tool_execution,
)
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import AuthorizationPersistenceError
from vectorops.runner.runner import errors
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.project import RunnerConfig, RunnerProject
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.sqlite_history import SQLiteHistory


@pytest.mark.parametrize("workflow_tool", [False, True])
@pytest.mark.parametrize("tool_policy_version", [None, 1])
@pytest.mark.parametrize("legacy_input", [False, True])
async def test_authorized_pending_call_resumes_without_reauthorization(
    history: MemoryHistory,
    tmp_path: Path,
    workflow_tool: bool,
    tool_policy_version: Optional[Literal[1]],
    legacy_input: bool,
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
                binding,
                connect.ToolCallBlock(
                    id="call", name="test", arguments={"mode": "success"}
                ),
            )
        ).model_dump(mode="json")

    task = asyncio.create_task(
        replay.run(
            history,
            workflow,
            tool_policy_version=tool_policy_version,
            workspace=tmp_path,
        )
    )
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    step = next(
        step for step in history.steps.values() if step.operation.kind == "llm.tool"
    )
    assert step.outcome.kind == "pending" and step.authorized is True
    assert isinstance(step.operation, models.StepOperation)
    assert step.operation.name == "invoke_tool"
    assert step.operation.input["tool_name"] == "test"
    assert step.operation.input["call"] == connect.ToolCallBlock(
        id="call", name="test", arguments={"mode": "success"}
    ).model_dump(mode="json")
    if legacy_input:
        recorded_input = dict(step.operation.input)
        del recorded_input["tool_name"]
        del recorded_input["call"]
        history.steps[step.id] = step.model_copy(
            update={
                "operation": step.operation.model_copy(update={"input": recorded_input})
            }
        )
    release.set()
    with patch.object(
        tool_policy.ToolPolicy,
        "check",
        side_effect=AssertionError("approval repeated"),
    ):
        completion = await replay.run(
            history,
            workflow,
            step.id,
            tool_policy_version=tool_policy_version,
            workspace=tmp_path,
        )
    assert completion.outcome.kind == "completed"
    assert history.steps[step.id].authorized is True
    gates = [
        record
        for record in history.path(completion.id)
        if record.operation.kind == "llm.tool.authorization"
    ]
    assert len(gates) == int(workflow_tool)


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
    if workflow_tool:
        gate = next(
            step
            for step in history.steps.values()
            if step.operation.kind == "llm.tool.authorization"
        )
        outer = next(
            step for step in history.steps.values() if step.operation.kind == "llm.tool"
        )
        assert gate.outcome.kind == outer.outcome.kind == "pending"
        assert outer.authorized is None


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
        tool_policy.ToolPolicy,
        "check",
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


async def _run_history(
    history: WorkflowHistory,
    workflow: Callable[[], Awaitable[pydantic.JsonValue]],
    workspace: Path,
    policy: tool_policy.ToolPolicy,
    head: Optional[str] = None,
) -> models.WorkflowStep:
    """Run the same authorization regression against memory and SQLite storage."""

    async def child(
        configuration: models.Workflow,
        *,
        continue_from_step_id: Optional[str] = None,
        child_heads: Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        raise AssertionError("unexpected child workflow")

    configuration = models.Workflow(
        id="test", definition_ref="test:workflow", tool_policy_version=1
    )
    if head is None:
        await history.save_workflow(configuration)
    project = RunnerProject(
        workspace,
        RunnerConfig(components={"tools": {"approval": {"approve_all": True}}}),
    )
    session = ExecutionSession(
        history, configuration, child, continue_from_step_id=head, child_heads={}
    )
    context = WorkflowExecutionContext(project, configuration, session)
    authorization = tool_authorization.ToolAuthorizationService(
        tool_authorization_store.AuthorizationStore(project.configuration), policy
    )
    token = c._CURRENT_CONTEXT.set(context)
    try:
        with authorization.activate():
            return await session.run(workflow)
    finally:
        c._CURRENT_CONTEXT.reset(token)


@pytest.mark.parametrize("sqlite", [False, True])
@pytest.mark.parametrize("denied", [False, True])
@pytest.mark.parametrize("strict", [False, True])
async def test_workflow_authorization_delivery_order(
    tmp_path: Path, sqlite: bool, denied: bool, strict: bool
) -> None:
    history: Union[MemoryHistory, SQLiteHistory] = (
        SQLiteHistory(tmp_path / "history.sqlite3") if sqlite else MemoryHistory()
    )
    configuration_started = asyncio.Event()
    authorization_started = asyncio.Event()
    live = True
    mismatch = False
    trace: list[str] = []
    rebuilt: list[pydantic.JsonValue] = []
    responses: list[connect.ToolResultMessage] = []
    policy = tool_policy.ToolPolicy()

    async def check(call: tool_policy.ToolCall) -> Optional[str]:
        assert live, "live policy during replay"
        outer = await history.get_step(call.step_id)
        assert outer.operation.kind == "llm.tool"
        authorization_started.set()
        await configuration_started.wait()
        return "denied by test" if denied else None

    policy.register_check("test", check)

    async def operation(name: str) -> pydantic.JsonValue:
        trace.append(f"submit:{name}")

        async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
            assert live, "completed activity implementation during replay"
            if name == "worker":
                await authorization_started.wait()
            if name == "configuration":
                configuration_started.set()
            return name

        result = await c.workflow_context().run_activity(
            models.StepOperation(kind=f"test.{name}", name=name), invoke
        )
        trace.append(f"deliver:{name}")
        return result

    class OrderedTool(WorkflowTool):
        async def execute_workflow(
            self,
            context: WorkflowExecutionContext,
            arguments: dict[str, pydantic.JsonValue],
        ) -> tools.ToolResponse:
            rebuilt.append(await operation("changed" if mismatch else "validation"))
            return tools.ToolResponse(
                content=[connect.TextBlock(text="original" if live else "changed")]
            )

    async def sibling() -> None:
        await operation("worker")
        await operation("configuration")

    async def tool() -> None:
        trace.append("submit:tool")
        try:
            result = await tool_execution.invoke_tool(
                OrderedTool(),
                connect.ToolCallBlock(id="call", name="test", arguments={}),
                raise_errors=strict,
            )
        except errors.ActivityError as exc:
            assert denied and strict
            assert "denied by test" in str(exc)
        else:
            assert not (denied and strict)
            assert result.is_error is denied
            responses.append(result)
        trace.append("deliver:tool")

    async def workflow() -> pydantic.JsonValue:
        async with asyncio.TaskGroup() as group:
            group.create_task(tool())
            group.create_task(sibling())
        return None

    try:
        async with asyncio.timeout(5):
            first = await _run_history(history, workflow, tmp_path, policy)
        assert first.outcome.kind == "completed", first.outcome
        before = [step async for step in history.replay_path("test", first.id)]
        kinds = [step.operation.kind for step in before]
        outer = next(step for step in before if step.operation.kind == "llm.tool")
        gate = next(
            step for step in before if step.operation.kind == "llm.tool.authorization"
        )
        assert isinstance(outer.operation, models.StepOperation)
        assert (
            outer.operation.input["workflow_tool_format"] == "authorization.activity.v1"
        )
        assert isinstance(gate.operation, models.StepOperation)
        assert gate.operation.input == {"invocation_id": outer.id}
        assert outer.authorized is (None if denied else True)
        assert gate.authorized is None
        deliveries = [identifier for step in before for identifier in step.deliveries]
        assert deliveries.index(gate.id) < deliveries.index(outer.id)
        gate_delivery = next(
            i for i, step in enumerate(before) if gate.id in step.deliveries
        )
        assert kinds.index("test.configuration") <= gate_delivery
        if not denied:
            assert kinds.index("test.configuration") < kinds.index("test.validation")
            assert rebuilt == ["validation"]
        else:
            assert "test.validation" not in kinds
            assert rebuilt == []
        initial_trace = list(trace)
        initial_responses = list(responses)
        trace.clear()
        responses.clear()
        live = False
        with (
            patch.object(
                tool_authorization.ToolAuthorizationService,
                "authorize",
                side_effect=AssertionError("authorization during replay"),
            ),
            patch.object(
                history, "append_step", side_effect=AssertionError("replay append")
            ),
            patch.object(
                history, "update_step", side_effect=AssertionError("replay update")
            ),
            patch.object(
                history,
                "record_delivery",
                side_effect=AssertionError("replay delivery write"),
            ),
        ):
            async with asyncio.timeout(5):
                assert (
                    await _run_history(history, workflow, tmp_path, policy, first.id)
                    == first
                )
        assert trace == initial_trace
        assert responses == initial_responses
        assert rebuilt == ([] if denied else ["validation", "validation"])
        assert [step async for step in history.replay_path("test", first.id)] == before
        if not denied:
            mismatch = True
            async with asyncio.timeout(5):
                with pytest.raises(errors.ReplayMismatchError):
                    await _run_history(history, workflow, tmp_path, policy, first.id)
    finally:
        if isinstance(history, SQLiteHistory):
            await history.aclose()


@pytest.mark.parametrize("delivered", [False, True])
async def test_workflow_authorization_interrupted_delivery(
    history: MemoryHistory, tmp_path: Path, delivered: bool
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    interrupted = asyncio.Event()
    release = asyncio.Event()
    record_delivery = history.record_delivery
    binding = WorkflowTool()

    async def delivery(head_id: str, step_id: str) -> models.WorkflowStep:
        step = await history.get_step(step_id)
        if step.operation.kind != "llm.tool.authorization":
            return await record_delivery(head_id, step_id)
        if delivered:
            result = await record_delivery(head_id, step_id)
        interrupted.set()
        await release.wait()
        return result if delivered else await record_delivery(head_id, step_id)

    async def workflow() -> pydantic.JsonValue:
        return (
            await tool_execution.invoke_tool(
                binding, connect.ToolCallBlock(id="call", name="test", arguments={})
            )
        ).model_dump(mode="json")

    with patch.object(history, "record_delivery", delivery):
        task = asyncio.create_task(
            replay.run(history, workflow, tool_policy_version=1, workspace=tmp_path)
        )
        try:
            await asyncio.wait_for(interrupted.wait(), 5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert binding.executed == binding.rebuilt == 0
    head = history.heads["test"]
    gate = next(
        step
        for step in history.steps.values()
        if step.operation.kind == "llm.tool.authorization"
    )
    assert gate.outcome.kind == "completed"
    assert (
        gate.id
        in [identifier for step in history.path(head) for identifier in step.deliveries]
    ) is delivered
    with patch.object(
        tool_authorization.ToolAuthorizationService,
        "authorize",
        side_effect=AssertionError("authorization repeated after interruption"),
    ):
        async with asyncio.timeout(5):
            completion = await replay.run(
                history, workflow, head, tool_policy_version=1, workspace=tmp_path
            )
    assert completion.outcome.kind == "completed"
    assert binding.executed == binding.rebuilt == 1
