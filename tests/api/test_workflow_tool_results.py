import asyncio
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import connect
import pydantic
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.fixtures import replay, sources
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c, tool_contracts as tools
from vectorops.runner.api import tool_execution
from vectorops.runner.api.builtin_tools.plan import update_plan_tool
from vectorops.runner.api.llm import LLM
from vectorops.runner.control import client, protocol
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.events import StepEvent
from vectorops.runner.runner import history_query, mailbox
from vectorops.runner.runner.workflow import WorkflowRunner


class Arguments(pydantic.BaseModel):
    mode: str = "success"


async def test_persistent_plan_tool_results_and_replay(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/plan.py").write_text(sources.read("plan_tool_results.py"))
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    runner = WorkflowRunner(tmp_path)
    results = await runner.run("plan:main")
    identifier = runner.last_execution_id
    assert identifier is not None
    page = await runner.query_history(
        history_query.HistoryQuery(execution_id=identifier)
    )
    tool_steps = [
        item.step for item in page.items if item.step.operation.kind == "llm.tool"
    ]
    assert len(tool_steps) == 2
    recorded = [
        step.outcome.result for step in tool_steps if step.outcome.kind == "completed"
    ]
    assert recorded == results
    assert await WorkflowRunner(tmp_path).resume(identifier) == results


async def test_plan_approval_pause_preserves_public_call_identity(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/plan.py").write_text(sources.read("plan_tool_results.py"))
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            protocol.Start(reference="plan:main", idempotency_key="start")
        )
        async with asyncio.timeout(5):
            while True:
                try:
                    execution = await connection.execution(accepted.execution_id)
                except protocol.RemoteError as exc:
                    if exc.code != "not_found":
                        raise
                    await asyncio.sleep(0.01)
                    continue
                if execution.status == "paused":
                    break
                await asyncio.sleep(0.01)
        page = await connection.history(
            history_query.HistoryQuery(execution_id=accepted.execution_id)
        )
        tool_steps = [
            item.step for item in page.items if item.step.operation.kind == "llm.tool"
        ]
        assert len(tool_steps) == 1
        assert tool_steps[0].outcome.kind == "pending"
        requests = await connection.pending_inputs(
            mailbox.RequestQuery(execution_id=accepted.execution_id)
        )
        assert requests.requests[0].request_id == tool_steps[0].id


class WorkflowTool(tools.WorkflowToolBinding):
    def __init__(self) -> None:
        self.executed = 0
        self.rebuilt = 0

    @property
    def definition(self) -> connect.ToolSpec:
        return connect.ToolSpec.external(
            name="test", description="Test", input_schema=Arguments.model_json_schema()
        )

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> Optional[tools.ToolApproval]:
        return None

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        Arguments.model_validate(arguments)

    @c.activity_wrapper(name="work", kind="test.work")
    async def work(self, call: c.ActivityCall, mode: str) -> pydantic.JsonValue:
        async def invoke(context: ActivityExecutionContext) -> pydantic.JsonValue:
            self.executed += 1
            return mode

        return await call.run(input={"mode": mode}, invoke=invoke)

    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        mode = await self.work(Arguments.model_validate(arguments).mode)
        self.rebuilt += 1
        if mode == "exception":
            raise RuntimeError("exact failure")
        if mode == "cancel":
            raise asyncio.CancelledError
        return tools.ToolResponse(
            content=[connect.TextBlock(text="result")], is_error=mode == "error"
        )


async def test_workflow_validation_failure_has_final_tool_result(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    binding = WorkflowTool()

    async def workflow() -> pydantic.JsonValue:
        result = await tool_execution.invoke_tool(
            binding,
            connect.ToolCallBlock(id="call", name="test", arguments={"mode": 42}),
        )
        assert result.is_error
        return result.model_dump(mode="json")

    first = await replay.run(
        history, workflow, tool_policy_version=1, workspace=tmp_path
    )
    assert first.outcome.kind == "completed"
    steps = [
        step for step in history.path(first.id) if step.operation.kind == "llm.tool"
    ]
    assert len(steps) == 1
    assert steps[0].outcome == first.outcome
    assert binding.executed == binding.rebuilt == 0
    before = dict(history.steps)
    assert (
        await replay.run(
            history, workflow, first.id, tool_policy_version=1, workspace=tmp_path
        )
    ) == first
    assert history.steps == before


async def test_workflow_tool_resumes_before_final_result(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    binding = WorkflowTool()

    async def workflow() -> pydantic.JsonValue:
        return (
            await tool_execution.invoke_tool(
                binding, connect.ToolCallBlock(id="call", name="test", arguments={})
            )
        ).model_dump(mode="json")

    first = await replay.run(
        history, workflow, tool_policy_version=1, workspace=tmp_path
    )
    assert first.outcome.kind == "completed"
    path = history.path(first.id)
    child = next(step for step in path if step.operation.kind == "test.work")
    outer = next(step for step in path if step.operation.kind == "llm.tool")
    history.steps[outer.id] = outer.model_copy(
        update={"outcome": models.StepPending(), "completed_at": None}
    )
    history.steps[child.id] = child.model_copy(update={"deliveries": [child.id]})
    for step in path[path.index(child) + 1 :]:
        del history.steps[step.id]
    history.heads[child.workflow_id] = child.id
    binding = WorkflowTool()
    resumed = await replay.run(
        history, workflow, child.id, tool_policy_version=1, workspace=tmp_path
    )
    assert resumed.outcome == first.outcome
    assert binding.executed == 0
    assert binding.rebuilt == 1
    results = [
        step for step in history.path(resumed.id) if step.operation.kind == "llm.tool"
    ]
    assert len(results) == 1
    assert results[0].outcome == resumed.outcome


@pytest.mark.parametrize("workflow_tool", [False, True])
@pytest.mark.parametrize("mode", ["success", "error", "exception"])
@pytest.mark.parametrize("strict", [False, True])
async def test_tool_result_protocol_parity(
    history: MemoryHistory, tmp_path: Path, workflow_tool: bool, mode: str, strict: bool
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    binding = WorkflowTool()
    events: list[StepEvent] = []
    results: list[connect.ToolResultMessage] = []

    async def execute(
        context: ActivityExecutionContext, arguments: Arguments
    ) -> tools.ToolResponse:
        if arguments.mode == "exception":
            raise RuntimeError("exact failure")
        return tools.ToolResponse(
            content=[connect.TextBlock(text="result")],
            is_error=arguments.mode == "error",
        )

    async def workflow() -> pydantic.JsonValue:
        selected: tools.ToolBinding = (
            binding if workflow_tool else tools.Tool("test", "Test", Arguments, execute)
        )
        request = connect.ToolCallBlock(
            id="call", name="test", arguments={"mode": mode}
        )
        if strict and mode == "exception":
            with pytest.raises(ActivityError, match="exact failure"):
                await tool_execution.invoke_tool(selected, request, raise_errors=True)
        else:
            result = await tool_execution.invoke_tool(
                selected, request, raise_errors=strict
            )
            results.append(result)
        return None

    first = await replay.run(
        history,
        workflow,
        tool_policy_version=1,
        workspace=tmp_path,
        event_sink=events.append,
    )
    assert first.outcome.kind == "completed"
    tool_steps = [
        step for step in history.path(first.id) if step.operation.kind == "llm.tool"
    ]
    assert len(tool_steps) == 1
    step = tool_steps[0]
    if strict and mode == "exception":
        assert step.outcome.kind == "failed"
        assert step.outcome.error.message == "exact failure"
    else:
        assert step.outcome.kind == "completed"
        assert (
            connect.ToolResultMessage.model_validate(step.outcome.result) == results[0]
        )
        assert results[0].tool_call_id == "call"
        assert results[0].tool_name == "test"
        assert results[0].is_error is (mode != "success")
        assert results[0].content == [
            connect.TextBlock(text="exact failure" if mode == "exception" else "result")
        ]
    assert any(
        event.step.id == step.id and event.step.outcome == step.outcome
        for event in events
    )
    lifecycle = [event for event in events if event.step.id == step.id]
    assert lifecycle[0].kind == "step.created"
    assert lifecycle[0].step.outcome.kind == "pending"
    assert lifecycle[-1].kind == (
        "step.failed" if strict and mode == "exception" else "step.completed"
    )
    gates = [
        step
        for step in history.path(first.id)
        if step.operation.kind == "llm.tool.authorization"
    ]
    assert len(gates) == int(workflow_tool)
    if gates:
        assert isinstance(gates[0].operation, models.StepOperation)
        assert gates[0].operation.input == {"invocation_id": step.id}
        assert gates[0].authorized is None
    before = dict(history.steps)
    second = await replay.run(
        history, workflow, first.id, tool_policy_version=1, workspace=tmp_path
    )
    assert second == first
    assert history.steps == before
    if workflow_tool:
        assert binding.executed == 1
        assert binding.rebuilt == 2
    if results:
        assert results == [results[0], results[0]]


async def test_plan_full_results_reach_history_and_model(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    backend = FakeProvider(
        [
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="first",
                        name="update_plan",
                        arguments={
                            "merge": False,
                            "todos": [
                                {"id": "a", "title": "First", "status": "in_progress"},
                                {"id": "b", "title": "Second", "status": "pending"},
                            ],
                        },
                    )
                ],
                finish_reason="tool_call",
            ),
            connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="second",
                        name="update_plan",
                        arguments={"todos": [{"id": "a", "status": "completed"}]},
                    )
                ],
                finish_reason="tool_call",
            ),
            answer("done"),
        ]
    )
    snapshots: list[list[pydantic.JsonValue]] = []

    async def workflow() -> pydantic.JsonValue:
        binding = update_plan_tool()
        agent = LLM("chat", model="test", backend=backend)
        agent.add_user_message("Plan")
        await agent.run(tools=[binding])
        snapshots.append([todo.model_dump(mode="json") for todo in binding.todos])
        return None

    first = await replay.run(
        history, workflow, tool_policy_version=1, workspace=tmp_path
    )
    assert first.outcome.kind == "completed"
    recorded = [
        connect.ToolResultMessage.model_validate(step.outcome.result)
        for step in history.path(first.id)
        if step.operation.kind == "llm.tool" and step.outcome.kind == "completed"
    ]
    model_results = [
        message for message in backend.requests[-1].messages if message.role == "tool"
    ]
    assert recorded == model_results
    assert [message.tool_call_id for message in recorded] == ["first", "second"]
    block = recorded[1].content[0]
    assert isinstance(block, connect.TextBlock)
    assert pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
        block.text
    ) == {
        "todos": [
            {"id": "a", "title": "First", "status": "completed"},
            {"id": "b", "title": "Second", "status": "pending"},
        ]
    }
    before = dict(history.steps)
    assert (
        await replay.run(
            history, workflow, first.id, tool_policy_version=1, workspace=tmp_path
        )
    ) == first
    assert snapshots[0] == snapshots[1]
    assert history.steps == before


async def test_legacy_workflow_gate_replays_without_new_steps(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    request = connect.ToolCallBlock(
        id="plan",
        name="update_plan",
        arguments={"todos": [{"id": "a", "title": "First", "status": "pending"}]},
    )

    async def legacy() -> pydantic.JsonValue:
        binding = update_plan_tool()
        gate = await tool_execution._gated_tool(
            binding, request, None, False, gate_only=True
        )
        assert not gate.is_error
        response = await binding.execute_workflow(
            c.workflow_context(), request.arguments
        )
        return tool_execution._result(request, response).model_dump(mode="json")

    first = await replay.run(history, legacy, tool_policy_version=1, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    before = dict(history.steps)

    async def current() -> pydantic.JsonValue:
        return (
            await tool_execution.invoke_tool(update_plan_tool(), request)
        ).model_dump(mode="json")

    assert (
        await replay.run(
            history, current, first.id, tool_policy_version=1, workspace=tmp_path
        )
    ) == first
    assert history.steps == before


@pytest.mark.parametrize("mode", ["success", "exception", "invalid"])
async def test_legacy_outer_workflow_tool_replays_without_gate(
    history: MemoryHistory, tmp_path: Path, mode: str
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    binding = WorkflowTool()
    request = connect.ToolCallBlock(
        id="call", name="test", arguments={"mode": 42 if mode == "invalid" else mode}
    )

    async def invoke(
        ctx: ActivityExecutionContext, step: models.WorkflowStep
    ) -> pydantic.JsonValue:
        denial = await tool_execution._authorize_call(
            ctx, binding, request, None, False
        )
        result = (
            denial
            if denial is not None
            else await tool_execution._execute_binding(binding, ctx, request, False)
        )
        return result.model_dump(mode="json")

    async def legacy() -> pydantic.JsonValue:
        return await c.run_recorded_invocation(
            models.StepOperation(
                kind="llm.tool",
                name="invoke_tool",
                version=3,
                input={"tool_name": "test"},
            ),
            invoke,
        )

    first = await replay.run(history, legacy, tool_policy_version=1, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    before = dict(history.steps)
    assert not any(
        step.operation.kind == "llm.tool.authorization" for step in before.values()
    )

    async def current() -> pydantic.JsonValue:
        return (await tool_execution.invoke_tool(binding, request)).model_dump(
            mode="json"
        )

    with patch.object(
        tool_execution,
        "_authorize_call",
        side_effect=AssertionError("authorization during legacy replay"),
    ):
        assert (
            await replay.run(
                history, current, first.id, tool_policy_version=1, workspace=tmp_path
            )
            == first
        )
    assert history.steps == before
    assert binding.rebuilt == (0 if mode == "invalid" else 2)


async def test_workflow_tool_cancellation_has_no_final_result(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )

    async def workflow() -> pydantic.JsonValue:
        await tool_execution.invoke_tool(
            WorkflowTool(),
            connect.ToolCallBlock(id="call", name="test", arguments={"mode": "cancel"}),
        )
        return None

    with pytest.raises(asyncio.CancelledError):
        await replay.run(history, workflow, tool_policy_version=1, workspace=tmp_path)
    tool_steps = [
        step for step in history.steps.values() if step.operation.kind == "llm.tool"
    ]
    assert len(tool_steps) == 1
    assert tool_steps[0].outcome.kind == "pending"
