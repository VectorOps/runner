import asyncio
from pathlib import Path

import connect
import pydantic
import pytest

from tests.api.test_tools import ApprovalStore, approve
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import input_schema, tool_policy
from vectorops.runner.api import llm
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.project import RunnerProject


class Arguments(pydantic.BaseModel):
    value: int


@pytest.mark.parametrize("settings", ["approve_all = true", 'whitelist = ["test"]'])
async def test_grants_and_replay_without_policy_reads(
    history: MemoryHistory, tmp_path: Path, settings: str
) -> None:
    config = tmp_path / "config.toml"
    config.write_text("[components.tools.approval]\n" + settings)
    project = await RunnerProject.open(tmp_path)
    calls: list[int] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        calls.append(args.value)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        return (
            await llm.invoke_tool(
                llm.Tool(
                    "test", "Test", Arguments, execute, approval=llm.ToolApproval()
                ),
                connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
            )
        ).model_dump(mode="json")

    first = await replay.run(history, workflow, project=project, tool_policy_version=1)
    assert first.outcome.kind == "completed"
    assert calls == [1]
    assert {step.operation.kind for step in history.path(first.id)} == {
        "llm.tool",
        "workflow.resume",
        "workflow.complete",
    }
    config.write_text("invalid TOML")
    second = await replay.run(
        history, workflow, first.id, project=project, tool_policy_version=1
    )
    assert second.outcome == first.outcome
    assert calls == [1]


@pytest.mark.parametrize(
    "answer",
    [{"approved": True}, {"approved": False, "reason": "no"}, {"approved": "yes"}],
)
@pytest.mark.parametrize("legacy", [False, True])
async def test_host_gate_recovery_and_result_replay(
    history: MemoryHistory,
    tmp_path: Path,
    answer: dict[str, pydantic.JsonValue],
    legacy: bool,
) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    calls: list[str] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        calls.append(ctx.step_id)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        return (
            await llm.invoke_tool(
                llm.Tool("test", "Test", Arguments, execute),
                connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
            )
        ).model_dump(mode="json")

    task = asyncio.create_task(
        replay.run(
            history, workflow, mailbox=box, workspace=tmp_path, tool_policy_version=1
        )
    )
    await asyncio.wait_for(store.registered.wait(), 2)
    request = next(iter(store.requests.values()))
    assert (await history.get_step(request.request_id)).operation.kind == "llm.tool"
    box.suspending = True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    if legacy:
        request = request.model_copy(
            update={
                "request_type": None,
                "request_schema": None,
                "response_schema": None,
            }
        )
        store.requests[request.request_id] = request
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true"
    )
    if not legacy and answer.get("approved") == "yes":
        with pytest.raises(input_schema.InputValidationError):
            await approve(box, request, answer)
        assert store.requests[request.request_id].status == "pending"
        answer = {"approved": False}
    await approve(box, request, answer)
    first = await replay.run(
        history,
        workflow,
        request.request_id,
        mailbox=mailbox.Mailbox(store),
        workspace=tmp_path,
        tool_policy_version=1,
    )
    assert first.outcome.kind == "completed"
    result = connect.ToolResultMessage.model_validate(first.outcome.result)
    approved = answer.get("approved") is True
    assert result.is_error is not approved
    assert calls == ([request.request_id] if approved else [])
    second = await replay.run(
        history, workflow, first.id, workspace=tmp_path, tool_policy_version=1
    )
    assert second.outcome == first.outcome
    assert len(store.requests) == 1


@pytest.mark.parametrize("strict", [False, True])
async def test_mandatory_check_denial_overrides_approve_all(
    history: MemoryHistory, tmp_path: Path, strict: bool
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true"
    )
    policy = tool_policy.ToolPolicy()

    async def deny(call: tool_policy.ToolCall) -> str:
        return "blocked"

    policy.register_check("deny", deny)

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("denied tools must not execute")

    async def workflow() -> pydantic.JsonValue:
        return (
            await llm.invoke_tool(
                llm.Tool("test", "Test", Arguments, execute),
                connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
                raise_errors=strict,
            )
        ).model_dump(mode="json")

    first = await replay.run(
        history, workflow, workspace=tmp_path, tool_policy_version=1, policy=policy
    )
    if strict:
        assert first.outcome.kind == "failed"
        assert first.outcome.error.message == "blocked"
    else:
        assert first.outcome.kind == "completed"
        result = connect.ToolResultMessage.model_validate(first.outcome.result)
        assert result.is_error
        assert result.content == [connect.TextBlock(text="blocked")]
    second = await replay.run(
        history, workflow, first.id, workspace=tmp_path, tool_policy_version=1
    )
    assert second.outcome == first.outcome


@pytest.mark.parametrize(
    "settings",
    [
        'approve_all = "yes"',
        'whitelist = [""]',
        'whitelist = ["a", "a"]',
        "unknown = true",
    ],
)
def test_invalid_settings(settings: str, tmp_path: Path) -> None:
    import tomllib

    with pytest.raises(pydantic.ValidationError):
        tool_policy.ApprovalSettings.model_validate(tomllib.loads(settings))


async def test_registry_extensions_and_isolation(tmp_path: Path) -> None:
    policy = tool_policy.ToolPolicy()
    call = tool_policy.ToolCall(
        name="test",
        version=1,
        arguments={"value": 1},
        definition_digest="digest",
        workflow_id="workflow",
        step_id="step",
    )
    settings = tool_policy.ApprovalSettings()
    assert (await policy.evaluate(settings, call)).action == "require_confirmation"
    policy.allow("test")
    assert (await policy.evaluate(settings, call)).action == "allow"
    assert (
        await tool_policy.ToolPolicy().evaluate(settings, call)
    ).action == "require_confirmation"
    policy.remove("test")

    async def match(call: tool_policy.ToolCall) -> bool:
        return call.arguments["value"] == 1

    policy.register_matcher("value", match)
    assert (await policy.evaluate(settings, call)).action == "allow"
    with pytest.raises(ValueError, match="unique"):
        policy.register_matcher("value", match)


async def test_approve_all_does_not_bypass_validation(
    history: MemoryHistory, tmp_path: Path
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true"
    )

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("invalid calls must not execute")

    async def workflow() -> pydantic.JsonValue:
        result = await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute),
            connect.ToolCallBlock(id="a", name="test", arguments={"value": "bad"}),
        )
        assert result.is_error
        return None

    result = await replay.run(
        history, workflow, workspace=tmp_path, tool_policy_version=1
    )
    assert result.outcome.kind == "completed"


async def test_live_policy_changes_and_parallel_gate(
    history: MemoryHistory, tmp_path: Path
) -> None:
    config = tmp_path / "config.toml"
    config.write_text('[components.tools.approval]\nwhitelist = ["automatic"]')
    project = await RunnerProject.open(tmp_path)
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    automatic = asyncio.Event()
    executed: list[int] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        executed.append(args.value)
        if args.value == 2:
            automatic.set()
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def invoke(name: str, value: int) -> connect.ToolResultMessage:
        return await llm.invoke_tool(
            llm.Tool(name, name, Arguments, execute),
            connect.ToolCallBlock(id=str(value), name=name, arguments={"value": value}),
        )

    async def workflow() -> pydantic.JsonValue:
        results = await asyncio.gather(invoke("manual", 1), invoke("automatic", 2))
        assert all(not result.is_error for result in results)
        assert not (await invoke("manual", 3)).is_error
        return None

    task = asyncio.create_task(
        replay.run(
            history, workflow, project=project, mailbox=box, tool_policy_version=1
        )
    )
    await asyncio.wait_for(automatic.wait(), 2)
    await asyncio.wait_for(store.registered.wait(), 2)
    assert executed == [2]
    config.write_text("[components.tools.approval]\napprove_all = true")
    await project.configuration.reload()
    await approve(box, next(iter(store.requests.values())), {"approved": True})
    first = await asyncio.wait_for(task, 2)
    assert first.outcome.kind == "completed"
    assert executed == [2, 1, 3]
    assert len(store.requests) == 1
    assert (
        await replay.run(
            history, workflow, first.id, workspace=tmp_path, tool_policy_version=1
        )
    ).outcome == first.outcome
    assert executed == [2, 1, 3]


async def test_policy_exception_is_tool_error_but_storage_error_propagates(
    history: MemoryHistory, tmp_path: Path
) -> None:
    policy = tool_policy.ToolPolicy()

    async def broken_check(call: tool_policy.ToolCall) -> str:
        raise ValueError("check failed")

    policy.register_check("broken", broken_check)

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("failed gate must not execute")

    async def workflow() -> pydantic.JsonValue:
        return (
            await llm.invoke_tool(
                llm.Tool("test", "Test", Arguments, execute),
                connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
            )
        ).model_dump(mode="json")

    first = await replay.run(
        history, workflow, workspace=tmp_path, policy=policy, tool_policy_version=1
    )
    assert first.outcome.kind == "completed"
    result = connect.ToolResultMessage.model_validate(first.outcome.result)
    assert result.is_error and result.content == [
        connect.TextBlock(text="check failed")
    ]

    class BrokenStore(ApprovalStore):
        async def get_input(self, request_id: str) -> mailbox.InputRequest:
            raise OSError("storage failed")

    failed = await replay.run(
        MemoryHistory(),
        workflow,
        workspace=tmp_path,
        mailbox=mailbox.Mailbox(BrokenStore()),
        tool_policy_version=1,
    )
    assert failed.outcome.kind == "failed"
    assert failed.outcome.error.message == "storage failed"
