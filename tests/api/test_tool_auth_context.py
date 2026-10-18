import asyncio
from typing import Literal, Optional
from unittest.mock import AsyncMock
from uuid import uuid4

import connect
import pydantic
import pytest

from tests.api.test_tools import ApprovalStore, approve
from tests.api.test_workflow_tool_results import WorkflowTool
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import tool_policy
from vectorops.runner.tool_authorization import ToolAuthorizationService
from vectorops.runner.api import tool_contracts as tools, tool_registry
from vectorops.runner.api.tool_execution import invoke_tool
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext


class Arguments(pydantic.BaseModel):
    path: str


async def execute(ctx: ActivityExecutionContext, args: Arguments) -> tools.ToolResponse:
    return tools.ToolResponse(content=[connect.TextBlock(text=args.path)])


def call() -> connect.ToolCallBlock:
    return connect.ToolCallBlock(id="call", name="read", arguments={"path": "file"})


def test_protocol_defaults_and_json_roundtrip() -> None:
    binding = tools.Tool("read", "Read", Arguments, execute)
    request = tool_policy.ToolApprovalRequest(
        call=call(), tool=binding.definition, tool_version=1
    )
    assert request.auth_context == tools.AuthContext(properties={})
    legacy = request.model_dump(exclude={"auth_context"})
    assert tool_policy.ToolApprovalRequest.model_validate(legacy) == request
    context = tools.AuthContext(
        properties={"scope": "internal", "nested": [1, True, None]}
    )
    enriched = request.model_copy(update={"auth_context": context})
    spec = enriched.input_spec()
    assert spec.request_type == "tool.approval.v1"
    assert tool_policy.ToolApprovalRequest.model_validate(spec.payload) == enriched
    assert (
        tool_policy.ToolApprovalRequest.model_validate_json(enriched.model_dump_json())
        == enriched
    )
    context.properties["changed"] = True
    assert request.auth_context.properties == {}
    with pytest.raises(pydantic.ValidationError):
        tools.AuthContext.model_validate({"properties": {"invalid": object()}})


@pytest.mark.parametrize("legacy", [False, True])
async def test_decorator_metadata_is_durable_and_not_recomputed(
    history: MemoryHistory, legacy: bool
) -> None:
    calls = 0

    async def auth(arguments: dict[str, pydantic.JsonValue]) -> tools.AuthContext:
        nonlocal calls
        calls += 1
        assert arguments == {"path": "file"}
        arguments["path"] = "must not affect execution"
        return tools.AuthContext(properties={"scope": "internal"})

    @tool_registry.register_tool("auth_" + uuid4().hex, auth_context=auth)
    def factory() -> tools.ToolBinding:
        return tools.Tool(
            "read", "Read", Arguments, execute, approval=tools.ToolApproval()
        )

    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def workflow() -> pydantic.JsonValue:
        return (await invoke_tool(factory(), call())).model_dump(mode="json")

    version: Optional[Literal[1]] = None if legacy else 1
    task = asyncio.create_task(
        replay.run(history, workflow, mailbox=box, tool_policy_version=version)
    )
    await asyncio.wait_for(store.registered.wait(), 2)
    request = next(iter(store.requests.values()))
    payload = tool_policy.ToolApprovalRequest.model_validate(request.payload)
    assert payload.auth_context.properties == {"scope": "internal"}
    box.suspending = True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await approve(box, request, {"approved": True})
    result = await replay.run(
        history, workflow, request.request_id, mailbox=box, tool_policy_version=version
    )
    assert result.outcome.kind == "completed"
    response = connect.ToolResultMessage.model_validate(result.outcome.result)
    assert response.content == [connect.TextBlock(text="file")]
    assert calls == 1
    await replay.run(
        history, workflow, result.id, mailbox=box, tool_policy_version=version
    )
    assert calls == 1


async def test_typed_context_reaches_policy(history: MemoryHistory) -> None:
    async def auth(args: Arguments) -> tools.AuthContext:
        assert isinstance(args, Arguments)
        return tools.AuthContext(properties={"scope": "internal"})

    async def matcher(request: tool_policy.ToolCall) -> bool:
        return request.auth_context.properties == {"scope": "internal"}

    policy = tool_policy.ToolPolicy()
    policy.register_matcher("internal", matcher)
    binding = tools.Tool("read", "Read", Arguments, execute, auth_context=auth)

    async def workflow() -> pydantic.JsonValue:
        response = await invoke_tool(binding, call())
        assert not response.is_error
        return None

    result = await replay.run(history, workflow, policy=policy, tool_policy_version=1)
    assert result.outcome.kind == "completed"


@pytest.mark.parametrize("invalid_arguments", [False, True])
async def test_callback_errors_fail_closed_after_validation(
    history: MemoryHistory, invalid_arguments: bool
) -> None:
    calls = 0

    async def auth(args: Arguments) -> tools.AuthContext:
        nonlocal calls
        calls += 1
        raise ValueError("metadata unavailable")

    async def forbidden(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        raise AssertionError("must not execute")

    binding = tools.Tool("read", "Read", Arguments, forbidden, auth_context=auth)
    policy = tool_policy.ToolPolicy()
    policy.allow("read")

    async def workflow() -> pydantic.JsonValue:
        request = call()
        if invalid_arguments:
            request = request.model_copy(update={"arguments": {}})
        response = await invoke_tool(binding, request)
        assert response.is_error
        return None

    result = await replay.run(history, workflow, policy=policy, tool_policy_version=1)
    assert result.outcome.kind == "completed"
    assert calls == (0 if invalid_arguments else 1)


async def test_registry_build_supports_async_factories_and_lists() -> None:
    async def auth(arguments: dict[str, pydantic.JsonValue]) -> tools.AuthContext:
        return tools.AuthContext(properties={"scope": "internal"})

    name = "auth_" + uuid4().hex

    @tool_registry.register_tool(name, auth_context=auth)
    async def factory() -> list[tools.ToolBinding]:
        return [tools.Tool("read", "Read", Arguments, execute)]

    direct = await factory()
    built = await tool_registry.registrations()[name].build(tool_registry.ToolConfig())
    for binding in [*direct, *built]:
        context = await ToolAuthorizationService.resolve_auth_context(binding, call())
        assert context.properties == {"scope": "internal"}


async def test_decorator_preserves_workflow_binding_capability() -> None:
    async def auth(arguments: dict[str, pydantic.JsonValue]) -> tools.AuthContext:
        return tools.AuthContext(properties={"scope": "child"})

    @tool_registry.register_tool("auth_" + uuid4().hex, auth_context=auth)
    def factory() -> tools.WorkflowToolBinding:
        return WorkflowTool()

    binding = factory()
    assert isinstance(binding, tools.WorkflowToolBinding)
    result = await ToolAuthorizationService.resolve_auth_context(binding, call())
    assert result.properties == {"scope": "child"}


async def test_missing_callback_and_invalid_result() -> None:
    binding = tools.Tool("read", "Read", Arguments, execute)
    assert (
        await ToolAuthorizationService.resolve_auth_context(binding, call())
        == tools.AuthContext()
    )
    assert (
        await ToolAuthorizationService.resolve_auth_context(WorkflowTool(), call())
        == tools.AuthContext()
    )

    invalid = tools.Tool(
        "read", "Read", Arguments, execute, auth_context=AsyncMock(return_value=None)
    )
    with pytest.raises(TypeError, match="must return AuthContext"):
        await ToolAuthorizationService.resolve_auth_context(invalid, call())


async def test_callback_cancellation_propagates(history: MemoryHistory) -> None:
    async def auth(args: Arguments) -> tools.AuthContext:
        raise asyncio.CancelledError

    binding = tools.Tool("read", "Read", Arguments, execute, auth_context=auth)

    async def workflow() -> pydantic.JsonValue:
        await invoke_tool(binding, call())
        return None

    with pytest.raises(asyncio.CancelledError):
        await replay.run(history, workflow, tool_policy_version=1)
