import asyncio
from pathlib import Path
from typing import Optional

import connect
import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import input_schema, models, tool_policy
from vectorops.runner.api import composition, llm
from vectorops.runner.api.llm.patch import apply_patch_tool
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError


class Arguments(pydantic.BaseModel):
    value: int


class ApprovalStore:
    def __init__(self) -> None:
        self.requests: dict[str, mailbox.InputRequest] = {}
        self.registered = asyncio.Event()

    async def register_input(
        self, request_id: str, spec: mailbox.InputSpec
    ) -> mailbox.InputRequest:
        if request_id not in self.requests:
            self.requests[request_id] = mailbox.InputRequest(
                request_id=request_id,
                execution_id="test",
                workflow_id="test",
                **spec.model_dump(),
            )
        else:
            assert self.requests[request_id].payload == spec.payload
        self.registered.set()
        return self.requests[request_id]

    async def get_input(self, request_id: str) -> mailbox.InputRequest:
        return self.requests[request_id]

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool:
        return any(
            request.workflow_id == workflow_id
            and request.status == "answered"
            and tool_policy.workflow_grant(request.payload, request.response)
            == tool_name
            for request in self.requests.values()
        )

    async def answer_input(self, answer: mailbox.Answer) -> mailbox.InputRequest:
        request = self.requests[answer.request_id]
        request.validate_response(answer.response)
        updated = request.model_copy(
            update={"status": "answered", "response": answer.response}
        )
        self.requests[answer.request_id] = updated
        return updated

    async def cancel_inputs(self, request_id: Optional[str] = None) -> None:
        raise AssertionError("unexpected cancellation of durable approval")


async def approve(
    box: mailbox.Mailbox, request: mailbox.InputRequest, value: pydantic.JsonValue
) -> None:
    await box.answer(
        mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response=value,
            idempotency_key=request.request_id,
        )
    )


@pytest.mark.parametrize("approved", [True, False])
async def test_patch_tool_approval_and_replay(
    history: MemoryHistory, tmp_path: Path, approved: bool
) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    binding = apply_patch_tool(PatchService(environment), approval=llm.ToolApproval())
    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def workflow() -> pydantic.JsonValue:
        result = await llm.invoke_tool(
            binding,
            connect.ToolCallBlock(
                id="patch",
                name="apply_patch",
                arguments={
                    "text": "*** Begin Patch\n*** Add File: file\n+value\n*** End Patch"
                },
            ),
        )
        return result.model_dump(mode="json")

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    assert not (tmp_path / "file").exists()
    await approve(box, next(iter(store.requests.values())), {"approved": approved})
    first = await task
    assert first.outcome.kind == "completed"
    response = connect.ToolResultMessage.model_validate(first.outcome.result)
    assert response.is_error is not approved
    assert (tmp_path / "file").exists() is approved
    if approved:
        (tmp_path / "file").unlink()
    second = await replay.run(history, workflow, first.id)
    assert second.outcome == first.outcome
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize("approved", [True, False])
async def test_standalone_tool_approval_and_replay(
    history: MemoryHistory, approved: bool
) -> None:
    calls: list[int] = []
    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        assert composition._CURRENT_CONTEXT.get() is None
        calls.append(args.value)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    tool = llm.Tool("test", "Test", Arguments, execute, approval=llm.ToolApproval())

    async def workflow() -> pydantic.JsonValue:
        result = await llm.invoke_tool(
            tool, connect.ToolCallBlock(id="call", name="test", arguments={"value": 1})
        )
        return result.model_dump(mode="json")

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    assert calls == [] and not task.done()
    request = next(iter(store.requests.values()))
    assert request.channel == "tool.approval"
    assert request.response_type == "json"
    assert request.request_type == "tool.approval.v1"
    assert request.request_schema is not None
    assert request.response_schema is not None
    assert isinstance(request.payload, dict)
    assert isinstance(request.payload["call"], dict)
    assert request.payload["call"]["arguments"] == {"value": 1}
    await approve(box, request, {"approved": approved, "reason": "host decision"})
    first = await task
    assert first.outcome.kind == "completed"
    result = connect.ToolResultMessage.model_validate(first.outcome.result)
    assert result.is_error is not approved
    if not approved:
        assert result.content[0].type == "text"
        assert result.content[0].text == "host decision"
    second = await replay.run(history, workflow, first.id)
    assert second.outcome == first.outcome
    assert calls == ([1] if approved else [])
    assert len(store.requests) == 1


@pytest.mark.parametrize("value", ["invalid", None])
async def test_validation_happens_before_approval(
    history: MemoryHistory, value: pydantic.JsonValue
) -> None:
    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("invalid arguments must not execute")

    async def workflow() -> pydantic.JsonValue:
        result = await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute, approval=llm.ToolApproval()),
            connect.ToolCallBlock(id="a", name="test", arguments={"value": value}),
        )
        assert result.is_error
        return None

    result = await replay.run(history, workflow)
    assert result.outcome.kind == "completed"
    assert all(
        step.operation.kind != "mailbox.input" for step in history.path(result.id)
    )


async def test_parallel_approval_does_not_block_automatic_tool(
    history: MemoryHistory,
) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    executed: list[int] = []
    automatic_finished = asyncio.Event()

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        executed.append(args.value)
        if args.value == 2:
            automatic_finished.set()
        return llm.ToolResponse(content=[connect.TextBlock(text=str(args.value))])

    async def workflow() -> pydantic.JsonValue:
        results = await asyncio.gather(
            *(
                llm.invoke_tool(
                    llm.Tool(
                        "test",
                        "Test",
                        Arguments,
                        execute,
                        approval=llm.ToolApproval() if value == 1 else None,
                    ),
                    connect.ToolCallBlock(
                        id=str(value), name="test", arguments={"value": value}
                    ),
                )
                for value in (1, 2)
            )
        )
        assert [result.tool_call_id for result in results] == ["1", "2"]
        return None

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(automatic_finished.wait(), 2)
    await asyncio.wait_for(store.registered.wait(), 2)
    assert executed == [2]
    await approve(box, next(iter(store.requests.values())), {"approved": True})
    first = await task
    assert first.outcome.kind == "completed"
    await replay.run(history, workflow, first.id)
    assert executed == [2, 1]


async def test_pending_approval_reattaches_after_interruption(
    history: MemoryHistory,
) -> None:
    store = ApprovalStore()
    calls: list[str] = []

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        calls.append(ctx.step_id)
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        result = await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute, approval=llm.ToolApproval()),
            connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
        )
        return result.model_dump(mode="json")

    original_box = mailbox.Mailbox(store)
    task = asyncio.create_task(replay.run(history, workflow, mailbox=original_box))
    await asyncio.wait_for(store.registered.wait(), 2)
    request = next(iter(store.requests.values()))
    original_box.suspending = True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    box = mailbox.Mailbox(store)
    await approve(box, request, {"approved": True})
    result = await replay.run(history, workflow, request.request_id, mailbox=box)
    assert result.outcome.kind == "completed", result.outcome
    assert len(calls) == 1 and len(store.requests) == 1


@pytest.mark.parametrize(
    "answer",
    [
        {"approved": "yes"},
        {},
        {"approved": True, "extra": 1},
        {"approved": False, "scope": "workflow"},
        {"approved": True, "scope": "all"},
    ],
)
async def test_invalid_approval_fails_closed(
    history: MemoryHistory, answer: dict[str, pydantic.JsonValue]
) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        raise AssertionError("malformed approval must never execute")

    async def workflow() -> pydantic.JsonValue:
        response = await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute, approval=llm.ToolApproval()),
            connect.ToolCallBlock(id="a", name="test", arguments={"value": 1}),
        )
        assert response.is_error
        return None

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    request = next(iter(store.requests.values()))
    with pytest.raises(input_schema.InputValidationError):
        await approve(box, request, answer)
    assert store.requests[request.request_id].status == "pending"
    assert not task.done()
    await approve(box, request, {"approved": False})
    result = await task
    assert result.outcome.kind == "completed"
    assert (await replay.run(history, workflow, result.id)).outcome.kind == "completed"


async def test_changed_tool_arguments_reject_replay(history: MemoryHistory) -> None:
    value = 1

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> llm.ToolResponse:
        return llm.ToolResponse(content=[connect.TextBlock(text="done")])

    async def workflow() -> pydantic.JsonValue:
        await llm.invoke_tool(
            llm.Tool("test", "Test", Arguments, execute),
            connect.ToolCallBlock(id="a", name="test", arguments={"value": value}),
        )
        return None

    first = await replay.run(history, workflow)
    assert first.outcome.kind == "completed"
    value = 2
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id)
