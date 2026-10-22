import asyncio
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

from tests.fixtures import cancellation_workflows as flows
from vectorops.runner import models
from vectorops.runner.runner import history_query
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner import session as session_models
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner import context, handlers, mailbox
from vectorops.runner.control import client, protocol
from tests.control.test_client_commands import pending, settled
from tests.control.test_paused import status


@pytest.fixture(autouse=True)
def reset() -> None:
    flows.started = asyncio.Queue()
    flows.release = asyncio.Event()
    flows.cleaned = []
    flows.calls = []
    flows.resource_cleanup = []
    flows.cleanup_gate = asyncio.Event()
    flows.cleanup_started = asyncio.Event()
    flows.owner_task = None
    flows.should_cancel = True


@pytest.mark.parametrize(
    "name,expected",
    [
        ("owned", "parent continues"),
        ("explicit", ["cancelled", "cancelled", "cancelled"]),
        ("observer", "completed"),
        ("group_failure", "caught"),
    ],
)
async def test_cancellation_and_completed_replay(
    workspace: Path, name: str, expected: object
) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(f"python:tests.fixtures.cancellation_workflows:{name}"), 5
        )
        == expected
    )
    assert flows.cleaned == ["child"]
    assert flows.resource_cleanup == ["child"]
    before = list(flows.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == expected
    assert flows.calls == before
    assert flows.cleaned == ["child"]
    execution_id = runner.last_execution_id
    assert execution_id is not None
    page = await runner.query_history(
        history_query.HistoryQuery(execution_id=execution_id, include_children=True)
    )
    cancelled = [
        item.step for item in page.items if item.step.outcome.kind == "cancelled"
    ]
    assert len(cancelled) == (0 if name == "observer" else 1)
    assert all(
        isinstance(step.operation, models.WorkflowCompleteOperation)
        for step in cancelled
    )


async def test_owned_cancellation_cascades(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(
                "python:tests.fixtures.cancellation_workflows:owned",
                input={"nested_child": True},
            ),
            5,
        )
        == "parent continues"
    )
    assert flows.cleaned == ["grandchild"]
    assert (
        await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5)
        == "parent continues"
    )
    assert flows.calls == ["grandchild"]


async def test_cancel_awaitable_handle_waiter_leaves_child_running(
    workspace: Path,
) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(
                "python:tests.fixtures.cancellation_workflows:observer",
                input={"await_handle": True},
            ),
            5,
        )
        == "completed"
    )
    assert flows.calls == ["child"]
    assert flows.cleaned == ["child"]
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "completed"
    assert flows.calls == ["child"]
    identifier = runner.last_execution_id
    assert identifier is not None
    page = await runner.query_history(
        history_query.HistoryQuery(execution_id=identifier, include_children=True)
    )
    assert not any(item.step.operation.kind == "workflow.cancel" for item in page.items)


@pytest.mark.parametrize("name", ["cancel_completed", "cancel_suppressed"])
async def test_cancellation_preserves_success(workspace: Path, name: str) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(f"python:tests.fixtures.cancellation_workflows:{name}"), 5
        )
        == "completed"
    )
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "completed"


async def test_resume_persisted_intent_without_restarting_child(
    workspace: Path,
) -> None:
    reached = asyncio.Event()
    original = ExecutionSession.cancel_child

    async def interrupted(
        self: ExecutionSession, workflow: models.Workflow, reason: Optional[str]
    ) -> models.WorkflowStep:
        reached.set()
        await asyncio.Event().wait()
        return await original(self, workflow, reason)

    runner = WorkflowRunner(workspace)
    with patch.object(ExecutionSession, "cancel_child", interrupted):
        task = asyncio.create_task(
            runner.run("python:tests.fixtures.cancellation_workflows:explicit")
        )
        await asyncio.wait_for(reached.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    calls = list(flows.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == [
        "cancelled",
        "cancelled",
        "cancelled",
    ]
    assert flows.calls == calls


async def test_repeated_cancel_waits_for_cleanup(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("python:tests.fixtures.cancellation_workflows:repeated")
    )
    await asyncio.wait_for(flows.cleanup_started.wait(), 5)
    assert flows.owner_task is not None
    flows.owner_task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    flows.cleanup_gate.set()
    assert await asyncio.wait_for(task, 5) == "done"
    assert flows.cleaned == ["slow"]
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "done"


async def test_cancel_during_spawn_delivery(workspace: Path) -> None:
    original = handlers.SpawnHandler.execute

    async def cancel_owner(
        self: handlers.SpawnHandler, request: context.SpawnRequest
    ) -> context.SpawnResponse:
        response = await original(self, request)
        assert flows.owner_task is not None
        flows.owner_task.cancel()
        return response

    runner = WorkflowRunner(workspace)
    with patch.object(handlers.SpawnHandler, "execute", cancel_owner):
        assert (
            await asyncio.wait_for(
                runner.run("python:tests.fixtures.cancellation_workflows:spawn_race"), 5
            )
            == "done"
        )
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "done"


async def test_child_cancellation_keeps_parent_input_available(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("python:tests.fixtures.cancellation_workflows:mailbox_parent")
    )
    async with asyncio.timeout(5):
        while runner.last_execution_id is None:
            await asyncio.sleep(0.01)
        identifier = runner.last_execution_id
        while True:
            page = await runner.pending_inputs(
                mailbox.RequestQuery(execution_id=identifier)
            )
            if len(page.requests) == 2:
                break
            await asyncio.sleep(0.01)
        child = next(item for item in page.requests if item.channel == "child")
        parent = next(item for item in page.requests if item.channel == "parent")
        await runner.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=parent.request_id,
                response="go",
                idempotency_key="one",
            )
        )
        while True:
            page = await runner.pending_inputs(
                mailbox.RequestQuery(execution_id=identifier)
            )
            if (
                len(page.requests) == 1
                and page.requests[0].channel == "parent"
                and page.requests[0].request_id != parent.request_id
            ):
                break
            await asyncio.sleep(0.01)
        assert page.requests[0].channel == "parent"
        assert (
            await runner.input_request(
                mailbox.RequestRef(execution_id=identifier, request_id=child.request_id)
            )
        ).status == "cancelled"
        await runner.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=page.requests[0].request_id,
                response="done",
                idempotency_key="two",
            )
        )
        assert await task == "cancelled"
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "cancelled"


async def test_legacy_decorated_cancellation_does_not_add_intent(
    workspace: Path,
) -> None:
    original = WorkflowExecutor.configure

    async def legacy(
        self: WorkflowExecutor, configuration: models.Workflow
    ) -> models.Workflow:
        return (await original(self, configuration)).model_copy(
            update={"child_cancellation_version": None}
        )

    runner = WorkflowRunner(workspace)
    with patch.object(WorkflowExecutor, "configure", legacy):
        assert (
            await asyncio.wait_for(
                runner.run("python:tests.fixtures.cancellation_workflows:owned"), 5
            )
            == "parent continues"
        )
    assert (
        await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5)
        == "parent continues"
    )
    identifier = runner.last_execution_id
    assert identifier is not None
    page = await runner.query_history(
        history_query.HistoryQuery(execution_id=identifier, include_children=True)
    )
    assert not any(item.step.operation.kind == "workflow.cancel" for item in page.items)


async def test_owned_cancellation_leaves_sibling_running(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run("python:tests.fixtures.cancellation_workflows:sibling"), 5
        )
        == "remaining"
    )
    assert sorted(flows.cleaned) == ["cancelled", "remaining"]
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "remaining"
    assert sorted(flows.calls) == ["cancelled", "remaining"]


async def test_shutdown_suspends_owned_child_without_cancelling(
    workspace: Path,
) -> None:
    async with client.embedded(workspace) as host:
        accepted = await host.start(
            protocol.Start(
                reference="python:tests.fixtures.cancellation_workflows:suspended",
                idempotency_key="start",
            )
        )
        request = await pending(host, accepted.execution_id)
        await status(host, accepted.execution_id, "paused")
    async with client.embedded(workspace) as host:
        restored = await pending(host, accepted.execution_id)
        assert restored.request_id == request.request_id
        await host.answer_input(
            mailbox.Answer(
                execution_id=accepted.execution_id,
                request_id=request.request_id,
                response="answer",
                idempotency_key="answer",
            )
        )
        await settled(host, accepted.execution_id)
        page = await host.history(
            history_query.HistoryQuery(
                execution_id=accepted.execution_id, include_children=True
            )
        )
        assert not any(
            item.step.operation.kind == "workflow.cancel"
            or item.step.outcome.kind == "cancelled"
            for item in page.items
        )


async def test_replay_injects_owned_cancellation(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(
                "python:tests.fixtures.cancellation_workflows:replay_cancellation"
            ),
            5,
        )
        == "cancelled"
    )
    flows.should_cancel = False
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "cancelled"


async def test_successful_child_can_have_cancelled_invocation(workspace: Path) -> None:
    original = ExecutionSession._deliver

    async def cancel_before_delivery(
        self: ExecutionSession,
        invocation: session_models._Invocation,
        *,
        replay: bool = False,
    ) -> None:
        if (
            flows.owner_task is not None
            and not replay
            and isinstance(invocation.request, context.JoinRequest)
        ):
            flows.owner_task.cancel()
        await original(self, invocation, replay=replay)

    runner = WorkflowRunner(workspace)
    with patch.object(ExecutionSession, "_deliver", cancel_before_delivery):
        assert (
            await asyncio.wait_for(
                runner.run(
                    "python:tests.fixtures.cancellation_workflows:cancelled_success"
                ),
                5,
            )
            == "cancelled"
        )
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == "cancelled"
