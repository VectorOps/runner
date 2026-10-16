import asyncio
from collections.abc import Callable
from pathlib import Path
import secrets
from unittest.mock import AsyncMock, patch

import connect
import pydantic
import pytest

from tests.runner.test_mailbox import answer, store
from tests.runner.test_usage import generation, seed, spawn
from vectorops.runner import models
from vectorops.runner.api import chat_input, user_input
from vectorops.runner.control import client, protocol as p
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.control.updates import SnapshotPublisher
from vectorops.runner.runner import events, inspection, mailbox
from vectorops.runner.runner.llm import RunnerLLM
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def receive(
    connection: client.RunnerClient,
    matches: Callable[[dict[str, pydantic.JsonValue]], bool],
) -> dict[str, pydantic.JsonValue]:
    async with asyncio.timeout(5):
        while True:
            packet = await connection.next_event()
            if matches(packet.event):
                return packet.event


async def test_committed_name_and_usage_push_full_snapshots(tmp_path: Path) -> None:
    async with RunnerService(tmp_path) as service:
        token = secrets.token_urlsafe(32)
        server = RunnerServer(service, token)
        try:
            async with await client.RunnerClient.tcp(
                await server.listen(), token
            ) as connection:
                await connection.subscribe(
                    p.Subscribe(
                        execution_id="execution",
                        kinds=["workflow.updated", "execution.updated"],
                    )
                )
                project = await service.runner.project()
                database = SQLiteHistory(
                    project.data_path / "executions/execution/history.sqlite3",
                    change_sink=lambda ids: service._changed("execution", ids),
                )
                await seed(database)
                await spawn(database, "root", "child")
                received: list[dict[str, pydantic.JsonValue]] = []

                async def named_snapshots() -> None:
                    workflow_seen = False
                    summary_seen = False
                    async with asyncio.timeout(5):
                        while not (workflow_seen and summary_seen):
                            event = (await connection.next_event()).event
                            received.append(event)
                            if event.get("kind") == "workflow.updated":
                                workflow_seen |= (
                                    events.WorkflowUpdated.model_validate(
                                        event
                                    ).workflow.name
                                    == "A pushed title"
                                )
                            elif event.get("kind") == "execution.updated":
                                summary_seen |= (
                                    events.ExecutionUpdated.model_validate(
                                        event
                                    ).summary.name
                                    == "A pushed title"
                                )

                await database.set_workflow_name("root", "A pushed title")
                await named_snapshots()
                pushed = events.WorkflowUpdated.model_validate(
                    next(
                        event
                        for event in reversed(received)
                        if event.get("kind") == "workflow.updated"
                        and events.WorkflowUpdated.model_validate(event).workflow.name
                        == "A pushed title"
                    )
                )
                assert pushed.workflow == await database.get_workflow("root")
                summary = events.ExecutionUpdated.model_validate(
                    next(
                        event
                        for event in reversed(received)
                        if event.get("kind") == "execution.updated"
                        and events.ExecutionUpdated.model_validate(event).summary.name
                        == "A pushed title"
                    )
                )
                assert summary.summary.name == "A pushed title"
                await database.append_step(generation("generation", "child"))
                seen: dict[str, models.Workflow] = {}
                accounted = False
                async with asyncio.timeout(5):
                    while len(seen) < 2 or not accounted:
                        event = (await connection.next_event()).event
                        if event.get("kind") == "workflow.updated":
                            workflow = events.WorkflowUpdated.model_validate(
                                event
                            ).workflow
                            if (
                                workflow.usage is not None
                                and workflow.usage.subtree.cost_estimate_usd > 0
                            ):
                                seen[workflow.id] = workflow
                        elif event.get("kind") == "execution.updated":
                            summary = events.ExecutionUpdated.model_validate(event)
                            accounted = (
                                summary.summary.usage is not None
                                and summary.summary.usage.totals.input_tokens == 100
                            )
                assert set(seen) == {"root", "child"}
                assert summary.summary.usage is not None
                assert summary.summary.usage.totals.input_tokens == 100
                assert summary.summary == await connection.execution_summary(
                    "execution"
                )
        finally:
            await server.aclose()


async def test_mailbox_changes_push_without_step_events(tmp_path: Path) -> None:
    await store(tmp_path / "data/executions/execution/history.sqlite3")
    with patch.object(RunnerService, "_schedule"):
        service = await RunnerService(tmp_path).__aenter__()
    try:
        captured: asyncio.Queue[dict[str, pydantic.JsonValue]] = asyncio.Queue()
        service.listen(captured.put_nowait)
        project = await service.runner.project()
        database = await service.runner._database(project, "execution")
        await database.register_input("request", mailbox.InputSpec(channel="approval"))
        async with asyncio.timeout(5):
            while True:
                event = await captured.get()
                if event.get("kind") == "execution.updated":
                    snapshot = events.ExecutionUpdated.model_validate(event)
                    if snapshot.summary.pending_inputs == 1:
                        break
        await database.answer_input(answer())
        async with asyncio.timeout(5):
            while True:
                event = await captured.get()
                if event.get("kind") == "execution.updated":
                    snapshot = events.ExecutionUpdated.model_validate(event)
                    if snapshot.summary.pending_inputs == 0:
                        break
    finally:
        await service.aclose()


async def test_generated_name_pushes_through_runner_without_new_steps(
    tmp_path: Path,
) -> None:
    database = await store(tmp_path / "data/executions/execution/history.sqlite3")
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    with patch.object(RunnerService, "_schedule"):
        service = await RunnerService(tmp_path).__aenter__()
    try:
        captured: asyncio.Queue[dict[str, pydantic.JsonValue]] = asyncio.Queue()
        service.listen(captured.put_nowait)
        generated = connect.AssistantMessage(
            content=[connect.TextBlock(text="Generated title")], finish_reason="stop"
        )
        with patch.object(RunnerLLM, "generate", new=AsyncMock(return_value=generated)):
            await service.runner.answer_input(
                answer({"content": [{"type": "text", "text": "Hello"}]})
            )
        named = False
        summarized = False
        async with asyncio.timeout(5):
            while not (named and summarized):
                event = await captured.get()
                if event.get("kind") == "workflow.updated":
                    snapshot = events.WorkflowUpdated.model_validate(event)
                    if snapshot.workflow.name == "Generated title":
                        named = True
                elif event.get("kind") == "execution.updated":
                    summary = events.ExecutionUpdated.model_validate(event)
                    if summary.summary.name == "Generated title":
                        summarized = True
        assert (await database.inspect_execution()).stats.steps == 1
    finally:
        await service.aclose()


async def test_publisher_coalesces_deduplicates_and_drains() -> None:
    current = inspection.ExecutionSummary(execution_id="execution", status="running")
    workflow = models.Workflow(id="root", definition_ref="test:main")
    reads = 0
    entered = asyncio.Event()
    release = asyncio.Event()
    pushed: list[dict[str, pydantic.JsonValue]] = []

    async def summary(identifier: str) -> inspection.ExecutionSummary:
        nonlocal reads
        reads += 1
        snapshot = current
        if reads == 1:
            entered.set()
            await release.wait()
        return snapshot

    async def state(identifier: str, workflow_id: str) -> models.Workflow:
        return workflow

    publisher = SnapshotPublisher(summary, state, pushed.append)
    publisher.changed("execution", ("root",))
    await entered.wait()
    current = current.model_copy(update={"status": "completed"})
    for _ in range(10):
        publisher.changed("execution", ("root",))
    release.set()
    await publisher.close()
    assert reads == 2
    assert [event["kind"] for event in pushed] == [
        "workflow.updated",
        "execution.updated",
        "execution.updated",
    ]
    assert (
        events.ExecutionUpdated.model_validate(pushed[-1]).summary.status == "completed"
    )


async def test_terminal_push_reflects_released_ownership(workspace: Path) -> None:
    (workspace / "workflows/push.py").write_text(
        "async def main(ctx):\n    return 'done'\n"
    )
    async with client.embedded(workspace) as connection:
        await connection.subscribe(p.Subscribe(kinds=["execution.updated"]))
        accepted = await connection.start(
            p.Start(reference="push:main", idempotency_key="start")
        )
        async with asyncio.timeout(5):
            while True:
                packet = events.ExecutionUpdated.model_validate(
                    (await connection.next_event()).event
                )
                if (
                    packet.execution_id == accepted.execution_id
                    and packet.summary.status == "completed"
                    and not packet.summary.active
                ):
                    break
        assert packet.summary == await connection.execution_summary(
            accepted.execution_id
        )
