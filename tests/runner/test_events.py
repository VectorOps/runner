from pathlib import Path

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner.runner.events import StepEvent
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_activity_events_are_snapshots_of_same_step(
    workspace: Path, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    events: list[StepEvent] = []
    runner = WorkflowRunner(workspace, history, event_sink=events.append)
    assert await runner.run("activities:workflow", workflow_id="root") == 2
    first, completed = events[:2]
    assert first.kind == "step.created"
    assert first.step.outcome.kind == "pending"
    assert completed.kind == "step.completed"
    assert completed.step.id == first.step.id
    assert completed.step.outcome.kind == "completed"
    assert StepEvent.model_validate_json(first.model_dump_json()) == first
    before = list(events)
    assert (
        await runner.run(
            "activities:workflow", continue_from_step_id=history.heads["root"]
        )
        == 2
    )
    assert events == before


async def test_nested_stream_updates_do_not_become_workflow_results(
    workspace: Path, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    events: list[StepEvent] = []
    runner = WorkflowRunner(workspace, history, event_sink=events.append)
    assert await runner.run("composed:streamed", workflow_id="root") == 7
    stream_events = [
        event
        for event in events
        if event.step.operation.model_dump().get("name") == "generate"
    ]
    assert len({event.step.id for event in stream_events}) == 1
    assert all(event.step.workflow_id != "root" for event in stream_events)
    assert [event.kind for event in stream_events] == [
        "step.created",
        "step.updated",
        "step.updated",
        "step.updated",
        "step.completed",
        "step.completed",
    ]
    snapshots = [
        event.step.outcome.model_dump().get("state")
        for event in stream_events
        if event.kind == "step.updated"
    ]
    assert snapshots == [{"text": "partial"}, {"text": "complete"}, 7]
    before = list(events)
    assert (
        await runner.run(
            "composed:streamed", continue_from_step_id=history.heads["root"]
        )
        == 7
    )
    assert events == before


async def test_persistent_events_include_execution_identity(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    events: list[StepEvent] = []
    runner = WorkflowRunner(workspace, event_sink=events.append)
    await runner.run("composed:streamed")
    assert events
    assert all(event.execution_id == runner.last_execution_id for event in events)


async def test_sink_failures_are_observational(
    workspace: Path,
    history: MemoryHistory,
    execution_probe: ExecutionProbe,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def sink(event: StepEvent) -> None:
        raise ValueError("listener failed")

    assert (
        await WorkflowRunner(workspace, history, event_sink=sink).run(
            "activities:workflow"
        )
        == 2
    )
    assert "Step event sink failed" in caplog.text


async def test_failed_storage_does_not_emit_unaccepted_events(
    workspace: Path, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    events: list[StepEvent] = []
    history.fail_append = True
    with pytest.raises(OSError):
        await WorkflowRunner(workspace, history, event_sink=events.append).run(
            "activities:workflow"
        )
    assert events == []
