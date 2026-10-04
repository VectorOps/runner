import asyncio
from pathlib import Path

from vectorops.runner import diagnostics as d
from vectorops.runner.runner.workflow import WorkflowRunner


def records(workspace: Path) -> list[d.LogRecord]:
    return [
        d.LogRecord.model_validate_json(line)
        for path in (workspace / "data" / "logs").glob("*.jsonl")
        for line in path.read_bytes().splitlines()
    ]


async def test_parallel_activity_correlation_and_replay(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("diagnostics:main")
    entries = records(workspace)
    bodies = [entry for entry in entries if entry.event == "activity.body"]
    assert len(bodies) == 6
    assert len({entry.workflow_id for entry in bodies}) == 2
    assert len({entry.step_id for entry in bodies}) == 4
    for entry in bodies:
        assert (
            entry.execution_id
            == entry.fields["expected_execution"]
            == runner.last_execution_id
        )
        assert entry.workflow_id == entry.fields["expected_workflow"]
        assert entry.step_id == entry.fields["expected_step"]
    await runner.resume()
    assert (
        len([entry for entry in records(workspace) if entry.event == "activity.body"])
        == 6
    )
    assert "private-snapshot" not in "".join(
        entry.model_dump_json() for entry in records(workspace)
    )


async def test_cancellation_drains_workflow_logs(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("diagnostics:wait"))
    async with asyncio.timeout(5):
        while not any(
            entry.event == "waiting"
            for entry in await asyncio.to_thread(records, workspace)
        ):
            await asyncio.sleep(0.01)
    runner.stop()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert any(
        entry.event == "workflow.cancelled"
        and entry.execution_id == runner.last_execution_id
        for entry in records(workspace)
    )
