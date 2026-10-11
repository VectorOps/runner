import asyncio
import json
from pathlib import Path
import sys
from unittest.mock import AsyncMock, patch

import pytest

from tests.fixtures import sources
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_legacy_resume_history_still_replays(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert await runner.run("activities:workflow", workflow_id="root") == 2
    original = history.path(history.heads["root"])
    legacy: list[models.WorkflowStep] = []
    predecessor = None
    for step in original:
        legacy.append(
            step.model_copy(update={"previous_step_id": predecessor, "deliveries": []})
        )
        predecessor = step.id
        for index, target in enumerate(step.deliveries):
            marker = models.WorkflowStep(
                id=f"{step.id}-resume-{index}",
                workflow_id="root",
                previous_step_id=predecessor,
                operation=models.WorkflowResumeOperation(step_id=target),
                outcome=models.StepCompleted(),
            )
            legacy.append(marker)
            predecessor = marker.id
    history.replay_override = legacy
    appended = list(history.appended)
    assert (
        await runner.run("activities:workflow", continue_from_step_id=original[-1].id)
        == 2
    )
    assert history.appended == appended


async def test_delivery_failure_never_wakes_workflow(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    with patch.object(
        history, "record_delivery", AsyncMock(side_effect=OSError("delivery write"))
    ):
        with pytest.raises(OSError, match="delivery write"):
            await runner.run("activities:workflow", workflow_id="root")
    path = history.path(history.heads["root"])
    assert len(path) == 1
    assert path[0].outcome.kind == "completed"
    assert not path[0].deliveries
    assert (
        await runner.run("activities:workflow", continue_from_step_id=path[0].id) == 2
    )
    assert all(
        step.operation.kind != "workflow.resume" for step in history.steps.values()
    )


@pytest.mark.parametrize("target", ["missing", "future"])
async def test_invalid_delivery_references_fail_replay(
    runner: WorkflowRunner, history: MemoryHistory, target: str
) -> None:
    await runner.run("activities:workflow", workflow_id="root")
    path = history.path(history.heads["root"])
    reference = path[1].id if target == "future" else target
    history.replay_override = [
        path[0].model_copy(update={"deliveries": [reference]}),
        *path[1:],
    ]
    with pytest.raises(ReplayMismatchError, match="inactive invocation"):
        await runner.run("activities:workflow", continue_from_step_id=path[-1].id)


async def test_delivery_metadata_survives_pending_anchor_updates(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="example:main")
    )
    first = models.WorkflowStep(
        id="first",
        workflow_id="root",
        deferred_delivery=True,
        operation=models.StepOperation(kind="test", name="first"),
        outcome=models.StepCompleted(result=1),
    )
    anchor = models.WorkflowStep(
        id="anchor",
        workflow_id="root",
        previous_step_id="first",
        deferred_delivery=True,
        operation=models.StepOperation(kind="test", name="anchor"),
        outcome=models.StepPending(),
    )
    await database.append_step(first)
    await database.append_step(anchor)
    async with CachedHistory(database) as history:
        delivered = await history.record_delivery("anchor", "first")
        assert delivered.deliveries == ["first"]
        assert delivered.outcome.kind == "pending"
        with pytest.raises(ValueError):
            await history.update_step(
                anchor.model_copy(update={"outcome": models.StepCompleted(result=2)})
            )
        await history.update_step(
            delivered.model_copy(update={"outcome": models.StepCompleted(result=2)})
        )
        delivered = await history.record_delivery("anchor", "anchor")
        assert delivered.deliveries == ["first", "anchor"]
        with pytest.raises(ValueError, match="already delivered"):
            await history.record_delivery("anchor", "first")
    restored = await SQLiteHistory(database.path).get_step("anchor")
    assert restored.deliveries == ["first", "anchor"]
    assert restored.outcome == models.StepCompleted(result=2)
    with pytest.raises(ValueError, match="terminal"):
        await database.update_step(
            restored.model_copy(update={"outcome": models.StepCompleted(result=3)})
        )


async def test_crash_after_one_parallel_result_is_delivered(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "partial.py").write_text(sources.read("partial_delivery.py"))
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "vectorops.runner",
        "--project",
        str(tmp_path),
        "--workspace-directory",
        "",
        "start",
        "partial:main",
        "--execution-id",
        "partial",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started: dict[str, str] = {}
    try:
        assert process.stdout is not None
        async with asyncio.timeout(10):
            while True:
                line = (await process.stdout.readline()).decode().strip()
                assert line, "runner exited before delivering B"
                if line == "delivered:B":
                    break
                prefix, name, step_id = line.split(":")
                assert prefix == "started"
                started[name] = step_id
        assert list(started) == ["A", "B"]
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()

    database = SQLiteHistory(tmp_path / "data/executions/partial/history.sqlite3")
    first = await database.get_step(started["A"])
    second = await database.get_step(started["B"])
    assert first.outcome.kind == "pending"
    assert not first.deliveries
    assert second.previous_step_id == first.id
    assert second.outcome == models.StepCompleted(result="B")
    assert second.deliveries == [second.id]

    (tmp_path / "release-a").touch()
    restarted = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "vectorops.runner",
        "--project",
        str(tmp_path),
        "--workspace-directory",
        "",
        "resume",
        "partial",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(10):
            stdout, stderr = await restarted.communicate()
    finally:
        if restarted.returncode is None:
            restarted.kill()
            await restarted.communicate()
    assert restarted.returncode == 0, stderr.decode()
    lines = stdout.decode().splitlines()
    assert lines[:2] == ["delivered:B", f"started:A:{first.id}"]
    assert len(lines) == 3
    assert json.loads(lines[2]) == {"execution_id": "partial", "result": ["A", "B"]}
    assert (await database.get_step(first.id)).outcome == models.StepCompleted(
        result="A"
    )
    record = await database.get_execution()
    assert record.status == "completed"
    path = [
        step
        async for step in database.replay_path(
            first.workflow_id, record.checkpoint.heads[first.workflow_id]
        )
    ]
    assert [step.operation.kind for step in path] == [
        first.operation.kind,
        second.operation.kind,
        "workflow.complete",
    ]
    assert [target for step in path for target in step.deliveries] == [
        second.id,
        first.id,
    ]
    assert await WorkflowRunner(tmp_path, workspace_directory="").resume("partial") == [
        "A",
        "B",
    ]
    assert capsys.readouterr().out.splitlines() == ["delivered:B"]
