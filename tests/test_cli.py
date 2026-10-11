import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

import pytest

from tests.fixtures import sources
from vectorops.runner import cli, models
from vectorops.runner.runner import execution_state
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner


def arguments(project: Path, *values: str) -> cli.Arguments:
    return cli.parser().parse_args(
        ["--project", str(project), *values], namespace=cli.Arguments()
    )


def test_start_status_resume_and_export(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prefix = ["--project", str(workspace)]
    assert (
        cli.main(prefix + ["start", "activities:empty", "--execution-id", "sample"])
        == 0
    )
    started = json.loads(capsys.readouterr().out)
    assert started["execution_id"] == "sample"
    assert started["result"]["data"] == str(workspace / "data")
    assert cli.main(prefix + ["status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["execution"]["id"] == "sample"
    assert status["execution"]["status"] == "completed"
    assert status["stats"] == {
        "workflows": 1,
        "steps": 1,
        "operations": {"workflow.complete": 1},
        "outcomes": {"completed": 1},
    }
    assert cli.main(prefix + ["resume", "sample"]) == 0
    assert json.loads(capsys.readouterr().out)["execution_id"] == "sample"
    output = workspace / "analysis.jsonl"
    assert cli.main(prefix + ["export", "--output", str(output)]) == 0
    capsys.readouterr()
    header, step = [json.loads(line) for line in output.read_text().splitlines()]
    assert header["kind"] == "execution.header"
    assert header["version"] == 1
    assert header["execution"]["id"] == "sample"
    assert header["workflows"][0]["definition_ref"] == "activities:empty"
    assert (
        models.WorkflowStep.model_validate(step).operation.kind == "workflow.complete"
    )
    original = output.read_bytes()
    assert cli.main(prefix + ["export", "sample", "--output", str(output)]) == 1
    assert output.read_bytes() == original
    assert "vops-runner:" in capsys.readouterr().err
    assert (
        cli.main(prefix + ["start", "activities:workflow", "--execution-id", "sample"])
        == 1
    )
    assert "execution already exists" in capsys.readouterr().err


@pytest.mark.parametrize(
    "value", ["[]", "null", "1", "bad", '{"x": NaN}', '{"x": Infinity}']
)
def test_invalid_input(tmp_path: Path, value: str) -> None:
    with pytest.raises(SystemExit) as error:
        arguments(tmp_path, "start", "example:workflow", "--input", value)
    assert error.value.code == 2


def test_missing_execution_and_invalid_identity(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prefix = ["--project", str(tmp_path)]
    assert cli.main(prefix + ["status"]) == 1
    assert "no last execution" in capsys.readouterr().err
    assert cli.main(prefix + ["status", "../escape"]) == 1
    capsys.readouterr()
    assert (
        cli.main(
            prefix + ["export", "missing", "--output", str(tmp_path / "missing.jsonl")]
        )
        == 1
    )
    assert not (tmp_path / "missing.jsonl").exists()
    assert not (tmp_path / "data" / "executions" / "missing").exists()


async def test_input_is_passed_to_workflow(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (workspace / "workflows" / "echo.py").write_text(sources.read("echo.py"))
    await cli.dispatch(
        arguments(workspace, "start", "echo:workflow", "--input", '{"value": [1, "a"]}')
    )
    assert json.loads(capsys.readouterr().out)["result"] == [1, "a"]


async def test_inspection_includes_children_and_branches_with_writer_lock(
    tmp_path: Path,
) -> None:
    path = tmp_path / "data" / "executions" / "sample" / "history.sqlite3"
    database = SQLiteHistory(path, batch_size=1)
    for identity in ("root", "child"):
        await database.save_workflow(
            models.Workflow(id=identity, definition_ref="example:workflow")
        )
    record = execution_state.ExecutionRecord(id="sample", root_workflow_id="root")
    await database.save_execution(record)
    for identity, workflow_id, predecessor, outcome in (
        ("first", "root", None, models.StepCompleted(result=1)),
        ("old", "root", "first", models.StepCompleted(result=2)),
        ("selected", "root", "first", models.StepPending()),
        ("child-step", "child", None, models.StepStreaming(state=3)),
    ):
        await database.append_step(
            models.WorkflowStep(
                id=identity,
                workflow_id=workflow_id,
                previous_step_id=predecessor,
                operation=models.StepOperation(kind="test.activity", name="example"),
                outcome=outcome,
            )
        )
    runner = WorkflowRunner(tmp_path)
    async with RunnerStateStore(tmp_path / "data"):
        status = await runner.inspect_execution("sample")
        assert status.execution.status == "running"
        assert status.execution.checkpoint.heads == {
            "root": "selected",
            "child": "child-step",
        }
        assert status.stats.workflows == 2
        assert status.stats.steps == 4
        assert status.stats.operations == {"test.activity": 4}
        assert status.stats.outcomes == {"completed": 2, "pending": 1, "streaming": 1}
        output = tmp_path / "all.jsonl"
        await runner.export_execution(output, "sample")
    entries = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(entries[0]["workflows"]) == 2
    assert [entry["id"] for entry in entries[1:]] == [
        "first",
        "old",
        "selected",
        "child-step",
    ]
    assert (
        entries[0]["execution"]["checkpoint"]["updated_at"]
        == record.checkpoint.updated_at
    )


async def test_inspection_missing_database_does_not_create_it(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "missing.sqlite3")
    with pytest.raises(sqlite3.OperationalError):
        await database.inspect_execution()
    assert not database.path.exists()


async def test_export_failure_removes_partial_output(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="example:workflow")
    )
    await database.save_execution(
        execution_state.ExecutionRecord(id="sample", root_workflow_id="root")
    )
    with closing(sqlite3.connect(database.path)) as connection:
        with connection:
            connection.execute("INSERT INTO steps VALUES ('bad', 'root', NULL, '{}')")
    output = tmp_path / "partial.jsonl"
    with pytest.raises(ValueError):
        await database.export_execution(output)
    assert not output.exists()


async def test_resume_after_process_crash(tmp_path: Path) -> None:
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    marker = tmp_path / "identity"
    release = tmp_path / "release"
    (workflows / "crash.py").write_text(sources.read("crash.py"))
    prefix = [sys.executable, "-m", "vectorops.runner", "--project", str(tmp_path)]
    process = await asyncio.create_subprocess_exec(
        *prefix,
        "start",
        "crash:workflow",
        "--execution-id",
        "crashed",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(10):
            while not marker.exists():
                if process.returncode is not None:
                    _, stderr = await process.communicate()
                    pytest.fail(stderr.decode())
                await asyncio.sleep(0.01)
        identity = marker.read_text()
        live = await WorkflowRunner(tmp_path).inspect_execution("crashed")
        assert live.execution.status == "running"
        assert live.stats.outcomes == {"pending": 1}
        await WorkflowRunner(tmp_path).export_execution(tmp_path / "live.jsonl")
        live_entries = [
            json.loads(line)
            for line in (tmp_path / "live.jsonl").read_text().splitlines()
        ]
        assert live_entries[1]["id"] == identity
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
    status = await WorkflowRunner(tmp_path).inspect_execution("crashed")
    assert status.execution.status == "running"
    assert status.stats.outcomes == {"pending": 1}
    release.touch()
    resumed = await asyncio.create_subprocess_exec(
        *prefix,
        "resume",
        "crashed",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(10):
            stdout, stderr = await resumed.communicate()
    finally:
        if resumed.returncode is None:
            resumed.kill()
            await resumed.communicate()
    assert resumed.returncode == 0, stderr.decode()
    assert json.loads(stdout) == {"execution_id": "crashed", "result": 42}
    assert marker.read_text() == identity
    assert (
        await WorkflowRunner(tmp_path).inspect_execution()
    ).execution.status == "completed"
