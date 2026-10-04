from datetime import datetime, timedelta, timezone
from pathlib import Path
import typing

import pydantic
import pytest

from tests.fixtures.probe import ExecutionProbe
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.project import RetentionPolicy, RunnerConfig, RunnerProject
from vectorops.runner.runner.retention import ExecutionRetention
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner

NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


async def create_history(
    data_path: Path,
    identifier: str,
    *,
    days: float = 0,
    status: typing.Literal["running", "completed", "failed", "cancelled"] = "completed",
) -> Path:
    path = data_path / "executions" / identifier / "history.sqlite3"
    database = SQLiteHistory(path)
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="activities:workflow")
    )
    await database.save_execution(
        ExecutionRecord(
            id=identifier,
            root_workflow_id="root",
            status=status,
            updated_at=(NOW - timedelta(days=days)).isoformat(),
        )
    )
    return path.parent


@pytest.mark.parametrize(
    "options",
    [
        {"max_age_days": -1},
        {"max_completed_executions": -1},
        {"max_age_days": 1.5},
        {"max_completed_executions": True},
        {"max_age_days": "30"},
        {"unknown": 1},
    ],
)
def test_invalid_retention_configuration(options: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        RetentionPolicy.model_validate(options)


async def test_retention_defaults_disabled_and_toml_overrides(tmp_path: Path) -> None:
    assert RunnerConfig().retention == RetentionPolicy()
    (tmp_path / "config.toml").write_text(
        "[retention]\nmax_age_days = 30\nmax_completed_executions = 100\n"
    )
    project = await RunnerProject.open(tmp_path)
    assert project.config.retention == RetentionPolicy(
        max_age_days=30, max_completed_executions=100
    )


async def test_disabled_retention_does_not_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    retention = ExecutionRetention(tmp_path, RetentionPolicy())

    def forbidden() -> list[Path]:
        raise AssertionError("disabled retention must not scan")

    monkeypatch.setattr(retention, "_directories", forbidden)
    assert await retention.apply(None) == []


async def test_age_limit_uses_execution_timestamp_and_preserves_boundary(
    tmp_path: Path,
) -> None:
    old = await create_history(tmp_path, "old", days=31)
    boundary = await create_history(tmp_path, "boundary", days=30)
    recent = await create_history(tmp_path, "recent", days=1)
    deleted = await ExecutionRetention(
        tmp_path, RetentionPolicy(max_age_days=30)
    ).apply(None, now=NOW)
    assert deleted == ["old"]
    assert not old.exists()
    assert boundary.exists() and recent.exists()


async def test_count_limit_keeps_newest_and_counts_protected_execution(
    tmp_path: Path,
) -> None:
    oldest = await create_history(tmp_path, "oldest", days=10)
    middle = await create_history(tmp_path, "middle", days=5)
    newest = await create_history(tmp_path, "newest", days=1)
    deleted = await ExecutionRetention(
        tmp_path, RetentionPolicy(max_completed_executions=2)
    ).apply("oldest", now=NOW)
    assert deleted == ["middle"]
    assert oldest.exists() and newest.exists() and not middle.exists()


async def test_combined_limits_remove_histories_exceeding_either(
    tmp_path: Path,
) -> None:
    for identifier, days in (("old", 40), ("middle", 20), ("recent", 1)):
        await create_history(tmp_path, identifier, days=days)
    deleted = await ExecutionRetention(
        tmp_path, RetentionPolicy(max_age_days=10, max_completed_executions=2)
    ).apply(None, now=NOW)
    assert set(deleted) == {"old", "middle"}
    assert (tmp_path / "executions/recent").exists()


async def test_zero_limits_preserve_last_and_noncompleted_histories(
    tmp_path: Path,
) -> None:
    await create_history(tmp_path, "last", days=100)
    await create_history(tmp_path, "completed", days=100)
    for status in ("running", "failed", "cancelled"):
        await create_history(tmp_path, status, days=100, status=status)
    deleted = await ExecutionRetention(
        tmp_path, RetentionPolicy(max_age_days=0, max_completed_executions=0)
    ).apply("last", now=NOW)
    assert deleted == ["completed"]
    assert all(
        (tmp_path / "executions" / identifier).exists()
        for identifier in ("last", "running", "failed", "cancelled")
    )


async def test_reserved_histories_survive_retention_until_released(
    tmp_path: Path,
) -> None:
    for identifier in ("resuming", "finishing", "old", "last"):
        await create_history(tmp_path, identifier, days=100)
    retention = ExecutionRetention(
        tmp_path, RetentionPolicy(max_completed_executions=0, max_age_days=0)
    )
    assert await retention.apply(
        "last", now=NOW, protected_ids={"resuming", "finishing"}
    ) == ["old"]
    assert set(await retention.apply("last", now=NOW)) == {"resuming", "finishing"}


async def test_running_last_pointer_does_not_consume_completed_limit(
    tmp_path: Path,
) -> None:
    await create_history(tmp_path, "last", status="running")
    await create_history(tmp_path, "old", days=10)
    await create_history(tmp_path, "new", days=1)
    assert await ExecutionRetention(
        tmp_path, RetentionPolicy(max_completed_executions=1)
    ).apply("last", now=NOW) == ["old"]


async def test_invalid_and_symlinked_histories_are_preserved(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    incomplete = tmp_path / "executions/incomplete"
    incomplete.mkdir(parents=True)
    corrupt = tmp_path / "executions/corrupt"
    corrupt.mkdir()
    (corrupt / "history.sqlite3").write_bytes(b"not sqlite")
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "executions/link").symlink_to(outside, target_is_directory=True)
    mismatched = await create_history(tmp_path, "original")
    mismatched.rename(tmp_path / "executions/wrong")
    deleted = await ExecutionRetention(
        tmp_path, RetentionPolicy(max_completed_executions=0)
    ).apply(None, now=NOW)
    assert deleted == []
    assert incomplete.exists() and corrupt.exists() and outside.exists()
    assert "Preserving unreadable" in caplog.text


async def test_symlinked_executions_root_is_not_scanned(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    await create_history(outside, "old", days=100)
    data = tmp_path / "data"
    data.mkdir()
    (data / "executions").symlink_to(outside / "executions", target_is_directory=True)
    assert (
        await ExecutionRetention(
            data, RetentionPolicy(max_completed_executions=0)
        ).apply(None, now=NOW)
        == []
    )
    assert (outside / "executions/old").exists()


async def test_naive_or_invalid_timestamps_are_preserved(tmp_path: Path) -> None:
    await create_history(tmp_path, "invalid")
    database = SQLiteHistory(tmp_path / "executions/invalid/history.sqlite3")
    record = await database.get_execution()
    await database.save_execution(
        record.model_copy(update={"updated_at": "not a timestamp"})
    )
    assert (
        await ExecutionRetention(
            tmp_path, RetentionPolicy(max_completed_executions=0)
        ).apply(None, now=NOW)
        == []
    )
    with pytest.raises(ValueError, match="timezone"):
        await ExecutionRetention(tmp_path, RetentionPolicy(max_age_days=1)).apply(
            None, now=NOW.replace(tzinfo=None)
        )


async def test_removal_errors_logged_without_failing_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    await create_history(tmp_path, "old")

    def fail(path: Path) -> None:
        raise PermissionError("blocked")

    monkeypatch.setattr("vectorops.runner.runner.retention.shutil.rmtree", fail)
    assert (
        await ExecutionRetention(
            tmp_path, RetentionPolicy(max_completed_executions=0)
        ).apply(None, now=NOW)
        == []
    )
    assert "Could not remove" in caplog.text


async def test_cleanup_removes_entire_execution_directory(tmp_path: Path) -> None:
    directory = await create_history(tmp_path, "old")
    (directory / "extra-state").write_text("child state")
    (directory / "history.sqlite3-shm").write_text("unused sidecar")
    assert await ExecutionRetention(
        tmp_path, RetentionPolicy(max_completed_executions=0)
    ).apply(None, now=NOW) == ["old"]
    assert not directory.exists()


async def test_successful_runner_applies_retention_and_keeps_minimal_global_state(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    (workspace / "config.toml").write_text(
        "[retention]\nmax_completed_executions = 1\n"
    )
    runner = WorkflowRunner(workspace)
    await runner.run("activities")
    first = str(runner.last_execution_id)
    await runner.run("activities")
    second = str(runner.last_execution_id)
    assert not (workspace / "data/executions" / first).exists()
    assert (workspace / "data/executions" / second / "history.sqlite3").exists()
    assert (await runner.project_state()).last_execution_id == second
    assert await runner.resume() == 2


async def test_failed_run_does_not_trigger_retention(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    old = await create_history(workspace / "data", "old", days=100)
    (workspace / "config.toml").write_text(
        "[retention]\nmax_completed_executions = 0\n"
    )
    with pytest.raises(ActivityError):
        await WorkflowRunner(workspace).run("activities", "unhandled")
    assert old.exists()


async def test_default_runner_retains_all_completed_histories(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities")
    first = str(runner.last_execution_id)
    await runner.run("activities")
    assert (workspace / "data/executions" / first).exists()


async def test_injected_history_bypasses_disk_retention(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    old = await create_history(workspace / "data", "old", days=100)
    (workspace / "config.toml").write_text(
        "[retention]\nmax_completed_executions = 0\n"
    )
    assert await WorkflowRunner(workspace, MemoryHistory()).run("activities") == 2
    assert old.exists()


async def test_scan_failure_does_not_change_successful_result(
    workspace: Path,
    execution_probe: ExecutionProbe,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    (workspace / "config.toml").write_text(
        "[retention]\nmax_completed_executions = 1\n"
    )

    def fail(self: ExecutionRetention) -> list[Path]:
        raise PermissionError("scan blocked")

    monkeypatch.setattr(ExecutionRetention, "_directories", fail)
    assert await WorkflowRunner(workspace).run("activities") == 2
    assert "Could not scan" in caplog.text
