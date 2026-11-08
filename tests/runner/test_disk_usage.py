from pathlib import Path

import pytest

from vectorops.runner.runner import disk_usage


async def test_workspace_sizes_and_ranking(tmp_path: Path) -> None:
    workspace = tmp_path / ".voc"
    data = workspace / "custom-data"
    files = {
        "config.toml": 5,
        "workflows/example.py": 11,
        "custom-data/workspace.sqlite3": 17,
        "custom-data/logs/runner.log": 23,
        "custom-data/executions/alpha/history.sqlite3": 100,
        "custom-data/executions/alpha/history.sqlite3-wal": 30,
        "custom-data/executions/alpha/history.sqlite3-shm": 20,
        "custom-data/executions/alpha/artifacts/output": 50,
        "custom-data/executions/beta/history.sqlite3": 200,
        "custom-data/executions/small/history.sqlite3": 10,
    }
    for name, size in files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * size)
    (data / "executions/empty").mkdir()
    (tmp_path / "unrelated").write_bytes(b"x" * 1000)

    report = await disk_usage.measure(workspace, data, disk_usage.Query(top=2))

    assert report.total_bytes == sum(files.values())
    assert report.workflow_count == 4
    assert report.top_workflows == [
        disk_usage.WorkflowUsage(execution_id="alpha", size_bytes=200),
        disk_usage.WorkflowUsage(execution_id="beta", size_bytes=200),
    ]
    all_workflows = await disk_usage.measure(workspace, data, disk_usage.Query())
    assert [item.execution_id for item in all_workflows.top_workflows] == [
        "alpha",
        "beta",
        "small",
        "empty",
    ]
    totals = await disk_usage.measure(workspace, data, disk_usage.Query(top=0))
    assert totals.total_bytes == report.total_bytes
    assert totals.workflow_count == 4
    assert totals.top_workflows == []


async def test_symlinks_are_not_followed(tmp_path: Path) -> None:
    workspace = tmp_path / ".voc"
    data = workspace / "data"
    execution = data / "executions/one"
    execution.mkdir(parents=True)
    (execution / "history.sqlite3").write_bytes(b"123")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "large").write_bytes(b"x" * 1000)
    (execution / "file-link").symlink_to(outside / "large")
    (execution / "directory-link").symlink_to(outside, target_is_directory=True)
    (execution / "cycle").symlink_to(workspace, target_is_directory=True)
    (execution / "broken").symlink_to(outside / "missing")
    (data / "executions/alias").symlink_to(execution, target_is_directory=True)

    report = await disk_usage.measure(workspace, data, disk_usage.Query())

    assert report.total_bytes == 3
    assert report.workflow_count == 1
    assert report.top_workflows == [
        disk_usage.WorkflowUsage(execution_id="one", size_bytes=3)
    ]


async def test_missing_or_empty_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / ".voc"
    for exists in (False, True):
        if exists:
            workspace.mkdir()
        report = await disk_usage.measure(
            workspace, workspace / "data", disk_usage.Query()
        )
        assert report == disk_usage.Report(total_bytes=0, workflow_count=0)


async def test_unreadable_directory_fails_instead_of_undercounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied(path: Path) -> None:
        raise PermissionError(str(path))

    monkeypatch.setattr(disk_usage.os, "scandir", denied)
    with pytest.raises(PermissionError):
        await disk_usage.measure(tmp_path, tmp_path / "data", disk_usage.Query())
