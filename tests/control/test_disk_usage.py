from pathlib import Path

import pydantic
import pytest

from vectorops.runner.control import client, protocol
from vectorops.runner.runner import disk_usage


@pytest.mark.parametrize("workspace_directory", [".voc", ".custom"])
async def test_disk_usage_api(tmp_path: Path, workspace_directory: str) -> None:
    workspace = tmp_path / workspace_directory
    workspace.mkdir()
    (workspace / "config.toml").write_text('data_directory = "storage"\n')
    async with client.embedded(
        tmp_path, workspace_directory=workspace_directory
    ) as connection:
        before = await connection.runner_disk_usage()
        assert before.total_bytes > 0
        assert before.workflow_count == 0
        assert before.top_workflows == []
        for identifier, size in (("large", 200), ("small", 10)):
            directory = workspace / "storage/executions" / identifier
            directory.mkdir(parents=True)
            (directory / "artifact").write_bytes(b"x" * size)
        (workspace / "workflows/definition.py").write_bytes(b"x" * 50)
        (tmp_path / "outside").write_bytes(b"x" * 1000)

        report = await connection.runner_disk_usage(top=1)

        assert report.total_bytes == before.total_bytes + 260
        assert report.workflow_count == 2
        assert report.top_workflows == [
            disk_usage.WorkflowUsage(execution_id="large", size_bytes=200)
        ]
        result = await connection.request("runner.disk_usage")
        assert disk_usage.Report.model_validate(result).workflow_count == 2
        totals = await connection.runner_disk_usage(top=0)
        assert totals.total_bytes == report.total_bytes
        assert totals.top_workflows == []
        assert await connection.execution_disk_usage("large") == report.top_workflows[0]
        assert await connection.execution_disk_usage(
            "small"
        ) == disk_usage.WorkflowUsage(execution_id="small", size_bytes=10)
        assert await connection.request(
            "executions.disk_usage", {"execution_id": "small"}
        ) == {"execution_id": "small", "size_bytes": 10}


@pytest.mark.parametrize(
    "params",
    [
        {"top": -1},
        {"top": 501},
        {"top": True},
        {"top": "2"},
        {"top": 1.5},
        {"other": 1},
    ],
)
async def test_disk_usage_rejects_invalid_parameters(
    tmp_path: Path, params: dict[str, pydantic.JsonValue]
) -> None:
    async with client.embedded(tmp_path) as connection:
        with pytest.raises(protocol.RemoteError):
            await connection.request("runner.disk_usage", params)


def test_query_limit_boundaries() -> None:
    assert disk_usage.Query().top == 10
    assert disk_usage.Query(top=0).top == 0
    assert disk_usage.Query(top=500).top == 500


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"execution_id": ""},
        {"execution_id": "../outside"},
        {"execution_id": "/outside"},
        {"execution_id": "a" * 129},
        {"execution_id": None},
        {"execution_id": True},
        {"execution_id": "one", "top": 1},
    ],
)
async def test_execution_disk_usage_rejects_invalid_parameters(
    tmp_path: Path, params: dict[str, pydantic.JsonValue]
) -> None:
    async with client.embedded(tmp_path) as connection:
        with pytest.raises(protocol.RemoteError) as exc:
            await connection.request("executions.disk_usage", params)
        assert exc.value.code == "invalid_argument"


async def test_execution_disk_usage_empty_and_missing(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as connection:
        directory = tmp_path / "data/executions/empty"
        directory.mkdir(parents=True)
        assert await connection.execution_disk_usage(
            "empty"
        ) == disk_usage.WorkflowUsage(execution_id="empty", size_bytes=0)
        (directory.parent / "alias").symlink_to(directory, target_is_directory=True)
        (directory.parent / "file").write_bytes(b"123")
        for execution_id in ("missing", "alias", "file"):
            with pytest.raises(protocol.RemoteError) as exc:
                await connection.execution_disk_usage(execution_id)
            assert exc.value.code == "not_found"
