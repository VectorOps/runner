import asyncio
import json
from pathlib import Path
import sys
from typing import Optional

import pytest

from tests.fixtures import sources
from vectorops.runner import cli
from vectorops.runner.control import client
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture(autouse=True)
def default_layout(
    direct_workspace_layout: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VOPS_RUNNER_WORKSPACE_DIRECTORY", raising=False)


async def test_default_layout_keeps_project_files_untouched(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text("not runner configuration")
    project = await RunnerProject.open(tmp_path)
    assert project.root == tmp_path.resolve()
    assert project.workspace_path == tmp_path / ".voc"
    assert project.config_path == tmp_path / ".voc/config.toml"
    assert project.workflows_path == tmp_path / ".voc/workflows"
    assert project.data_path == tmp_path / ".voc/data"
    assert project.worktrees.directory == tmp_path / ".voc/data/worktrees"
    assert project.workflows_path.is_dir()
    assert project.data_path.is_dir()
    await project.configuration.set("test.value", 1, permanent=True)
    assert (project.data_path / "workspace.sqlite3").is_file()
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "workflows").exists()
    assert config.read_text() == "not runner configuration"
    assert (await RunnerProject.open(tmp_path)).configuration.get("test.value") == 1


@pytest.mark.parametrize("directory", [".voc", "custom/runner", ""])
async def test_configuration_and_execution_paths(
    tmp_path: Path, directory: str
) -> None:
    workspace = tmp_path / directory
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "config.toml").write_text(
        'workflows_directory = "flows"\ndata_directory = "state"\n'
    )
    runner = WorkflowRunner(tmp_path, workspace_directory=directory)
    project = await runner.project()
    assert project.workflows_path == workspace / "flows"
    assert project.data_path == workspace / "state"
    assert project.worktrees.directory == workspace / "state/worktrees"
    (project.workflows_path / "sample.py").write_text(sources.read("answer.py"))
    assert await runner.run("sample:main", new_execution_id="test") == 42
    assert (workspace / "state/executions/test/history.sqlite3").is_file()
    restarted = WorkflowRunner(tmp_path, workspace_directory=directory)
    assert await restarted.resume("test") == 42


@pytest.mark.parametrize(
    "environment,explicit,expected",
    [
        (None, None, ".voc"),
        ("", None, ""),
        ("custom", None, "custom"),
        ("custom", "", ""),
        ("custom", ".voc", ".voc"),
    ],
)
async def test_workspace_selection_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment: Optional[str],
    explicit: Optional[str],
    expected: str,
) -> None:
    if environment is not None:
        monkeypatch.setenv("VOPS_RUNNER_WORKSPACE_DIRECTORY", environment)
    project = await RunnerProject.open(tmp_path, workspace_directory=explicit)
    assert project.workspace_path == tmp_path / expected


@pytest.mark.parametrize(
    "directory", [".", "..", "../outside", "/outside", "nested/../../outside"]
)
async def test_workspace_rejects_invalid_paths(tmp_path: Path, directory: str) -> None:
    with pytest.raises(ValueError):
        await RunnerProject.open(tmp_path, workspace_directory=directory)


async def test_workspace_symlink_cannot_escape_project(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / ".voc").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="inside"):
        await RunnerProject.open(root)
    assert list(outside.iterdir()) == []


async def test_data_symlink_cannot_escape_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / ".voc"
    workspace.mkdir()
    (workspace / "data").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="inside"):
        await RunnerProject.open(tmp_path)


async def test_existing_file_cannot_be_workspace(tmp_path: Path) -> None:
    (tmp_path / ".voc").write_text("keep")
    with pytest.raises(FileExistsError):
        await RunnerProject.open(tmp_path)
    assert (tmp_path / ".voc").read_text() == "keep"


@pytest.mark.parametrize("directory", [None, "custom", ""])
async def test_service_and_embedded_forward_workspace_selection(
    tmp_path: Path, directory: Optional[str]
) -> None:
    workspace = tmp_path / (".voc" if directory is None else directory)
    async with RunnerService(tmp_path, workspace_directory=directory) as service:
        assert service.path == tmp_path.resolve()
        project = await service.runner.project()
        assert project.workspace_path == workspace
        await project.configuration.set("test.value", "persisted", permanent=True)
    async with client.embedded(tmp_path, workspace_directory=directory) as connection:
        assert (
            await connection.get_configuration("components.test.value")
        ).value == "persisted"
    assert not (workspace / ".voc").exists()


@pytest.mark.parametrize("directory", [None, "custom", ""])
def test_cli_workspace_selection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], directory: Optional[str]
) -> None:
    selected = ".voc" if directory is None else directory
    workflows = tmp_path / selected / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "sample.py").write_text(sources.read("answer.py"))
    prefix = ["--project", str(tmp_path)]
    if directory is not None:
        prefix.extend(["--workspace-directory", directory])
    assert cli.main(prefix + ["start", "sample:main"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == 42
    assert cli.main(prefix + ["resume"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == 42
    assert (tmp_path / selected / "data/workspace.sqlite3").is_file()


@pytest.mark.parametrize("directory", [None, "custom", ""])
async def test_daemon_workspace_selection(
    tmp_path: Path, directory: Optional[str]
) -> None:
    args = [sys.executable, "-m", "vectorops.runner", "--project", str(tmp_path)]
    if directory is not None:
        args.extend(["--workspace-directory", directory])
    process = await asyncio.create_subprocess_exec(
        *args,
        "daemon",
        "serve",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        async with asyncio.timeout(10):
            line = await process.stdout.readline()
        assert line
        endpoint = json.loads(line)
        assert endpoint["workspace"] == str(tmp_path.resolve())
        async with await client.RunnerClient.tcp(endpoint["port"]) as connection:
            await connection.set_configuration(
                "components.test.value", 1, permanent=True
            )
            await connection.shutdown()
        async with asyncio.timeout(10):
            await process.wait()
        assert process.returncode == 0
        selected = ".voc" if directory is None else directory
        assert (tmp_path / selected / "data/workspace.sqlite3").is_file()
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
