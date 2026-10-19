import asyncio
import json
from pathlib import Path

import pytest

from vectorops.runner.runner.workspace_database import WorkspaceDatabase
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager
from tests.fixtures import worktree_cases


async def git(repository: Path, *arguments: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        str(repository),
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, error = await process.communicate()
    assert process.returncode == 0, error.decode()
    return output.decode().strip()


@pytest.fixture
async def repository(tmp_path: Path) -> Path:
    root = tmp_path / "repository"
    root.mkdir()
    await git(root, "init")
    (root / "file").write_text("committed\n")
    await git(root, "add", "file")
    await git(
        root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "initial",
    )
    return root


@pytest.fixture
def manager(tmp_path: Path, repository: Path) -> Manager:
    workspace = tmp_path / "workspace" / ".voc"
    workspace.mkdir(parents=True)
    return Manager(
        workspace, repository, WorkspaceDatabase(workspace / "data"), m.Settings()
    )


@pytest.fixture
async def worktree_project(workspace: Path, repository: Path) -> RunnerProject:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(repository))}\n'
        "[components.tools.approval]\napprove_all = true\n"
    )
    return await RunnerProject.open(workspace)


@pytest.fixture
def worktree_probe(monkeypatch: pytest.MonkeyPatch) -> worktree_cases.Probe:
    probe = worktree_cases.Probe()
    monkeypatch.setattr(worktree_cases, "current", probe)
    return probe
