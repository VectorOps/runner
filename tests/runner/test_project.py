from pathlib import Path
import tomllib

import pydantic
import pytest

from vectorops.runner.runner.project import RunnerConfig, RunnerProject


async def test_workspace_defaults(tmp_path: Path) -> None:
    project = await RunnerProject.open(tmp_path)
    assert project.root == tmp_path.resolve()
    assert project.config_path == project.root / "config.toml"
    assert project.workflows_path.is_dir()
    assert project.data_path.is_dir()
    assert project.config == RunnerConfig()
    assert project.config.environment is None


async def test_configuration_overrides(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text(
        'workflows_directory = "custom/python"\ndata_directory = "custom/state"\n'
    )
    project = await RunnerProject.open(tmp_path)
    assert project.workflows_path == project.root / "custom/python"
    assert project.data_path == project.root / "custom/state"
    assert project.data_path.is_dir()


@pytest.mark.parametrize("directory", ["", ".", "..", "../outside", "/outside"])
def test_invalid_directory_configuration(directory: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        RunnerConfig(data_directory=directory)


async def test_unknown_configuration_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text("unknown = true")
    with pytest.raises(pydantic.ValidationError):
        await RunnerProject.open(tmp_path)


async def test_malformed_toml(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text("invalid = [")
    with pytest.raises(tomllib.TOMLDecodeError):
        await RunnerProject.open(tmp_path)


async def test_missing_workspace(tmp_path: Path) -> None:
    with pytest.raises(NotADirectoryError):
        await RunnerProject.open(tmp_path / "missing")


async def test_workspace_directory_symlink_cannot_escape(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "data").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="inside"):
        await RunnerProject.open(root)


async def test_environment_configuration(tmp_path: Path) -> None:
    target = tmp_path / "separate-project"
    (tmp_path / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = "{target}"\n'
    )
    project = await RunnerProject.open(tmp_path)
    assert project.config.environment is not None
    assert project.config.environment.kind == "local"
    assert project.config.environment.base_project_path == str(target)
    assert not target.exists()


@pytest.mark.parametrize(
    "configuration",
    [
        {"kind": "remote", "base_project_path": "/tmp/project"},
        {"kind": "local"},
        {"kind": "local", "base_project_path": "relative"},
        {"kind": "local", "base_project_path": ""},
        {"kind": "local", "base_project_path": "/bad\0path"},
        {"kind": "local", "base_project_path": "/tmp/project", "unknown": True},
        {"kind": "local", "base_project_path": "/tmp/project", "max_read_bytes": 0},
    ],
)
def test_invalid_environment_configuration(configuration: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        RunnerConfig.model_validate({"environment": configuration})
