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
    assert project.config == RunnerConfig().for_project(project.root)
    assert project.config.environment is not None
    assert project.config.environment.base_project_path == str(project.root)


@pytest.mark.parametrize("directory", [".voc", "custom/runner", ""])
@pytest.mark.parametrize("existing_config", [False, True])
async def test_default_target_is_project_not_runner_directory(
    tmp_path: Path, directory: str, existing_config: bool
) -> None:
    workspace = tmp_path / directory
    workspace.mkdir(parents=True, exist_ok=True)
    config = workspace / "config.toml"
    if existing_config:
        config.write_text('[components.llm]\nmodel = "test"\n')
    project = await RunnerProject.open(tmp_path, workspace_directory=directory)
    assert project.config.environment is not None
    assert project.config.environment.kind == "local"
    assert project.config.environment.base_project_path == str(tmp_path.resolve())
    assert not project.config.environment.processes.allow_shell
    assert project.data_path == workspace / "data"
    if existing_config:
        assert config.read_text() == '[components.llm]\nmodel = "test"\n'
    else:
        assert not config.exists()


@pytest.mark.parametrize("directory", [".voc", "custom/runner", ""])
@pytest.mark.parametrize("explicit_path", [False, True])
async def test_local_environment_path_defaults(
    tmp_path: Path, directory: str, explicit_path: bool
) -> None:
    workspace = tmp_path / directory
    workspace.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "separate-project" if explicit_path else tmp_path.resolve()
    contents = '[environment]\nkind = "local"\n'
    if explicit_path:
        contents += f'base_project_path = "{target}"\n'
    contents += "[environment.processes]\nallow_shell = true\n"
    config = workspace / "config.toml"
    config.write_text(contents)

    project = await RunnerProject.open(tmp_path, workspace_directory=directory)

    assert project.config.environment is not None
    assert project.config.environment.base_project_path == str(target)
    assert project.config.environment.processes.allow_shell
    assert project.workspace_path == workspace
    assert project.data_path == workspace / "data"
    assert config.read_text() == contents


@pytest.mark.parametrize("path", ["", "relative"])
async def test_explicit_invalid_local_path_is_not_defaulted(
    tmp_path: Path, path: str
) -> None:
    workspace = tmp_path / ".voc"
    workspace.mkdir()
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = "{path}"\n'
    )
    with pytest.raises(pydantic.ValidationError):
        await RunnerProject.open(tmp_path, workspace_directory=".voc")


def test_direct_projects_resolve_independent_defaults(tmp_path: Path) -> None:
    config = RunnerConfig()
    first = RunnerProject(tmp_path / "first", config)
    second = RunnerProject(tmp_path / "second", config)
    assert config.environment is None
    assert first.config.environment is not None
    assert second.config.environment is not None
    assert first.config.environment.base_project_path == str(tmp_path / "first")
    assert second.config.environment.base_project_path == str(tmp_path / "second")


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
