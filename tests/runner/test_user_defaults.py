from pathlib import Path
from unittest.mock import patch

from vectorops.runner import global_config
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_project_components_merge_over_user_defaults(tmp_path: Path) -> None:
    directory = global_config.directory()
    directory.mkdir()
    (directory / "config.toml").write_text(
        '[components.llm]\nmodel = "openai/gpt-4.1"\ntemperature = 0.7\n'
        '[components.llm.reasoning]\neffort = "high"\nsummary = "auto"\n'
        '[components.llm.profiles.strong]\nmodel = "openai/o3"\n'
        '[components.llm.profiles.weak]\nmodel = "openai/gpt-4.1-mini"\n'
        '[components.report]\nlanguages = ["en", "fr"]\nenabled = true\n'
        '[components.mcp.servers.example]\ncommand = "example"\nargs = ["old"]\n'
    )
    (tmp_path / "config.toml").write_text(
        'data_directory = "state"\n'
        "[components.llm]\ntemperature = 0\n"
        '[components.llm.reasoning]\neffort = "low"\n'
        '[components.llm.profiles.strong]\nmodel = "openai/gpt-4.1"\n'
        "[components.report]\nlanguages = []\nenabled = false\n"
        '[mcp.servers.example]\nargs = ["new"]\n'
    )
    project = await RunnerProject.open(tmp_path)
    assert project.data_path == tmp_path / "state"
    assert project.configuration.get("llm") == {
        "model": "openai/gpt-4.1",
        "temperature": 0,
        "reasoning": {"effort": "low", "summary": "auto"},
        "profiles": {
            "strong": {"model": "openai/gpt-4.1"},
            "weak": {"model": "openai/gpt-4.1-mini"},
        },
    }
    assert project.configuration.get("report") == {
        "languages": [],
        "enabled": False,
    }
    assert project.configuration.get("mcp.servers.example") == {
        "command": "example",
        "args": ["new"],
    }
    assert not (directory / "secrets.json").exists()


async def test_user_defaults_are_loaded_once_and_replayed(workspace: Path) -> None:
    directory = global_config.directory()
    directory.mkdir()
    path = directory / "config.toml"
    path.write_text('[components.llm]\nmodel = "openai/gpt-4.1"\n')
    runner = WorkflowRunner(workspace)
    with patch.object(
        global_config.GlobalConfiguration,
        "read_defaults",
        autospec=True,
        side_effect=global_config.GlobalConfiguration.read_defaults,
    ) as read:
        first = await runner.run("configuration:main")
        path.write_text('[components.llm]\nmodel = "openai/o3"\n')
        assert await runner.run("configuration:main") == first
        assert read.call_count == 1
    assert isinstance(first, dict) and first["model"] == "openai/gpt-4.1"
    restarted = WorkflowRunner(workspace)
    assert await restarted.resume() == first
    fresh = await restarted.run("configuration:main")
    assert isinstance(fresh, dict) and fresh["model"] == "openai/o3"
    assert not (workspace / "config.toml").exists()


async def test_explicit_config_directory_is_shared_with_authentication(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "selected"
    directory.mkdir()
    (directory / "config.toml").write_text('[components]\nvalue = "selected"\n')
    environment = global_config.directory()
    environment.mkdir()
    (environment / "config.toml").write_text("invalid TOML [")
    runner = WorkflowRunner(tmp_path, config_dir=directory)
    assert runner.authentication.configuration.path == directory
    assert (await runner.project()).configuration.get("value") == "selected"
    async with RunnerService(tmp_path, config_dir=directory) as service:
        assert service.authentication.configuration.path == directory
        assert (await service.runner.project()).configuration.get("value") == "selected"


async def test_runtime_overrides_use_merged_baseline(tmp_path: Path) -> None:
    directory = global_config.directory()
    directory.mkdir()
    path = directory / "config.toml"
    path.write_text('[components]\ninherited = "first"\nlocal = "user"\n')
    (tmp_path / "config.toml").write_text('[components]\nlocal = "project"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("inherited", "temporary")
    await config.reset("inherited")
    assert config.get("inherited") == "first"
    await config.set("inherited", "override", permanent=True)
    await config.set("local", "override", permanent=True)
    restarted = (await RunnerProject.open(tmp_path)).configuration
    assert restarted.get("inherited") == "override"
    path.write_text('[components]\ninherited = "second"\nlocal = "changed"\n')
    assert config.get("inherited") == "override"
    restarted = (await RunnerProject.open(tmp_path)).configuration
    assert restarted.get("inherited") == "second"
    assert restarted.get("local") == "override"
    await restarted.reset("local")
    assert restarted.get("local") == "project"


async def test_user_defaults_are_isolated_between_projects(tmp_path: Path) -> None:
    directory = global_config.directory()
    directory.mkdir()
    (directory / "config.toml").write_text('[components]\nvalue = "user"\n')
    other = tmp_path / "other"
    other.mkdir()
    first = (await RunnerProject.open(tmp_path)).configuration
    second = (await RunnerProject.open(other)).configuration
    await first.set("value", "override", permanent=True)
    assert second.get("value") == "user"
    assert (await RunnerProject.open(other)).configuration.get("value") == "user"
