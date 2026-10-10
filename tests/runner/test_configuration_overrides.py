import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
import pydantic

from vectorops.runner import global_config
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.runner.project import RunnerProject


@pytest.mark.parametrize("data_directory", ["data", "custom/state"])
async def test_override_files_follow_data_directory(
    tmp_path: Path, data_directory: str
) -> None:
    path = tmp_path / "config.toml"
    text = f'data_directory = "{data_directory}"\n[components.llm]\nmodel = "base"\n'
    path.write_text(text)
    project = await RunnerProject.open(tmp_path)
    data = tmp_path / data_directory
    assert (data / "config-overrides.lock").is_file()
    await project.configuration.set("llm.model", "override", permanent=True)
    assert (data / "config-overrides.json").is_file()
    assert not (tmp_path / "config-overrides.json").exists()
    assert not (tmp_path / "config-overrides.lock").exists()
    assert path.read_text() == text
    restarted = await RunnerProject.open(tmp_path)
    assert restarted.configuration.get("llm.model") == "override"


async def test_root_override_files_are_not_loaded_or_migrated(tmp_path: Path) -> None:
    old = tmp_path / "config-overrides.json"
    old.write_text("not json")
    project = await RunnerProject.open(tmp_path)
    assert project.configuration.get("llm.model") is None
    assert old.read_text() == "not json"
    assert not (project.data_path / "config-overrides.json").exists()


async def test_changed_data_directory_uses_its_own_overrides(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.llm]\nmodel = "base"\n')
    project = await RunnerProject.open(tmp_path)
    await project.configuration.set("llm.model", "override", permanent=True)
    path.write_text('data_directory = "state"\n[components.llm]\nmodel = "base"\n')
    restarted = await RunnerProject.open(tmp_path)
    assert restarted.configuration.get("llm.model") == "base"
    await restarted.configuration.set("llm.model", "other", permanent=True)
    assert (await RunnerProject.open(tmp_path)).configuration.get(
        "llm.model"
    ) == "other"
    path.write_text('[components.llm]\nmodel = "base"\n')
    assert (await RunnerProject.open(tmp_path)).configuration.get(
        "llm.model"
    ) == "override"


async def test_last_override_replaces_previous_lifetime(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "base"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("llm.model", "permanent", permanent=True)
    assert (await RunnerProject.open(tmp_path)).configuration.get(
        "llm.model"
    ) == "permanent"
    await config.set("llm.model", "temporary")
    assert config.inspect("llm.model").source == "temporary"
    assert config.get("llm.model") == "temporary"
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm.model") == "base"
    await config.set("llm.model", "replacement", permanent=True)
    assert (await RunnerProject.open(tmp_path)).configuration.get(
        "llm.model"
    ) == "replacement"
    await config.reset("llm.model")
    assert config.get("llm.model") == "base"
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm.model") == "base"


async def test_invalidation_survives_restart_and_canonical_reversion(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.llm]\nmodel = "base"\nother = 1\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("llm.model", "override", permanent=True)
    path.write_text('[components.llm]\nother = 2\nmodel = "base"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("llm.model") == "override"
    path.write_text('[components.llm]\nmodel = "changed"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("llm.model") == "changed"
    path.write_text('[components.llm]\nmodel = "base"\n')
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm.model") == "base"


async def test_missing_type_changes_and_temporary_survival(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text("[components]\nvalue = 1\n")
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("missing", None, permanent=True)
    assert config.inspect("missing").exists
    await config.set("value", 7, permanent=True)
    await config.set("temporary", "temp")
    path.write_text(
        '[components]\nvalue = true\nmissing = "new"\ntemporary = "canonical"\n'
    )
    assert config.get("value") == 7
    assert config.get("temporary") == "temp"
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("value") is True
    assert config.get("missing") == "new"
    assert config.get("temporary") == "canonical"
    await config.set("value", 8, permanent=True)
    path.unlink()
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("value", "absent") == "absent"


async def test_subtrees_replace_conflicting_overrides(tmp_path: Path) -> None:
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("llm", {"model": "first", "temperature": 1}, permanent=True)
    await config.set("llm.model", "second")
    assert config.get("llm") == {"model": "second"}
    assert config.inspect("llm").source == "mixed"
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm") is None
    await config.set("llm", {"model": "third"}, permanent=True)
    assert config.inspect("llm.model").source == "permanent"
    await config.reset("llm.model")
    assert config.get("llm") is None


async def test_failed_write_keeps_previous_state(tmp_path: Path) -> None:
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("llm.model", "old", permanent=True)
    with patch.object(
        global_config, "atomic_write", side_effect=OSError("write failed")
    ):
        with pytest.raises(OSError):
            await config.set("llm.model", "temporary")
    assert config.get("llm.model") == "old"
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm.model") == "old"


async def test_concurrent_updates_and_memory_only_reads(tmp_path: Path) -> None:
    config = (await RunnerProject.open(tmp_path)).configuration
    await asyncio.gather(
        *(config.set(f"worker.key{i}", i, permanent=True) for i in range(10))
    )
    with patch.object(Path, "open", side_effect=AssertionError("file read")):
        for i in range(10):
            assert config.get(f"worker.key{i}") == i
    restarted = (await RunnerProject.open(tmp_path)).configuration
    assert restarted.get("worker") == {f"key{i}": i for i in range(10)}


async def test_toml_changes_are_ignored_until_restart(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components.llm]\nmodel = "base"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("llm.model", "override", permanent=True)
    replacement = tmp_path / "replacement.toml"
    replacement.write_text('[components.llm]\nmodel = "new"\n')
    replacement.replace(path)
    assert config.get("llm.model") == "override"
    await config.set("llm.model", "latest", permanent=True)
    assert config.get("llm.model") == "latest"
    assert (await RunnerProject.open(tmp_path)).configuration.get("llm.model") == "new"


async def test_override_read_replay_and_fresh_workflow(
    tmp_path: Path, history: MemoryHistory
) -> None:
    project = await RunnerProject.open(tmp_path)
    await project.configuration.set("llm.model", "first")

    async def workflow() -> pydantic.JsonValue:
        return await get_config(composition.workflow_context(), "llm.model")

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed" and first.outcome.result == "first"
    await project.configuration.set("llm.model", "second")
    assert (
        await replay.run(history, workflow, first.id, project=project)
    ).outcome == first.outcome
    fresh = await replay.run(MemoryHistory(), workflow, project=project)
    assert fresh.outcome.kind == "completed" and fresh.outcome.result == "second"


async def test_corrupt_persistence_is_not_silently_discarded(tmp_path: Path) -> None:
    project = await RunnerProject.open(tmp_path)
    path = project.data_path / "config-overrides.json"
    path.write_text("not json")
    with pytest.raises(pydantic.ValidationError):
        await RunnerProject.open(tmp_path)
    assert path.read_text() == "not json"


async def test_invalid_startup_preserves_permanent_override(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components]\nvalue = "base"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    await config.set("value", "override", permanent=True)
    path.write_text("invalid toml")
    with pytest.raises(ValueError):
        await RunnerProject.open(tmp_path)
    assert config.get("value") == "override"
    path.write_text('[components]\nvalue = "base"\n')
    assert (await RunnerProject.open(tmp_path)).configuration.get("value") == "override"
