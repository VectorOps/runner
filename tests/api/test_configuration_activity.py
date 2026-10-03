import asyncio
from pathlib import Path

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import configuration
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError


async def test_paths_defaults_and_live_reads(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.worker]\ncount = 0\nenabled = false\nnames = []\n[components.worker.nested]\nvalue = "old"\n'
    )

    async def workflow() -> pydantic.JsonValue:
        assert await configuration.get("worker.count", 5) == 0
        assert await configuration.get("worker.enabled", True) is False
        assert await configuration.get("worker.names", ["default"]) == []
        assert await configuration.get("worker.count.missing", "fallback") == "fallback"
        assert await configuration.get("absent") is None
        value = await configuration.get("worker.nested", {})
        assert value == {"value": "old"}
        assert isinstance(value, dict)
        value["value"] = "mutated"
        await asyncio.to_thread(
            path.write_text, '[components.worker.nested]\nvalue = "fresh"\n'
        )
        return await configuration.get("worker.nested.value")

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed" and first.outcome.result == "fresh"
    assert any(
        step.outcome.kind == "completed" and step.outcome.result == {"value": "old"}
        for step in history.path(first.id)
    )
    path.unlink()
    second = await replay.run(history, workflow, first.id, workspace=tmp_path)
    assert second.outcome == first.outcome


async def test_replay_never_reopens_file_and_checks_inputs(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components]\nvalue = "old"\n')
    name = "value"
    default = "fallback"

    async def workflow() -> pydantic.JsonValue:
        return await configuration.get(name, default)

    first = await replay.run(history, workflow, workspace=tmp_path)
    path.write_text("invalid TOML [")
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    default = "different"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    default = "fallback"
    name = "different"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, workspace=tmp_path)


async def test_invalid_live_reads_are_recorded_failures(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text("[components]\nvalue = inf\n")

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ActivityError, match="finite"):
            await configuration.get("value")
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text("[components]\nvalue = 3\n")
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome.kind == "completed"


async def test_configuration_requires_workflow_context() -> None:
    with pytest.raises(RuntimeError, match="active workflow"):
        await configuration.get("name")
