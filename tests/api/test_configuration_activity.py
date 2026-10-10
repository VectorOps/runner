import asyncio
from pathlib import Path
from unittest.mock import patch

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.project import RunnerProject


async def test_paths_defaults_and_runtime_override(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.worker]\ncount = 0\nenabled = false\nnames = []\n[components.worker.nested]\nvalue = "old"\n'
    )
    project = await RunnerProject.open(tmp_path)

    async def workflow() -> pydantic.JsonValue:
        ctx = composition.workflow_context()
        assert await get_config(ctx, "worker.count", 5) == 0
        assert await get_config(ctx, "worker.enabled", True) is False
        assert await get_config(ctx, "worker.names", ["default"]) == []
        assert await get_config(ctx, "worker.count.missing", "fallback") == "fallback"
        assert await get_config(ctx, "absent") is None
        value = await get_config(ctx, "worker.nested", {})
        assert value == {"value": "old"}
        assert isinstance(value, dict)
        value["value"] = "mutated"
        await asyncio.to_thread(
            path.write_text, '[components.worker.nested]\nvalue = "fresh"\n'
        )
        assert project.configuration.get("worker.nested.value") == "old"
        await project.configuration.set("worker.nested.value", "fresh")
        return await get_config(ctx, "worker.nested.value")

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed" and first.outcome.result == "fresh"
    assert any(
        step.outcome.kind == "completed" and step.outcome.result == {"value": "old"}
        for step in history.path(first.id)
    )


async def test_replay_never_reopens_file_and_checks_inputs(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[components]\nvalue = "old"\n')
    project = await RunnerProject.open(tmp_path)
    name = "value"
    default = "fallback"

    async def workflow() -> pydantic.JsonValue:
        return await get_config(composition.workflow_context(), name, default)

    first = await replay.run(history, workflow, project=project)
    path.write_text("invalid TOML [")
    assert (
        await replay.run(history, workflow, first.id, project=project)
    ).outcome == first.outcome
    default = "different"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, project=project)
    default = "fallback"
    name = "different"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, project=project)


async def test_model_defaults_and_normalization(
    tmp_path: Path, history: MemoryHistory
) -> None:
    class Settings(pydantic.BaseModel):
        count: int = 2
        enabled: bool = True

    (tmp_path / "config.toml").write_text('[components.worker]\ncount = "3"\n')

    async def workflow() -> pydantic.JsonValue:
        ctx = composition.workflow_context()
        assert await get_config(ctx, "missing", {}, model=Settings) == {
            "count": 2,
            "enabled": True,
        }
        assert await get_config(ctx, "missing", {"count": "4"}, model=Settings) == {
            "count": 4,
            "enabled": True,
        }
        assert await get_config(ctx, "worker", {"enabled": False}, model=Settings) == {
            "count": 3,
            "enabled": True,
        }
        with pytest.raises(ActivityError, match="component configuration is invalid"):
            await get_config(ctx, "missing", model=Settings)
        return None

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed"


@pytest.mark.parametrize("from_default", [False, True])
async def test_model_validation_failure_does_not_record_payload(
    tmp_path: Path, history: MemoryHistory, from_default: bool
) -> None:
    class Settings(pydantic.BaseModel):
        count: int

    path = tmp_path / "config.toml"
    if not from_default:
        path.write_text('[components.worker]\ncount = "private-invalid-value"\n')
    project = await RunnerProject.open(tmp_path)
    fallback: pydantic.JsonValue = (
        {"count": "private-invalid-value"} if from_default else {}
    )

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ActivityError, match="component configuration is invalid"):
            await get_config(
                composition.workflow_context(), "worker", fallback, model=Settings
            )
        return None

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed"
    assert all(
        "private-invalid-value" not in step.model_dump_json()
        for step in history.path(first.id)
    )
    path.write_text("[components.worker]\ncount = 1\n")
    project = await RunnerProject.open(tmp_path)
    assert (
        await replay.run(history, workflow, first.id, project=project)
    ).outcome == first.outcome


async def test_model_and_fallback_changes_reject_replay(
    tmp_path: Path, history: MemoryHistory
) -> None:
    class Settings(pydantic.BaseModel):
        count: int = 2

    class OtherSettings(pydantic.BaseModel):
        count: str

    selected: type[pydantic.BaseModel] = Settings
    fallback: pydantic.JsonValue = {"count": "3"}

    async def workflow() -> pydantic.JsonValue:
        return await get_config(
            composition.workflow_context(), "missing", fallback, model=selected
        )

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed" and first.outcome.result == {"count": 3}
    fallback = {"count": "4"}
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    fallback = {"count": "3"}
    selected = OtherSettings
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id, workspace=tmp_path)


async def test_invalid_live_reads_are_recorded_failures(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text("[components]\nvalue = inf\n")

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ActivityError, match="finite"):
            await get_config(composition.workflow_context(), "value")
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text("[components]\nvalue = 3\n")
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome.kind == "completed"


async def test_explicit_context_without_ambient_context(
    tmp_path: Path, history: MemoryHistory
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "configured"\n[components.worker]\ncount = 0\n'
    )

    async def workflow() -> pydantic.JsonValue:
        ctx = composition.workflow_context()
        token = composition._CURRENT_CONTEXT.set(None)
        try:
            assert await get_config(ctx, "llm.model", "fallback") == "configured"
            assert await get_config(ctx, "missing", "fallback") == "fallback"
            assert await get_config(ctx, "missing") is None
            assert await get_config(ctx, "worker.count", 5) == 0
            value = await get_config(ctx, "worker")
            assert isinstance(value, dict)
            value["count"] = 9
            assert ctx.project.configuration.get("worker.count") == 0
            with pytest.raises(ValueError, match="nonempty"):
                await get_config(ctx, "worker..count")
            return await get_config(ctx, "worker")
        finally:
            composition._CURRENT_CONTEXT.reset(token)

    result = await replay.run(history, workflow, workspace=tmp_path)
    assert result.outcome.kind == "completed"
    assert result.outcome.result == {"count": 0}


async def test_explicit_context_replays_without_reading_configuration(
    tmp_path: Path, history: MemoryHistory
) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "old"\n')
    project = await RunnerProject.open(tmp_path)
    name = "llm.model"
    default = "fallback"

    async def workflow() -> pydantic.JsonValue:
        return await get_config(composition.workflow_context(), name, default)

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed" and first.outcome.result == "old"
    with patch.object(
        project.configuration, "get", side_effect=AssertionError("live read")
    ):
        assert (
            await replay.run(history, workflow, first.id, project=project)
        ).outcome == first.outcome
        default = "changed"
        with pytest.raises(ReplayMismatchError):
            await replay.run(history, workflow, first.id, project=project)
        default = "fallback"
        name = "other"
        with pytest.raises(ReplayMismatchError):
            await replay.run(history, workflow, first.id, project=project)


async def test_validated_replay_uses_history_not_current_snapshot(
    tmp_path: Path, history: MemoryHistory
) -> None:
    class Settings(pydantic.BaseModel):
        count: int = 2

    path = tmp_path / "config.toml"
    path.write_text('[components.worker]\ncount = "3"\n')
    project = await RunnerProject.open(tmp_path)

    async def workflow() -> pydantic.JsonValue:
        ctx = composition.workflow_context()
        token = composition._CURRENT_CONTEXT.set(None)
        try:
            return await get_config(ctx, "worker", {}, model=Settings)
        finally:
            composition._CURRENT_CONTEXT.reset(token)

    first = await replay.run(history, workflow, project=project)
    assert first.outcome.kind == "completed" and first.outcome.result == {"count": 3}
    path.write_text('[components.worker]\ncount = "invalid"\n')
    project = await RunnerProject.open(tmp_path)
    assert (
        await replay.run(history, workflow, first.id, project=project)
    ).outcome == first.outcome
