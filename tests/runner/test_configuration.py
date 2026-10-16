import asyncio
from pathlib import Path
import tomllib
from unittest.mock import patch

import pydantic
import pytest

from vectorops.runner.control import client, protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.project import Configuration, RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_shared_configuration_is_parsed_once(workspace: Path) -> None:
    path = workspace / "config.toml"
    path.write_text('[components.llm]\nmodel = "first"\n')
    with patch.object(Configuration, "read", wraps=Configuration.read) as read:
        async with RunnerService(workspace) as service:
            projects = await asyncio.gather(
                *(service.runner.project() for _ in range(10))
            )
            project = projects[0]
            assert all(item is project for item in projects)
            assert service.mcp.configuration is project.configuration
            path.write_text('[components.llm]\nmodel = "second"\n')
            for _ in range(2):
                result = await service.runner.run("configuration:main")
                assert isinstance(result, dict) and result["model"] == "first"
            assert read.call_count == 1
            await project.configuration.set("llm.model", "override")
            result = await service.runner.run("configuration:main")
            assert isinstance(result, dict) and result["model"] == "override"
            assert read.call_count == 1
    assert (await WorkflowRunner(workspace).project()).configuration.get(
        "llm.model"
    ) == "second"


async def test_concurrent_runner_initialization(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    with patch.object(Configuration, "read", wraps=Configuration.read) as read:
        projects = await asyncio.gather(*(runner.project() for _ in range(10)))
    assert all(project is projects[0] for project in projects)
    assert read.call_count == 1


async def test_internal_configuration_snapshots_and_restart(tmp_path: Path) -> None:
    class Settings(pydantic.BaseModel):
        count: int = 2

    path = tmp_path / "config.toml"
    path.write_text('[components.worker]\ncount = 1\nnames = ["first"]\n')
    project = await RunnerProject.open(tmp_path)
    config = project.configuration
    assert config is project.configuration
    assert config.get_validated("worker", Settings).count == 1
    assert config.get_validated("missing", Settings).count == 2
    value = config.get("worker")
    assert isinstance(value, dict)
    value["count"] = 9
    assert config.get("worker.count") == 1
    fallback: list[pydantic.JsonValue] = ["fallback"]
    result = config.get("missing", fallback)
    assert isinstance(result, list)
    result.append("changed")
    assert fallback == ["fallback"]
    path.write_text("invalid TOML [")
    with pytest.raises(tomllib.TOMLDecodeError):
        await RunnerProject.open(tmp_path)
    assert config.get("worker.count") == 1
    path.write_text('data_directory = "other"\n[components.worker]\ncount = 3\n')
    assert config.get("worker.count") == 1
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("worker.count") == 3
    assert project.data_path == tmp_path / "data"
    path.write_text('[components.worker]\ncount = "secret-invalid-value"\n')
    config = (await RunnerProject.open(tmp_path)).configuration
    with pytest.raises(ValueError, match="^component configuration is invalid$"):
        config.get_validated("worker", Settings)
    path.unlink()
    config = (await RunnerProject.open(tmp_path)).configuration
    assert config.get("worker") is None
    with pytest.raises(ValueError, match="nonempty"):
        config.get("worker..count")


async def test_workspace_configuration_isolation(tmp_path: Path) -> None:
    other = tmp_path / "other"
    other.mkdir()
    (tmp_path / "config.toml").write_text('[components]\nname = "first"\n')
    (other / "config.toml").write_text('[components]\nname = "second"\n')
    first, second = await asyncio.gather(
        WorkflowRunner(tmp_path).project(), WorkflowRunner(other).project()
    )
    assert first.configuration is not second.configuration
    assert first.configuration.get("name") == "first"
    assert second.configuration.get("name") == "second"


async def test_completed_children_replay_old_settings(workspace: Path) -> None:
    config = workspace / "config.toml"
    config.write_text('[components.llm]\nmodel = "first"\n')
    runner = WorkflowRunner(workspace)
    result = await runner.run("configuration:children")
    config.write_text('[components.llm]\nmodel = "second"\n')
    assert await WorkflowRunner(workspace).resume() == result
    updated = await WorkflowRunner(workspace).run("configuration:main")
    assert isinstance(updated, dict) and updated["model"] == "second"


async def test_api_updates_fresh_reads_without_reinitializing(workspace: Path) -> None:
    (workspace / "config.toml").write_text('[components.llm]\nmodel = "first"\n')
    async with RunnerService(workspace) as service:
        project = await service.runner.project()
        first = await service.runner.run("configuration:main")
        await service.dispatch(
            "configuration.set",
            {"name": "components.llm.model", "value": "second"},
        )
        assert await service.runner.project() is project
        assert project.configuration.get("llm.model") == "second"
        assert isinstance(first, dict) and first["model"] == "first"
        assert await service.runner.resume() == first
        fresh = await service.runner.run("configuration:main")
        assert isinstance(fresh, dict) and fresh["model"] == "second"


async def test_resume_replays_old_read_then_reads_fresh_settings(
    workspace: Path,
) -> None:
    path = workspace / "config.toml"
    path.write_text('[components.llm]\nmodel = "first"\n')
    async with client.embedded(workspace) as host:
        accepted = await host.start(
            p.Start(
                reference="configuration:changing",
                idempotency_key="config",
            )
        )
        async with asyncio.timeout(5):
            while True:
                try:
                    pending = await host.pending_inputs(
                        mailbox.RequestQuery(execution_id=accepted.execution_id)
                    )
                except p.RemoteError as exc:
                    if exc.code != "not_found":
                        raise
                    await asyncio.sleep(0.01)
                    continue
                if pending.requests:
                    break
                await asyncio.sleep(0.01)
    path.write_text('[components.llm]\nmodel = "second"\n')
    async with client.embedded(workspace) as host:
        await host.answer_input(
            mailbox.Answer(
                execution_id=accepted.execution_id,
                request_id=pending.requests[0].request_id,
                response="yes",
                idempotency_key="answer",
            )
        )
        async with asyncio.timeout(5):
            while await host.current() is not None:
                await asyncio.sleep(0.01)
    assert await WorkflowRunner(workspace).resume(accepted.execution_id) == [
        "first",
        "second",
    ]
