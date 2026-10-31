import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.control.test_client_commands import command, settled
from tests.control.test_mailbox import pending, response
from tests.control.test_paused import status
from tests.fixtures import sources
from vectorops.runner.control import client, protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.mark.parametrize("cold", [False, True])
async def test_delete_paused_execution(tmp_path: Path, cold: bool) -> None:
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(
                reference="python:tests.fixtures.paused_workflows:children",
                idempotency_key="paused",
            )
        )
        identifier = accepted.execution_id
        requests = await pending(connection, identifier, 2)
        await status(connection, identifier, "paused")
        if not cold:
            assert (await connection.delete_execution(identifier)).deleted
            assert await connection.current(identifier) is None
            with pytest.raises(p.RemoteError) as error:
                await connection.answer_input(response(requests[0]))
            assert error.value.code == "not_found"
    async with client.embedded(tmp_path) as connection:
        if cold:
            assert (await connection.delete_execution(identifier)).deleted
        assert not (tmp_path / "data/executions" / identifier).exists()
        assert not (await connection.executions()).execution_ids
        assert not (await connection.execution_summaries()).executions
        assert not (await connection.delete_execution(identifier)).deleted
        with pytest.raises(p.RemoteError) as error:
            await connection.resume(
                p.Resume(execution_id=identifier, idempotency_key="resume")
            )
        assert error.value.code == "not_found"


async def test_delete_terminal_cleans_records_and_receipts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/value.py").write_text(sources.read("value.py"))
    params = p.Start(reference="value:main", idempotency_key="first")
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(params)
        await settled(connection, accepted.execution_id)
        other = await connection.start(
            params.model_copy(update={"idempotency_key": "other"})
        )
        await settled(connection, other.execution_id)
        await command(connection, "execution", "delete", accepted.execution_id)
        assert (
            f"Permanently deleted execution '{accepted.execution_id}'."
            in capsys.readouterr().out
        )
        assert (await connection.executions()).execution_ids == [other.execution_id]
        assert (await connection.execution(other.execution_id)).status == "completed"
        runner = WorkflowRunner(tmp_path)
        assert (await runner.project_state()).last_execution_id == other.execution_id
        project = await runner.project()
        for table in (
            "execution_index",
            "execution_summaries",
            "execution_targets",
            "usage_confirmed_executions",
            "worktree_uses",
        ):
            count = await project.database.run(
                lambda connection: connection.execute(
                    f"SELECT count(*) FROM {table} WHERE execution_id = ?",
                    (accepted.execution_id,),
                ).fetchone()[0]
            )
            assert count == 0
        assert await project.database.receipt("first") is None
        assert await project.database.receipt("other") is not None
        await connection.delete_execution(other.execution_id)
        pointer = await runner.project_state()
        assert pointer.last_execution_id is None
        assert not pointer.active_execution_ids
        replacement = await connection.start(params)
        assert replacement.execution_id != accepted.execution_id
        await settled(connection, replacement.execution_id)


async def test_deletion_uses_durable_pause_state_before_event_delivery(
    tmp_path: Path,
) -> None:
    async with RunnerService(tmp_path) as service:
        accepted = p.Accepted.model_validate(
            await service.dispatch(
                "executions.start",
                p.Start(
                    reference="python:tests.fixtures.paused_workflows:children",
                    idempotency_key="paused",
                ).model_dump(mode="json"),
            )
        )
        identifier = accepted.execution_id
        async with asyncio.timeout(5):
            while True:
                try:
                    if (
                        await service.runner.execution_state(identifier)
                    ).status == "paused":
                        break
                except (FileNotFoundError, KeyError):
                    pass
                await asyncio.sleep(0.01)
        service._active[identifier] = p.Active(
            execution_id=identifier, status="running"
        )
        assert (await service._delete_execution(identifier)).deleted
        assert identifier not in service._active


async def test_running_and_starting_executions_cannot_be_deleted(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/wait.py").write_text(sources.read("wait.py"))
    async with RunnerService(tmp_path) as service:
        original = service.runner.run
        gate = asyncio.Event()

        async def delayed(*args: object, **kwargs: object) -> None:
            await gate.wait()

        with patch.object(service.runner, "run", side_effect=delayed):
            accepted = p.Accepted.model_validate(
                await service.dispatch(
                    "executions.start",
                    p.Start(reference="wait:main", idempotency_key="start").model_dump(
                        mode="json"
                    ),
                )
            )
            with pytest.raises(p.RemoteError) as error:
                await service.dispatch(
                    "executions.delete", {"execution_id": accepted.execution_id}
                )
            assert error.value.code == "execution_conflict"
            gate.set()
            await asyncio.gather(*service._tasks.values())
        assert service.runner.run == original
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="wait:main", idempotency_key="running")
        )
        async with asyncio.timeout(5):
            while True:
                active = await connection.current(accepted.execution_id)
                assert active is not None
                if active.status != "starting":
                    break
                await asyncio.sleep(0.01)
        with pytest.raises(p.RemoteError) as error:
            await connection.delete_execution(accepted.execution_id)
        assert error.value.code == "execution_conflict"
        assert (await connection.execution(accepted.execution_id)).status == "running"
        await connection.stop(accepted.execution_id)
        await settled(connection, accepted.execution_id)
        assert (await connection.delete_execution(accepted.execution_id)).deleted


@pytest.mark.parametrize("identifier", ["../other", "..", "/tmp/other", ""])
async def test_delete_rejects_invalid_identifiers(
    tmp_path: Path, identifier: str
) -> None:
    async with client.embedded(tmp_path) as connection:
        with pytest.raises(p.RemoteError) as error:
            await connection.delete_execution(identifier)
        assert error.value.code == "invalid_argument"


async def test_delete_rejects_redirected_directory(tmp_path: Path) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    directory = project.data_path / "executions"
    directory.mkdir(parents=True)
    other = tmp_path / "other"
    other.mkdir()
    (other / "keep").touch()
    (directory / "redirected").symlink_to(other, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinks"):
        await runner.remove_execution("redirected")
    assert (other / "keep").exists()
