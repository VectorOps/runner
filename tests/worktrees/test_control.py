import asyncio
import json
from pathlib import Path

import pytest

from vectorops.runner.control import client, protocol as p
from vectorops.runner.worktrees import models as m


async def test_crud_protocol_and_workflow_start(
    workspace: Path, repository: Path
) -> None:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(repository))}\n'
    )
    async with client.embedded(workspace) as connection:
        request = m.CreateRequest(name="Human feature", idempotency_key="create")
        record = await connection.create_worktree(request)
        assert record.origin.kind == "user"
        assert (await connection.create_worktree(request)).id == record.id
        with pytest.raises(p.RemoteError, match="different arguments"):
            await connection.create_worktree(
                request.model_copy(update={"name": "Changed"})
            )
        assert [item.id for item in (await connection.list_worktrees()).worktrees] == [
            record.id
        ]
        assert (
            await connection.update_worktree(
                m.Update(worktree_id=record.id, name="Renamed")
            )
        ).name == "Renamed"
        accepted = await connection.start(
            p.Start(
                reference="worktree:leaf",
                idempotency_key="start",
                worktree_id=record.id,
            )
        )
        async with asyncio.timeout(10):
            while await connection.current(accepted.execution_id) is not None:
                await asyncio.sleep(0.01)
        execution = await connection.execution(accepted.execution_id)
        assert execution.status == "completed"
        assert (
            execution.target is not None and execution.target.worktree_id == record.id
        )
        assert (
            await connection.get_worktree(m.Get(worktree_id=record.id, refresh=True))
        ).status == "ready"
    async with client.embedded(workspace) as connection:
        assert (await connection.create_worktree(request)).id == record.id
        assert (
            await connection.delete_worktree(m.Delete(worktree_id=record.id))
        ).status == "deleted"
        assert (await connection.list_worktrees()).worktrees == []


async def test_protocol_rejects_conflicting_target(
    workspace: Path, repository: Path
) -> None:
    async with client.embedded(workspace) as connection:
        with pytest.raises(p.RemoteError) as error:
            await connection.request(
                "executions.start",
                {
                    "reference": "worktree:leaf",
                    "idempotency_key": "bad",
                    "worktree_id": "00000000-0000-0000-0000-000000000001",
                    "base_project_path": str(repository),
                },
            )
        assert error.value.code == "invalid_argument"
        with pytest.raises(p.RemoteError) as error:
            await connection.request("worktrees.list")
        assert error.value.code == "worktree_error"
