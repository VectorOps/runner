import asyncio
import json
from pathlib import Path
import shutil

import pytest

from tests.worktrees.conftest import git
from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.git import GitBackend


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
        assert not Path(record.path).exists()
        assert record.path not in await git(
            repository, "worktree", "list", "--porcelain"
        )


@pytest.mark.parametrize("reason", ["modified", "untracked", "locked"])
async def test_failed_deletion_keeps_worktree_listed(
    worktree_project: RunnerProject, repository: Path, reason: str
) -> None:
    async with client.embedded(worktree_project.root) as connection:
        record = await connection.create_worktree(
            m.CreateRequest(name="Protected", idempotency_key="protected")
        )
        if reason == "locked":
            await git(repository, "worktree", "lock", record.path)
        else:
            filename = "file" if reason == "modified" else "untracked"
            (Path(record.path) / filename).write_text("keep this")
        with pytest.raises(p.RemoteError) as error:
            await connection.delete_worktree(m.Delete(worktree_id=record.id))
        assert error.value.code == "worktree_error"
        remaining = (await connection.list_worktrees()).worktrees
        assert len(remaining) == 1 and remaining[0].id == record.id
        assert remaining[0].status == "ready" and remaining[0].error
        assert Path(record.path).is_dir()
        assert record.path in await git(repository, "worktree", "list", "--porcelain")
        if reason != "locked":
            assert (Path(record.path) / filename).read_text() == "keep this"
        assert (
            await connection.delete_worktree(
                m.Delete(worktree_id=record.id, force=True)
            )
        ).status == "deleted"
        assert not Path(record.path).exists()
        assert record.path not in await git(
            repository, "worktree", "list", "--porcelain"
        )


async def test_force_deletion_removes_all_checkout_contents(
    worktree_project: RunnerProject, repository: Path, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep this")
    async with client.embedded(worktree_project.root) as connection:
        record = await connection.create_worktree(
            m.CreateRequest(name="Discard", idempotency_key="discard")
        )
        other = await connection.create_worktree(
            m.CreateRequest(name="Keep", idempotency_key="keep")
        )
        checkout = Path(record.path)
        (checkout / "file").write_text("staged changes")
        (checkout / ".gitignore").write_text("build/\n")
        await git(checkout, "add", "file", ".gitignore")
        (checkout / "file").write_text("unstaged changes")
        (checkout / ".untracked").write_text("untracked changes")
        (checkout / "build").mkdir()
        (checkout / "build" / "ignored").write_text("ignored content")
        nested = checkout / "nested"
        nested.mkdir()
        await git(nested, "init")
        (nested / "untracked").write_text("nested repository content")
        await git(
            checkout,
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            str(repository),
            "submodule",
        )
        (checkout / "submodule" / "file").write_text("submodule changes")
        (checkout / "external").symlink_to(outside, target_is_directory=True)
        with pytest.raises(p.RemoteError, match="force is required"):
            await connection.delete_worktree(m.Delete(worktree_id=record.id))
        assert (
            await connection.delete_worktree(
                m.Delete(worktree_id=record.id, force=True)
            )
        ).status == "deleted"
        assert not checkout.exists()
        registered = await git(repository, "worktree", "list", "--porcelain")
        assert record.path not in registered and other.path in registered
        assert [item.id for item in (await connection.list_worktrees()).worktrees] == [
            other.id
        ]
        assert (Path(other.path) / "file").read_text() == "committed\n"
        assert (repository / "file").read_text() == "committed\n"
        assert (outside / "keep").read_text() == "keep this"
        assert (
            await git(repository, "rev-parse", "refs/heads/" + record.branch)
            == record.base_commit
        )


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("leftover", ["both", "registration", "directory", "symlink"])
async def test_incomplete_git_removal_does_not_report_deletion(
    worktree_project: RunnerProject,
    monkeypatch: pytest.MonkeyPatch,
    leftover: str,
    force: bool,
) -> None:
    original = GitBackend._run

    async def run(backend: GitBackend, root: str, *arguments: str) -> bytes:
        if arguments[:2] != ("worktree", "remove"):
            return await original(backend, root, *arguments)
        path = Path(arguments[-1])
        if leftover == "registration":
            await asyncio.to_thread(shutil.rmtree, path)
        elif leftover in ("directory", "symlink"):
            await original(backend, root, *arguments)
            if leftover == "directory":
                path.mkdir()
            else:
                path.symlink_to(path.parent / "missing")
        return b""

    async with client.embedded(worktree_project.root) as connection:
        record = await connection.create_worktree(
            m.CreateRequest(name="Incomplete", idempotency_key="incomplete")
        )
        monkeypatch.setattr(GitBackend, "_run", run)
        with pytest.raises(p.RemoteError, match="removal incomplete") as error:
            await connection.delete_worktree(
                m.Delete(worktree_id=record.id, force=force)
            )
        assert error.value.code == "worktree_error"
        remaining = (await connection.list_worktrees()).worktrees
        assert len(remaining) == 1 and remaining[0].id == record.id
        assert remaining[0].status == "ready"
        assert remaining[0].error and "removal incomplete" in remaining[0].error
        if leftover in ("directory", "symlink"):
            with pytest.raises(p.RemoteError, match="unregistered directory"):
                await connection.delete_worktree(
                    m.Delete(worktree_id=record.id, force=True)
                )
            assert (await connection.list_worktrees()).worktrees[0].status == "ready"


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
