import asyncio
from pathlib import Path
from typing import Optional
from unittest.mock import patch
from uuid import uuid4

import pydantic
import pytest

from tests.worktrees.conftest import git
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager


async def test_default_worktree_storage_is_gitignored(repository: Path) -> None:
    ignore = Path(__file__).resolve().parents[2] / ".gitignore"
    (repository / ".gitignore").write_text(ignore.read_text())
    project = await RunnerProject.open(repository, workspace_directory=".voc")
    record = await project.worktrees.create(m.Create(name="Ignored"), key="ignored")
    assert Path(record.path) == repository / ".voc/data/worktrees" / str(record.id)
    assert not (project.workspace_path / "worktrees").exists()
    ignored = [
        f".voc/data/worktrees/{record.id}/file",
        ".voc/data/worktrees/.locks",
        ".voc/data/workspace.sqlite3",
        ".voc/data/logs/example.jsonl",
    ]
    assert (
        await git(repository, "check-ignore", "--", *ignored)
    ).splitlines() == ignored
    assert (
        await git(
            repository,
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            ".voc/data",
        )
        == ""
    )


async def test_worktrees_follow_configured_data_directory(repository: Path) -> None:
    workspace = repository / ".voc"
    workspace.mkdir()
    (workspace / "config.toml").write_text('data_directory = "runtime/state"\n')
    project = await RunnerProject.open(repository, workspace_directory=".voc")
    record = await project.worktrees.create(
        m.Create(name="Custom data"), key="custom-data"
    )
    assert Path(record.path) == project.data_path / "worktrees" / str(record.id)
    assert project.data_path == workspace / "runtime/state"
    assert not (workspace / "worktrees").exists()
    assert not (workspace / "data").exists()
    async with project.worktrees.use(record.id, None) as target:
        assert target.root == record.path
    assert (
        await project.worktrees.delete(m.Delete(worktree_id=record.id))
    ).status == "deleted"


async def test_crud_and_idempotency(manager: Manager, repository: Path) -> None:
    request = m.Create(name="Fix the parser")
    record = await manager.create(request, key="first")
    assert record.status == "ready"
    assert record.path == str(manager.data_path / "worktrees" / str(record.id))
    assert not Path(record.path).is_relative_to(repository)
    assert record.branch == f"runner/worktree/{record.id}"
    assert (Path(record.path) / "file").read_text() == "committed\n"
    assert record.origin.kind == "user"
    assert record.inspection is not None and not record.inspection.dirty
    assert await manager.create(request, key="first") == record
    with pytest.raises(m.WorktreeError, match="different arguments"):
        await manager.create(m.Create(name="Other"), key="first")
    updated = await manager.update(
        m.Update(worktree_id=record.id, name="Parser feature", description="Keep this")
    )
    assert updated.name == "Parser feature" and updated.description == "Keep this"
    (Path(record.path) / "untracked").write_text("important")
    cached = await manager.get(m.Get(worktree_id=record.id))
    assert cached.inspection is not None and not cached.inspection.dirty
    refreshed = await manager.get(m.Get(worktree_id=record.id, refresh=True))
    assert refreshed.inspection is not None and refreshed.inspection.dirty
    with pytest.raises(m.WorktreeError, match="modified or untracked"):
        await manager.delete(m.Delete(worktree_id=record.id))
    assert (await manager.get(m.Get(worktree_id=record.id))).status == "ready"
    deleted = await manager.delete(m.Delete(worktree_id=record.id, force=True))
    assert deleted.status == "deleted" and not Path(record.path).exists()
    assert await manager.delete(m.Delete(worktree_id=record.id)) == deleted
    assert await manager.create(request, key="first") == deleted
    assert (await manager.listing(m.Query())).worktrees == []
    assert (await manager.listing(m.Query(include_deleted=True))).worktrees == [deleted]
    assert (
        await git(repository, "rev-parse", "refs/heads/" + record.branch)
        == record.base_commit
    )


async def test_limit_is_atomic_and_shared_between_managers(manager: Manager) -> None:
    settings = m.Settings(max_worktrees=1)
    owners = [
        Manager(manager.data_path, manager.target, manager.store.database, settings)
        for _ in range(2)
    ]
    results = await asyncio.gather(
        *(
            owner.create(m.Create(name=str(index)), key=str(index))
            for index, owner in enumerate(owners)
        ),
        return_exceptions=True,
    )
    records = [item for item in results if isinstance(item, m.Record)]
    errors = [item for item in results if isinstance(item, m.WorktreeError)]
    assert len(records) == len(errors) == 1
    assert "limit reached" in str(errors[0])
    await manager.delete(m.Delete(worktree_id=records[0].id))
    assert (
        await owners[0].create(m.Create(name="another"), key="another")
    ).status == "ready"


async def test_concurrent_idempotent_creation(manager: Manager) -> None:
    other = Manager(
        manager.data_path, manager.target, manager.store.database, manager.settings
    )
    request = m.Create(name="Same feature", branch="feature/concurrent")
    records = await asyncio.gather(
        manager.create(request, key="same"), other.create(request, key="same")
    )
    assert records[0].id == records[1].id
    assert len((await manager.listing(m.Query())).worktrees) == 1


async def test_live_and_abandoned_leases(manager: Manager) -> None:
    record = await manager.create(m.Create(name="Shared"), key="shared")
    async with manager.use(record.id, "first") as target:
        assert target.worktree_id == record.id and target.root == record.path
        async with manager.use(record.id, "second"):
            for force in (False, True):
                with pytest.raises(m.WorktreeError, match="in use"):
                    await manager.delete(m.Delete(worktree_id=record.id, force=force))
    assert (await manager.get(m.Get(worktree_id=record.id))).last_used_at is not None
    await manager.store.acquire(record.id, uuid4(), "crashed")
    with pytest.raises(m.WorktreeError, match="abandoned"):
        await manager.delete(m.Delete(worktree_id=record.id))
    assert (
        await manager.delete(m.Delete(worktree_id=record.id, force=True))
    ).status == "deleted"


async def test_interrupted_creation_reconciles_same_id(manager: Manager) -> None:
    original = manager.backend.create
    calls = 0

    async def interrupted(record: m.Record, *, recover: bool) -> None:
        nonlocal calls
        calls += 1
        await original(record, recover=recover)
        raise asyncio.CancelledError

    with patch.object(manager.backend, "create", interrupted):
        with pytest.raises(asyncio.CancelledError):
            await manager.create(m.Create(name="Interrupted"), key="same")
    pending = (await manager.listing(m.Query())).worktrees[0]
    assert pending.status == "creating"
    restored = await manager.create(m.Create(name="Interrupted"), key="same")
    assert restored.id == pending.id and restored.status == "ready" and calls == 1


async def test_named_branch_and_dirty_source(
    manager: Manager, repository: Path
) -> None:
    (repository / "file").write_text("not committed")
    request = m.Create(name="Human feature", branch="feature/test")
    record = await manager.create(request, key="named")
    assert (Path(record.path) / "file").read_text() == "committed\n"
    with pytest.raises(m.WorktreeError, match="already exists"):
        await manager.create(
            m.Create(name="Duplicate", branch="feature/test"), key="duplicate"
        )
    with pytest.raises(m.WorktreeError, match="already exists"):
        await manager.create(
            m.Create(name="Duplicate", branch="feature/test"), key="duplicate"
        )


async def test_non_repository_and_unavailable_git(
    manager: Manager, tmp_path: Path
) -> None:
    invalid = Manager(manager.data_path, tmp_path, manager.store.database, m.Settings())
    with pytest.raises(m.WorktreeError):
        await invalid.listing(m.Query())
    with pytest.raises(m.WorktreeError):
        await invalid.create(m.Create(name="no repository"), key="bad")
    unavailable = Manager(
        manager.data_path,
        manager.target,
        manager.store.database,
        m.Settings(git_executable="/nonexistent/git"),
    )
    with pytest.raises(m.WorktreeError, match="cannot run Git"):
        await unavailable.create(m.Create(name="no git"), key="bad")


@pytest.mark.parametrize(
    "values",
    [
        {"name": ""},
        {"name": " "},
        {"name": "bad\0name"},
        {"name": "ok", "base_ref": ""},
    ],
)
def test_invalid_create(values: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        m.Create.model_validate(values)


async def test_custom_backend_without_git(manager: Manager) -> None:
    class Backend:
        async def repository(self, target: Path) -> m.Repository:
            return m.Repository(root=str(target), identity="custom-repository")

        async def commit(self, repository: m.Repository, reference: str) -> str:
            return "custom-commit"

        async def validate_branch(self, repository: m.Repository, branch: str) -> None:
            pass

        async def create(self, record: m.Record, *, recover: bool) -> None:
            await asyncio.to_thread(Path(record.path).mkdir)

        async def inspect(self, record: m.Record) -> Optional[m.Inspection]:
            if not await asyncio.to_thread(Path(record.path).is_dir):
                return None
            return m.Inspection(
                head=record.base_commit, branch=record.branch, dirty=False
            )

        async def delete(self, record: m.Record, *, force: bool) -> None:
            await asyncio.to_thread(Path(record.path).rmdir)

    custom = Manager(
        manager.data_path,
        manager.target,
        manager.store.database,
        m.Settings(git_executable="/does/not/exist"),
        backend=Backend(),
    )
    record = await custom.create(m.Create(name="Custom"), key="custom")
    assert record.repository.identity == "custom-repository"
    async with custom.use(record.id, None) as target:
        assert target.root == record.path
    assert (await custom.delete(m.Delete(worktree_id=record.id))).status == "deleted"


async def test_storage_symlink_is_rejected(manager: Manager, tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    manager.directory.symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(m.WorktreeError, match="without symlinks"):
        await manager.create(m.Create(name="wrong directory"), key="wrong")
    assert not list(elsewhere.iterdir())


async def test_missing_registered_directory_can_be_explicitly_removed(
    manager: Manager,
) -> None:
    import shutil

    record = await manager.create(m.Create(name="Missing"), key="missing")
    await asyncio.to_thread(shutil.rmtree, record.path)
    refreshed = await manager.get(m.Get(worktree_id=record.id, refresh=True))
    assert refreshed.status == "missing"
    assert (
        await manager.delete(m.Delete(worktree_id=record.id, force=True))
    ).status == "deleted"
