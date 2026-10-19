"""Workspace worktree lifecycle, idempotency, and protected target attachment."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner.locking import FileLease
from vectorops.runner.runner.resources import finish_cleanup
from vectorops.runner.runner.workspace_database import WorkspaceDatabase

from . import models as m
from .backend import WorktreeBackend
from .git import GitBackend
from .store import Store


class Manager:
    def __init__(
        self,
        workspace: Path,
        target: Path,
        database: WorkspaceDatabase,
        settings: m.Settings,
        backend: Optional[WorktreeBackend] = None,
    ) -> None:
        self.workspace = workspace
        self.target = target
        self.directory = workspace / "worktrees"
        self.settings = settings
        self.backend = (
            backend
            if backend is not None
            else GitBackend(settings.git_executable, settings.timeout_seconds)
        )
        self.store = Store(database)

    async def _prepare(self) -> None:
        def prepare() -> None:
            if (
                self.directory.resolve() != self.directory
                or not self.directory.is_relative_to(self.workspace)
            ):
                raise m.WorktreeError(
                    "worktree storage must remain inside the workspace without symlinks"
                )
            self.directory.mkdir(parents=True, exist_ok=True)
            locks = self.directory / ".locks"
            if locks.resolve() != locks:
                raise m.WorktreeError("worktree locks directory must not be a symlink")
            locks.mkdir(exist_ok=True)

        await asyncio.to_thread(prepare)

    @asynccontextmanager
    async def _lock(self, name: str) -> AsyncIterator[None]:
        await self._prepare()
        lease = FileLease(self.directory / ".locks" / name)
        try:
            async with asyncio.timeout(self.settings.timeout_seconds):
                while True:
                    try:
                        await finish_cleanup(
                            asyncio.create_task(asyncio.to_thread(lease.acquire))
                        )
                        break
                    except RuntimeError:
                        await asyncio.sleep(0.01)
            yield
        finally:
            await finish_cleanup(asyncio.create_task(asyncio.to_thread(lease.release)))

    def _check_path(self, record: m.Record) -> None:
        if record.path != str(self.directory / str(record.id)):
            raise m.WorktreeError(
                "worktree path differs from its workspace reservation"
            )

    async def create(
        self, request: m.Create, *, key: str, origin: Optional[m.Origin] = None
    ) -> m.Record:
        return await finish_cleanup(
            asyncio.create_task(self._create(request, key, origin or m.Origin()))
        )

    async def _create(self, request: m.Create, key: str, origin: m.Origin) -> m.Record:
        """Reserve identity before Git effects; replay a reserved operation after interruption."""
        repository = await self.backend.repository(self.target)
        await self._prepare()
        fingerprint = json.dumps(
            [request.model_dump(mode="json"), origin.model_dump(mode="json")],
            sort_keys=True,
        )
        record = await self.store.lookup(key, fingerprint)
        fresh = False
        if record is None:
            identifier = uuid4()
            record = m.Record(
                id=identifier,
                name=request.name,
                description=request.description,
                path=str(self.directory / str(identifier)),
                repository=repository,
                branch=request.branch or f"runner/worktree/{identifier}",
                base_commit=await self.backend.commit(repository, request.base_ref),
                origin=origin,
                status="creating",
            )
            try:
                await self.backend.validate_branch(repository, record.branch)
            except m.WorktreeError:
                concurrent = await self.store.lookup(key, fingerprint)
                if concurrent is None:
                    raise
                record = concurrent
            else:
                record, fresh = await self.store.reserve(
                    record, key, fingerprint, self.settings.max_worktrees or None
                )
        async with self._lock(f"operation-{record.id}"):
            record = await self.store.get(record.id)
            self._check_path(record)
            if record.status == "failed":
                raise m.WorktreeError(
                    record.error
                    or "worktree creation failed; delete it before retrying with a new key"
                )
            if record.status != "creating":
                return record
            try:
                await self.backend.create(record, recover=not fresh)
                inspection = await self.backend.inspect(record)
                if inspection is None:
                    raise m.WorktreeError(
                        "backend did not create the reserved worktree"
                    )
                return await self.store.change(
                    record.id,
                    lambda current: current.model_copy(
                        update={
                            "status": "ready",
                            "inspection": inspection,
                            "error": None,
                            "updated_at": m.timestamp(),
                        }
                    ),
                )
            except Exception as exc:
                await self.store.change(
                    record.id,
                    lambda current: current.model_copy(
                        update={
                            "status": "failed",
                            "error": str(exc),
                            "updated_at": m.timestamp(),
                        }
                    ),
                )
                raise

    async def get(self, request: m.Get) -> m.Record:
        await self.backend.repository(self.target)
        record = await self.store.get(request.worktree_id)
        if not request.refresh or record.status == "deleted":
            return record
        async with self._lock(f"operation-{record.id}"):
            record = await self.store.get(record.id)
            if record.status == "deleted":
                return record
            self._check_path(record)
            try:
                inspection = await self.backend.inspect(record)
                status = "ready" if inspection is not None else "missing"
                if record.status in ("creating", "deleting", "failed"):
                    status = record.status
                return await self.store.change(
                    record.id,
                    lambda current: current.model_copy(
                        update={
                            "inspection": inspection,
                            "status": status,
                            "error": None,
                        }
                    ),
                )
            except Exception as exc:
                return await self.store.change(
                    record.id,
                    lambda current: current.model_copy(update={"error": str(exc)}),
                )

    async def listing(self, query: m.Query) -> m.Page:
        await self.backend.repository(self.target)
        page = await self.store.page(query)
        if query.refresh:
            return m.Page(
                worktrees=[
                    await self.get(m.Get(worktree_id=record.id, refresh=True))
                    for record in page.worktrees
                ],
                next_after=page.next_after,
            )
        return page

    async def update(self, request: m.Update) -> m.Record:
        await self.backend.repository(self.target)

        def update(record: m.Record) -> m.Record:
            if record.status == "deleted":
                raise m.WorktreeError("worktree has been deleted")
            return record.model_copy(
                update={
                    "name": request.name,
                    "description": (
                        record.description
                        if request.description is None
                        else request.description
                    ),
                    "updated_at": m.timestamp(),
                }
            )

        return await self.store.change(request.worktree_id, update)

    async def delete(self, request: m.Delete) -> m.Record:
        return await finish_cleanup(asyncio.create_task(self._delete(request)))

    async def _delete(self, request: m.Delete) -> m.Record:
        """Serialize removal against attachment; stale crash leases require explicit force."""
        await self.backend.repository(self.target)
        async with self._lock(f"operation-{request.worktree_id}"):
            record = await self.store.get(request.worktree_id)
            self._check_path(record)
            if record.status == "deleted":
                return record
            for token in await self.store.uses(record.id):
                lease = FileLease(self.directory / ".locks" / f"use-{token}")
                try:
                    await asyncio.to_thread(lease.acquire)
                except RuntimeError as exc:
                    raise m.WorktreeError(
                        "worktree is in use by a workflow or process"
                    ) from exc
                finally:
                    await asyncio.to_thread(lease.release)
                if not request.force:
                    raise m.WorktreeError(
                        "worktree has an abandoned usage lease; inspect orphaned processes before force removal"
                    )
                await self.store.release(token)
            previous = record.status
            record = await self.store.change(
                record.id,
                lambda current: current.model_copy(
                    update={"status": "deleting", "updated_at": m.timestamp()}
                ),
            )
            try:
                await self.backend.delete(record, force=request.force)
            except Exception as exc:
                await self.store.change(
                    record.id,
                    lambda current: current.model_copy(
                        update={
                            "status": previous,
                            "error": str(exc),
                            "updated_at": m.timestamp(),
                        }
                    ),
                )
                raise
            return await self.store.change(
                record.id,
                lambda current: current.model_copy(
                    update={
                        "status": "deleted",
                        "error": None,
                        "updated_at": m.timestamp(),
                    }
                ),
            )

    async def owning_worktree(self, target: LocalExecutionTarget) -> Optional[UUID]:
        """Identify registered raw-path targets without changing their selected root."""
        if target.worktree_id is not None:
            return target.worktree_id
        try:
            relative = Path(target.root).relative_to(self.directory)
            identifier = UUID(relative.parts[0])
        except (ValueError, IndexError):
            return None
        record = await self.store.find(identifier)
        if record is None:
            return None
        self._check_path(record)
        return identifier if Path(target.root).is_relative_to(record.path) else None

    async def resolve(self, identifier: UUID) -> LocalExecutionTarget:
        record = await self.get(m.Get(worktree_id=identifier))
        self._check_path(record)
        if record.status != "ready":
            raise m.WorktreeError(f"worktree is not ready: {record.status}")
        target = await LocalExecutionTarget.resolve(record.path)
        if target.root != record.path:
            raise m.WorktreeError("worktree path has been redirected")
        if await self.backend.inspect(record) is None:
            raise m.WorktreeError("worktree is missing from its repository")
        return target.model_copy(update={"worktree_id": identifier})

    @asynccontextmanager
    async def use(
        self, identifier: UUID, execution_id: Optional[str]
    ) -> AsyncIterator[LocalExecutionTarget]:
        token = uuid4()
        await self._prepare()
        lease = FileLease(self.directory / ".locks" / f"use-{token}")
        acquired = False
        try:
            await finish_cleanup(asyncio.create_task(asyncio.to_thread(lease.acquire)))
            async with self._lock(f"operation-{identifier}"):
                target = await self.resolve(identifier)

                async def acquire() -> None:
                    nonlocal acquired
                    await self.store.acquire(identifier, token, execution_id)
                    acquired = True

                await finish_cleanup(asyncio.create_task(acquire()))
            try:
                yield target
            except BaseException:
                acquired = False
                raise
        finally:
            try:
                if acquired:
                    await finish_cleanup(asyncio.create_task(self.store.release(token)))
            finally:
                await finish_cleanup(
                    asyncio.create_task(asyncio.to_thread(lease.release))
                )
