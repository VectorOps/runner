"""POSIX Git executable adapter; commands never pass through a shell."""

import asyncio
import os
from pathlib import Path
import signal
from typing import Optional

from . import models as m


class GitBackend:
    def __init__(self, executable: str = "git", timeout_seconds: float = 120) -> None:
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    async def _run(self, root: str, *arguments: str) -> bytes:
        """Bound Git output and terminate the command group on timeout or cancellation."""
        env = dict(os.environ)
        for key in (
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_COMMON_DIR",
            "GIT_NAMESPACE",
        ):
            env.pop(key, None)
        env["GIT_TERMINAL_PROMPT"] = "0"
        try:
            process = await asyncio.create_subprocess_exec(
                self.executable,
                "-C",
                root,
                *arguments,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                stdin=asyncio.subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
        except OSError as exc:
            raise m.WorktreeError(f"cannot run Git: {exc}") from exc
        try:
            async with asyncio.timeout(self.timeout_seconds):
                assert process.stdout is not None
                output = await process.stdout.read(1024 * 1024 + 1)
                chunks = [output]
                size = len(output)
                while output and size <= 1024 * 1024:
                    output = await process.stdout.read(
                        min(65536, 1024 * 1024 + 1 - size)
                    )
                    chunks.append(output)
                    size += len(output)
                if size > 1024 * 1024:
                    raise m.WorktreeError("Git output exceeds limit")
                await process.wait()
            result = b"".join(chunks)
            if process.returncode:
                raise m.WorktreeError(result.decode(errors="replace").strip())
            return result
        except BaseException as exc:
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.communicate()
            if isinstance(exc, TimeoutError):
                raise m.WorktreeError("Git command timed out") from exc
            raise

    async def repository(self, target: Path) -> m.Repository:
        root = (
            (await self._run(str(target), "rev-parse", "--show-toplevel"))
            .decode()
            .strip()
        )
        identity = (
            (
                await self._run(
                    root, "rev-parse", "--path-format=absolute", "--git-common-dir"
                )
            )
            .decode()
            .strip()
        )
        return m.Repository(root=root, identity=identity)

    async def commit(self, repository: m.Repository, reference: str) -> str:
        return (
            (
                await self._run(
                    repository.root,
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    reference + "^{commit}",
                )
            )
            .decode()
            .strip()
        )

    async def _registered(self, record: m.Record) -> bool:
        listing = await self._run(
            record.repository.root, "worktree", "list", "--porcelain", "-z"
        )
        return os.fsencode("worktree " + record.path) in listing.split(b"\0")

    async def validate_branch(self, repository: m.Repository, branch: str) -> None:
        checked = (
            (await self._run(repository.root, "check-ref-format", "--branch", branch))
            .decode()
            .strip()
        )
        if checked != branch:
            raise m.WorktreeError("branch must be a literal new branch name")
        branches = (
            (
                await self._run(
                    repository.root,
                    "for-each-ref",
                    "--format=%(refname)",
                    "refs/heads/",
                )
            )
            .decode()
            .splitlines()
        )
        if "refs/heads/" + branch in branches:
            raise m.WorktreeError(
                "branch already exists; choose a new branch or omit branch"
            )

    async def inspect(self, record: m.Record) -> Optional[m.Inspection]:
        if not await self._registered(record):
            return None
        resolved = await asyncio.to_thread(Path(record.path).resolve)
        if str(resolved) != record.path:
            raise m.WorktreeError("registered worktree path is redirected")
        if not await asyncio.to_thread(resolved.exists):
            return None
        if not await asyncio.to_thread(resolved.is_dir):
            raise m.WorktreeError("registered worktree path is not a directory")
        repository = await self.repository(resolved)
        if (
            repository.identity != record.repository.identity
            or repository.root != record.path
        ):
            raise m.WorktreeError("worktree repository identity changed")
        head = await self.commit(repository, "HEAD")
        branch = (
            await self._run(record.path, "branch", "--show-current")
        ).decode().strip() or None
        dirty = bool(
            await self._run(
                record.path,
                "status",
                "--porcelain",
                "--untracked-files=normal",
                "--ignored=matching",
            )
        )
        return m.Inspection(head=head, branch=branch, dirty=dirty)

    async def create(self, record: m.Record, *, recover: bool) -> None:
        await self._run(
            record.repository.root, "check-ref-format", "--branch", record.branch
        )
        if await self._registered(record):
            inspection = await self.inspect(record)
            if inspection is None or inspection.branch != record.branch:
                raise m.WorktreeError(
                    "existing worktree does not match reserved branch"
                )
            return
        if await asyncio.to_thread(Path(record.path).exists):
            raise m.WorktreeError("refusing to replace an unregistered directory")
        branches = (
            (
                await self._run(
                    record.repository.root,
                    "for-each-ref",
                    "--format=%(refname)",
                    "refs/heads/",
                )
            )
            .decode()
            .splitlines()
        )
        if "refs/heads/" + record.branch in branches:
            if (
                not recover
                or await self.commit(record.repository, "refs/heads/" + record.branch)
                != record.base_commit
            ):
                raise m.WorktreeError(
                    "branch already exists; choose a new branch or omit branch"
                )
            await self._run(
                record.repository.root,
                "worktree",
                "add",
                "--",
                record.path,
                record.branch,
            )
        else:
            await self._run(
                record.repository.root,
                "worktree",
                "add",
                "-b",
                record.branch,
                "--",
                record.path,
                record.base_commit,
            )

    async def delete(self, record: m.Record, *, force: bool) -> None:
        if not await self._registered(record):
            if await asyncio.to_thread(os.path.lexists, record.path):
                raise m.WorktreeError("refusing to delete an unregistered directory")
            return
        inspection = await self.inspect(record)
        if inspection is not None and inspection.dirty and not force:
            raise m.WorktreeError(
                "worktree has modified or untracked files; force is required"
            )
        flags = ("--force", "--force") if force else ()
        await self._run(
            record.repository.root, "worktree", "remove", *flags, "--", record.path
        )
        if await self._registered(record) or await asyncio.to_thread(
            os.path.lexists, record.path
        ):
            raise m.WorktreeError(
                "Git worktree removal incomplete; checkout path or Git registration still exists"
            )
