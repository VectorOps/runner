"""Conservative completed-history retention under project writer ownership."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
import shutil
import sqlite3
import typing

from vectorops.runner import diagnostics
from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.project import RetentionPolicy
from vectorops.runner.runner.sqlite_history import SQLiteHistory

_LOGGER: typing.Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CompletedHistory:
    directory: Path
    execution: ExecutionRecord
    completed_at: datetime


class ExecutionRetention:
    def __init__(self, data_path: Path, policy: RetentionPolicy) -> None:
        self.data_path = data_path
        self.policy = policy

    def _directories(self) -> list[Path]:
        root = self.data_path / "executions"
        if not root.exists() or root.is_symlink() or not root.is_dir():
            return []
        return [
            directory
            for directory in root.iterdir()
            if directory.is_dir()
            and not directory.is_symlink()
            and (directory / "history.sqlite3").is_file()
            and not (directory / "history.sqlite3").is_symlink()
        ]

    async def apply(
        self,
        last_execution_id: typing.Optional[str],
        *,
        now: typing.Optional[datetime] = None,
        protected_ids: typing.AbstractSet[str] = frozenset(),
    ) -> list[str]:
        """Delete completed histories exceeding either limit; caller owns project lock."""
        if (
            self.policy.max_age_days is None
            and self.policy.max_completed_executions is None
        ):
            return []
        current = datetime.now(timezone.utc) if now is None else now
        if current.tzinfo is None:
            raise ValueError("retention clock must be timezone-aware")
        histories: list[_CompletedHistory] = []
        try:
            directories = await asyncio.to_thread(self._directories)
        except OSError as exc:
            diagnostics.log(
                "retention.scan_failed", level=diagnostics.Level.WARNING, error=exc
            )
            _LOGGER.warning("Could not scan execution histories: %s", exc)
            return []
        for directory in directories:
            try:
                execution = await SQLiteHistory(
                    directory / "history.sqlite3"
                ).get_execution()
                if execution.id != directory.name or execution.status != "completed":
                    continue
                completed_at = datetime.fromisoformat(execution.updated_at)
                if completed_at.tzinfo is None:
                    raise ValueError("completion timestamp must be timezone-aware")
                histories.append(_CompletedHistory(directory, execution, completed_at))
            except (KeyError, ValueError, sqlite3.Error, OSError) as exc:
                diagnostics.log(
                    "retention.history_unreadable",
                    level=diagnostics.Level.WARNING,
                    error=exc,
                )
                _LOGGER.warning(
                    "Preserving unreadable execution history %s: %s", directory, exc
                )
        histories.sort(
            key=lambda item: (item.completed_at, item.execution.id), reverse=True
        )
        keep: set[str] = set()
        if last_execution_id is not None:
            keep.add(last_execution_id)
        limit = self.policy.max_completed_executions
        if limit is not None:
            protected_count = sum(
                item.execution.id == last_execution_id for item in histories
            )
            available = max(0, limit - protected_count)
            for item in histories:
                if item.execution.id == last_execution_id:
                    continue
                if available == 0:
                    break
                keep.add(item.execution.id)
                available -= 1
        deleted: list[str] = []
        for item in histories:
            if (
                item.execution.id == last_execution_id
                or item.execution.id in protected_ids
            ):
                continue
            age_exceeded = (
                self.policy.max_age_days is not None
                and (current - item.completed_at).total_seconds()
                > self.policy.max_age_days * 86400
            )
            count_exceeded = limit is not None and item.execution.id not in keep
            if not age_exceeded and not count_exceeded:
                continue
            try:
                await asyncio.to_thread(shutil.rmtree, item.directory)
                deleted.append(item.execution.id)
            except OSError as exc:
                with diagnostics.bind(execution_id=item.execution.id):
                    diagnostics.log(
                        "retention.remove_failed",
                        level=diagnostics.Level.WARNING,
                        error=exc,
                    )
                _LOGGER.warning(
                    "Could not remove execution history %s: %s", item.directory, exc
                )
        return deleted
