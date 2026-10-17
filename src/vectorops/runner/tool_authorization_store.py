"""Workspace settings and transactional workflow grant storage for authorization."""

from __future__ import annotations

import sqlite3
from collections.abc import Set
from typing import Optional, Protocol, TYPE_CHECKING

import pydantic

from vectorops.runner import tool_policy

if TYPE_CHECKING:
    from vectorops.runner.runner.project import Configuration


class WorkflowGrants(Protocol):
    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool: ...


class AuthorizationStore:
    def __init__(
        self, configuration: Configuration, grants: Optional[WorkflowGrants] = None
    ) -> None:
        self._configuration = configuration
        self._grants = grants

    def settings(self) -> tool_policy.ApprovalSettings:
        return self._configuration.get_validated(
            "tools.approval", tool_policy.ApprovalSettings
        )

    async def save_settings(
        self, settings: tool_policy.ApprovalSettings, *, permanent: bool = False
    ) -> None:
        await self._configuration.set(
            "tools.approval", settings.model_dump(mode="json"), permanent=permanent
        )

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool:
        if self._grants is None:
            return False
        return await self._grants.tool_approved(workflow_id, tool_name)


class SQLiteWorkflowGrants:
    """Use the caller's transaction so grants commit with their accepted answers."""

    @staticmethod
    def approved(
        connection: sqlite3.Connection, workflow_id: str, tool_name: str
    ) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM tool_approval_grants WHERE workflow_id = ? AND tool_name = ?",
                (workflow_id, tool_name),
            ).fetchone()
            is not None
        )

    @staticmethod
    def record_answer(
        connection: sqlite3.Connection,
        *,
        operation_kind: str,
        workflow_id: str,
        request_id: str,
        payload: pydantic.JsonValue,
        response: pydantic.JsonValue,
    ) -> None:
        if operation_kind != "llm.tool":
            return
        tool_name = tool_policy.workflow_grant(payload, response)
        if tool_name is not None:
            connection.execute(
                "INSERT INTO tool_approval_grants VALUES (?, ?, ?) "
                "ON CONFLICT(workflow_id, tool_name) DO NOTHING",
                (workflow_id, tool_name, request_id),
            )

    @staticmethod
    def retain(connection: sqlite3.Connection, step_ids: Set[str]) -> None:
        grants = connection.execute(
            "SELECT request_id FROM tool_approval_grants"
        ).fetchall()
        connection.executemany(
            "DELETE FROM tool_approval_grants WHERE request_id = ?",
            [(row[0],) for row in grants if row[0] not in step_ids],
        )
