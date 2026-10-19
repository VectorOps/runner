"""Workspace settings and transactional workflow grant storage for authorization."""

from __future__ import annotations

import sqlite3
import asyncio
from collections.abc import Iterable, Set
from typing import Optional, Protocol, TYPE_CHECKING
import typing
from uuid import UUID, uuid4

import pydantic

from vectorops.runner import tool_policy
from vectorops.runner.runner import sql
from vectorops.runner.runner.workspace_database import WorkspaceDatabase

if TYPE_CHECKING:
    from vectorops.runner.runner.project import Configuration

RULES: typing.Final = sql.Table("tool_authorization_rules")
OUTBOX: typing.Final = sql.Table("tool_rule_outbox")
RECEIPTS: typing.Final = sql.Table("tool_rule_receipts")


def equivalent_rule(
    rules: Iterable[tool_policy.AuthorizationRule],
    value: tool_policy.RuleInput,
    exclude: Optional[UUID] = None,
) -> Optional[tool_policy.AuthorizationRule]:
    return next(
        (
            rule
            for rule in sorted(rules, key=lambda rule: str(rule.id))
            if rule.id != exclude
            and rule.tool_name == value.tool_name
            and rule.decision == value.decision
            and len(rule.properties) == len(value.properties)
            and tool_policy.matches(rule.properties, value.properties)
        ),
        None,
    )


class WorkflowGrants(Protocol):
    async def authorization_rules(
        self, workflow_id: str, tool_name: Optional[str] = None
    ) -> list[tool_policy.AuthorizationRule]: ...

    async def save_authorization_rule(
        self,
        workflow_id: str,
        value: tool_policy.RuleInput,
        rule_id: Optional[UUID] = None,
    ) -> tool_policy.AuthorizationRule: ...

    async def delete_authorization_rule(
        self, workflow_id: str, rule_id: UUID
    ) -> None: ...

    async def approval_rule_id(self, request_id: str) -> Optional[UUID]: ...


class RuleDocument(pydantic.BaseModel):
    rules: dict[str, tool_policy.AuthorizationRule] = pydantic.Field(
        default_factory=dict
    )
    imported: set[str] = pydantic.Field(default_factory=set)
    import_ids: dict[str, UUID] = pydantic.Field(default_factory=dict)


class WorkspaceRules:
    @staticmethod
    def _initial_document() -> str:
        """Seed editable internal-read rules only when no rule document exists."""
        rules = [
            tool_policy.AuthorizationRule(
                id=uuid4(),
                tool_name=name,
                properties={"scope": "internal"},
                decision="approve",
            )
            for name in ("read_file", "list_files")
        ]
        return RuleDocument(
            rules={str(rule.id): rule for rule in rules}
        ).model_dump_json()

    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database
        self._read_task: Optional[asyncio.Task[RuleDocument]] = None

    async def list(
        self, tool_name: Optional[str] = None
    ) -> list[tool_policy.AuthorizationRule]:
        async def read() -> RuleDocument:
            return RuleDocument.model_validate_json(
                await self.database.load_document(
                    "tool.authorization.rules", self._initial_document
                )
            )

        if self._read_task is None:
            self._read_task = asyncio.create_task(read())
        task = self._read_task
        try:
            document = await asyncio.shield(task)
        finally:
            if task.done() and self._read_task is task:
                self._read_task = None
        return sorted(
            (
                rule
                for rule in document.rules.values()
                if tool_name is None or rule.tool_name == tool_name
            ),
            key=lambda rule: str(rule.id),
        )

    async def save(
        self, value: tool_policy.RuleInput, rule_id: Optional[UUID] = None
    ) -> tool_policy.AuthorizationRule:
        rule = tool_policy.AuthorizationRule(
            **value.model_dump(), id=rule_id or uuid4()
        )

        def update(payload: str) -> str:
            nonlocal rule
            document = RuleDocument.model_validate_json(payload)
            if rule_id is not None and str(rule_id) not in document.rules:
                raise KeyError(str(rule_id))
            existing = equivalent_rule(document.rules.values(), value, rule_id)
            if existing is not None:
                if rule_id is not None:
                    raise ValueError(
                        f"equivalent authorization rule already exists: {existing.id}"
                    )
                rule = existing
            document.rules[str(rule.id)] = rule
            return document.model_dump_json()

        await self.database.update_document(
            "tool.authorization.rules", self._initial_document, update
        )
        self._read_task = None
        return rule

    async def delete(self, rule_id: UUID) -> None:
        def update(payload: str) -> str:
            document = RuleDocument.model_validate_json(payload)
            del document.rules[str(rule_id)]
            return document.model_dump_json()

        await self.database.update_document(
            "tool.authorization.rules", self._initial_document, update
        )
        self._read_task = None

    async def import_rule(self, rule: tool_policy.AuthorizationRule) -> UUID:
        """Remember imports even after deletion so retried delivery cannot resurrect rules."""
        canonical_id = rule.id

        def update(payload: str) -> str:
            nonlocal canonical_id
            document = RuleDocument.model_validate_json(payload)
            key = str(rule.id)
            if key in document.imported:
                canonical_id = document.import_ids.get(key, rule.id)
            else:
                existing = equivalent_rule(document.rules.values(), rule)
                canonical_id = existing.id if existing is not None else rule.id
                if existing is None:
                    document.rules[key] = rule
                document.imported.add(key)
                document.import_ids[key] = canonical_id
            return document.model_dump_json()

        await self.database.update_document(
            "tool.authorization.rules", self._initial_document, update
        )
        self._read_task = None
        return canonical_id


class AuthorizationStore:
    def __init__(
        self,
        configuration: Configuration,
        grants: Optional[WorkflowGrants] = None,
        workspace: Optional[WorkspaceRules] = None,
    ) -> None:
        self._configuration = configuration
        self._grants = grants
        self.workspace = workspace

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

    async def rules(
        self, workflow_id: str, tool_name: str
    ) -> list[tool_policy.ScopedRule]:
        workspace = (
            await self.workspace.list(tool_name) if self.workspace is not None else []
        )
        workflow = (
            await self._grants.authorization_rules(workflow_id, tool_name)
            if self._grants is not None
            else []
        )
        return [
            tool_policy.ScopedRule(scope="workspace", rule=rule) for rule in workspace
        ] + [tool_policy.ScopedRule(scope="workflow", rule=rule) for rule in workflow]

    async def approval_rule_id(self, request_id: str) -> Optional[UUID]:
        return (
            await self._grants.approval_rule_id(request_id)
            if self._grants is not None
            else None
        )


class SQLiteWorkflowGrants:
    """Use the caller's transaction so grants commit with their accepted answers."""

    @staticmethod
    def initialize(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS tool_authorization_rules (id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL REFERENCES workflows(id), request_id TEXT REFERENCES input_requests(request_id), payload TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS tool_rule_outbox (id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE REFERENCES input_requests(request_id), payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS tool_rule_receipts (request_id TEXT PRIMARY KEY REFERENCES input_requests(request_id), rule_id TEXT NOT NULL)"
        )

    @staticmethod
    def list(
        connection: sqlite3.Connection,
        workflow_id: str,
        tool_name: Optional[str] = None,
    ) -> list[tool_policy.AuthorizationRule]:
        query = (
            sql.Query.from_(RULES)
            .select(RULES.payload)
            .where(RULES.workflow_id == workflow_id)
        )
        if tool_name is not None:
            query = query.where(
                sql.json_extract(RULES.payload, "$.tool_name") == tool_name
            )
        return [
            tool_policy.AuthorizationRule.model_validate_json(row[0])
            for row in sql.execute(connection, query.orderby(RULES.id))
        ]

    @staticmethod
    def save(
        connection: sqlite3.Connection,
        workflow_id: str,
        value: tool_policy.RuleInput,
        rule_id: Optional[UUID] = None,
    ) -> tool_policy.AuthorizationRule:
        rule = tool_policy.AuthorizationRule(
            **value.model_dump(), id=rule_id or uuid4()
        )
        rules = SQLiteWorkflowGrants.list(connection, workflow_id)
        if rule_id is not None and not any(item.id == rule_id for item in rules):
            raise KeyError(str(rule_id))
        existing = equivalent_rule(rules, value, rule_id)
        if existing is not None:
            if rule_id is not None:
                raise ValueError(
                    f"equivalent authorization rule already exists: {existing.id}"
                )
            sql.execute(
                connection,
                sql.Query.update(RULES)
                .set(RULES.request_id, None)
                .where(RULES.id == str(existing.id)),
            )
            return existing
        if rule_id is None:
            sql.execute(
                connection,
                sql.Query.into(RULES).insert(
                    str(rule.id), workflow_id, None, rule.model_dump_json()
                ),
            )
        elif (
            sql.execute(
                connection,
                sql.Query.update(RULES)
                .set(RULES.payload, rule.model_dump_json())
                .where((RULES.id == str(rule.id)) & (RULES.workflow_id == workflow_id)),
            ).rowcount
            != 1
        ):
            raise KeyError(str(rule_id))
        return rule

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
        value = tool_policy.response_rule(payload, response)
        if value is None:
            return
        answer = tool_policy.ToolApprovalResponse.model_validate(response)
        rule = tool_policy.AuthorizationRule(**value.model_dump(), id=uuid4())
        existing = None
        if answer.scope == "workflow":
            existing = equivalent_rule(
                SQLiteWorkflowGrants.list(connection, workflow_id), value
            )
            if existing is not None:
                rule = existing
        sql.execute(
            connection,
            sql.Query.into(RECEIPTS).insert(request_id, str(rule.id)),
        )
        if answer.scope == "workspace":
            sql.execute(
                connection,
                sql.Query.into(OUTBOX)
                .columns(OUTBOX.id, OUTBOX.request_id, OUTBOX.payload)
                .insert(str(rule.id), request_id, rule.model_dump_json()),
            )
        elif existing is None:
            sql.execute(
                connection,
                sql.Query.into(RULES).insert(
                    str(rule.id), workflow_id, request_id, rule.model_dump_json()
                ),
            )

    @staticmethod
    def retain(connection: sqlite3.Connection, step_ids: Set[str]) -> None:
        grants = sql.execute(
            connection,
            sql.Query.from_(RULES).select(RULES.id).where(RULES.request_id.notnull()),
        ).fetchall()
        retained_rules = {
            row[1]
            for row in sql.execute(
                connection,
                sql.Query.from_(RECEIPTS).select(RECEIPTS.request_id, RECEIPTS.rule_id),
            )
            if row[0] in step_ids
        }
        sql.execute(
            connection,
            sql.Query.from_(RULES)
            .delete()
            .where(
                RULES.id.isin(
                    sql.json_values(
                        [row[0] for row in grants if row[0] not in retained_rules]
                    )
                )
            ),
        )
