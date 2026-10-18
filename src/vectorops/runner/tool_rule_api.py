"""Typed CRUD operations for workspace and workflow authorization rules."""

from typing import Literal, Optional
from uuid import UUID

import pydantic

from vectorops.runner import tool_policy
from vectorops.runner.tool_authorization_store import WorkspaceRules, WorkflowGrants


class RuleScope(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    scope: Literal["workspace", "workflow"] = "workspace"
    execution_id: Optional[str] = pydantic.Field(
        default=None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128
    )
    workflow_id: Optional[str] = pydantic.Field(default=None, min_length=1)

    @pydantic.model_validator(mode="after")
    def validate_scope(self) -> "RuleScope":
        if self.scope == "workflow":
            if self.execution_id is None or self.workflow_id is None:
                raise ValueError("workflow scope requires execution_id and workflow_id")
        elif self.execution_id is not None or self.workflow_id is not None:
            raise ValueError("workspace scope cannot specify a workflow")
        return self


class RuleQuery(RuleScope):
    tool_name: Optional[str] = None


class RuleReference(RuleScope):
    rule_id: UUID


class RuleCreate(RuleScope):
    rule: tool_policy.RuleInput


class RuleUpdate(RuleReference):
    rule: tool_policy.RuleInput


class RuleManagement:
    def __init__(
        self, workspace: WorkspaceRules, workflow: Optional[WorkflowGrants] = None
    ) -> None:
        self.workspace = workspace
        self.workflow = workflow

    async def dispatch(
        self, method: str, params: dict[str, pydantic.JsonValue]
    ) -> pydantic.JsonValue:
        if method == "tools.rules.list":
            query = RuleQuery.model_validate(params)
            rules = (
                await self.workspace.list(query.tool_name)
                if query.scope == "workspace"
                else await self._workflow().authorization_rules(
                    query.workflow_id or "", query.tool_name
                )
            )
            return {"rules": [rule.model_dump(mode="json") for rule in rules]}
        if method in ("tools.rules.get", "tools.rules.delete"):
            reference = RuleReference.model_validate(params)
            if method == "tools.rules.delete":
                if reference.scope == "workspace":
                    await self.workspace.delete(reference.rule_id)
                else:
                    await self._workflow().delete_authorization_rule(
                        reference.workflow_id or "", reference.rule_id
                    )
                return None
            rules = (
                await self.workspace.list()
                if reference.scope == "workspace"
                else await self._workflow().authorization_rules(
                    reference.workflow_id or ""
                )
            )
            for rule in rules:
                if rule.id == reference.rule_id:
                    return rule.model_dump(mode="json")
            raise KeyError(str(reference.rule_id))
        if method not in ("tools.rules.create", "tools.rules.update"):
            raise ValueError("unknown authorization rule method")
        rule_id = None
        request: RuleCreate
        if method == "tools.rules.update":
            update = RuleUpdate.model_validate(params)
            rule_id = update.rule_id
            request = RuleCreate.model_validate(update.model_dump(exclude={"rule_id"}))
        else:
            request = RuleCreate.model_validate(params)
        rule = (
            await self.workspace.save(request.rule, rule_id)
            if request.scope == "workspace"
            else await self._workflow().save_authorization_rule(
                request.workflow_id or "", request.rule, rule_id
            )
        )
        return rule.model_dump(mode="json")

    def _workflow(self) -> WorkflowGrants:
        if self.workflow is None:
            raise ValueError("workflow rule storage is unavailable")
        return self.workflow
