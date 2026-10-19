"""Opt-in worktree tools using the workspace lifecycle owner."""

import json
from typing import Optional

import connect

from vectorops.runner.api import tool_contracts as tools
from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager


def manager(context: ActivityExecutionContext) -> Manager:
    if context.targets is None:
        raise tools.ToolExecutionError("worktree service is unavailable")
    return context.targets.manager


@register_tool("worktree_create")
def worktree_create_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[m.Create]:
    async def execute(
        context: ActivityExecutionContext, arguments: m.Create
    ) -> tools.ToolResponse:
        origin = m.Origin(
            kind="workflow",
            execution_id=context.execution_id,
            workflow_id=context.workflow_id,
            step_id=context.step_id,
        )
        record = await manager(context).create(
            arguments,
            key="workflow:"
            + json.dumps([context.execution_id, context.workflow_id, context.step_id]),
            origin=origin,
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=record.model_dump_json())]
        )

    return tools.Tool(
        "worktree_create",
        "Create an isolated Git worktree with a required descriptive name. Optional branch creates a new branch; omit it for a UUID-generated branch. base_ref defaults to HEAD of the configured repository. Does not copy uncommitted changes. The worktree persists until explicitly deleted; reuse its id for child workflows or exec.",
        m.Create,
        execute,
        approval=approval,
    )


@register_tool("worktree_list")
def worktree_list_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[m.Query]:
    async def execute(
        context: ActivityExecutionContext, arguments: m.Query
    ) -> tools.ToolResponse:
        page = await manager(context).listing(arguments)
        return tools.ToolResponse(
            content=[connect.TextBlock(text=page.model_dump_json())]
        )

    return tools.Tool(
        "worktree_list",
        "List user- and workflow-created workspace worktrees with provenance and cached Git status. Set refresh to inspect current Git status; results are paginated. Sharing a worktree does not isolate concurrent writers.",
        m.Query,
        execute,
        approval=approval,
    )


@register_tool("worktree_delete")
def worktree_delete_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[m.Delete]:
    async def execute(
        context: ActivityExecutionContext, arguments: m.Delete
    ) -> tools.ToolResponse:
        record = await manager(context).delete(arguments)
        return tools.ToolResponse(
            content=[connect.TextBlock(text=record.model_dump_json())]
        )

    return tools.Tool(
        "worktree_delete",
        "Explicitly remove a workspace worktree, retaining its Git branch and audit record. Active use blocks deletion even with force. force discards dirty/untracked files and acknowledges abandoned crash leases; inspect orphaned processes first. Omitting deletion keeps the worktree for future workflows.",
        m.Delete,
        execute,
        approval=approval,
    )
