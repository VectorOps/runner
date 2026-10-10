"""Bounded directory listing through the execution's target environment."""

from typing import Optional

import connect

from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.environment import models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from . import tools
from .environment_tools import environment


@register_tool("list_files")
def list_files_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[models.ListRequest]:
    async def invoke(
        ctx: ActivityExecutionContext, request: models.ListRequest
    ) -> tools.ToolResponse:
        result = await environment(ctx).list_dir(request)
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.model_dump_json())]
        )

    return tools.Tool(
        "list_files",
        "List one directory in the target environment with a bounded entry count. "
        "Not recursive; inspect the truncated flag before treating a listing as complete.",
        models.ListRequest,
        invoke,
        approval=approval,
    )
