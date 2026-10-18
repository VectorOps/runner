"""Bounded directory listing using local filesystem APIs."""

import asyncio
import os
from pathlib import PurePosixPath
import stat
from typing import Literal, Optional

import connect

from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.environment import models, paths
from vectorops.runner.environment.errors import EnvironmentError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.api import tool_contracts as tools
from .environment import environment


@register_tool("list_files")
def list_files_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[models.ListRequest]:
    async def auth_context(
        context: ActivityExecutionContext, request: models.ListRequest
    ) -> tools.AuthContext:
        scope = await asyncio.to_thread(
            paths.path_scope, environment(context).root, request.path
        )
        return tools.AuthContext(properties={"scope": scope})

    async def invoke(
        ctx: ActivityExecutionContext, request: models.ListRequest
    ) -> tools.ToolResponse:
        target = environment(ctx)
        paths.check_limit(request.limit, target.config.max_list_entries)

        def listing() -> models.ListResult:
            entries: list[models.FileInfo] = []
            with os.scandir(
                paths.resolve_path(target.root, request.path, allow_external=True)
            ) as iterator:
                for entry in iterator:
                    if len(entries) == request.limit:
                        return models.ListResult(entries=entries, truncated=True)
                    metadata = entry.stat(follow_symlinks=False)
                    kind: Literal["file", "directory", "symlink", "other"] = "other"
                    if stat.S_ISREG(metadata.st_mode):
                        kind = "file"
                    elif stat.S_ISDIR(metadata.st_mode):
                        kind = "directory"
                    elif stat.S_ISLNK(metadata.st_mode):
                        kind = "symlink"
                    entries.append(
                        models.FileInfo(
                            path=str(PurePosixPath(request.path) / entry.name),
                            kind=kind,
                            size=metadata.st_size,
                            modified_ns=metadata.st_mtime_ns,
                        )
                    )
            return models.ListResult(entries=entries, truncated=False)

        try:
            result = await asyncio.to_thread(listing)
        except OSError as exc:
            raise EnvironmentError.from_os_error(exc) from exc
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.model_dump_json())]
        )

    return tools.Tool(
        "list_files",
        "List one directory in the target environment with a bounded entry count. "
        "Accepts target-relative, absolute, and home-relative paths, including outside the target root. "
        "Not recursive; inspect the truncated flag before treating a listing as complete.",
        models.ListRequest,
        invoke,
        approval=approval,
        contextual_auth_context=auth_context,
    )
