"""File-byte accounting for the workspace and its execution directories."""

import asyncio
import os
from pathlib import Path
import stat
import typing

import pydantic


class Query(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    top: int = pydantic.Field(default=10, ge=0, le=500, strict=True)


class WorkflowUsage(pydantic.BaseModel):
    """A root workflow and its children share one execution directory."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    execution_id: str
    size_bytes: int = pydantic.Field(ge=0)


class Report(pydantic.BaseModel):
    """Live regular-file sizes, not allocated blocks or an atomic snapshot."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    total_bytes: int = pydantic.Field(ge=0)
    workflow_count: int = pydantic.Field(ge=0)
    top_workflows: list[WorkflowUsage] = pydantic.Field(default_factory=list)


async def measure(workspace: Path, data: Path, query: Query) -> Report:
    return await asyncio.to_thread(_measure, workspace, data / "executions", query)


async def measure_execution(
    workspace: Path, data: Path, execution_id: str
) -> WorkflowUsage:
    return await asyncio.to_thread(_measure_execution, workspace, data, execution_id)


def _measure_execution(workspace: Path, data: Path, execution_id: str) -> WorkflowUsage:
    """Measure only the selected directory, rejecting links along its workspace path."""
    if (
        not execution_id
        or execution_id in (".", "..")
        or Path(execution_id).name != execution_id
    ):
        raise ValueError("execution ID must be a single directory name")
    relative = (data / "executions" / execution_id).relative_to(workspace)
    directory = workspace
    for part in relative.parts:
        directory /= part
        if not stat.S_ISDIR(directory.stat(follow_symlinks=False).st_mode):
            raise FileNotFoundError("execution directory is missing")
    total, _ = _scan(directory, missing_root_ok=False)
    return WorkflowUsage(execution_id=execution_id, size_bytes=total)


def _measure(workspace: Path, executions: Path, query: Query) -> Report:
    total, workflows = _scan(workspace, executions)
    ranked = sorted(workflows.items(), key=lambda item: (-item[1], item[0]))
    return Report(
        total_bytes=total,
        workflow_count=len(workflows),
        top_workflows=[
            WorkflowUsage(execution_id=identifier, size_bytes=size)
            for identifier, size in ranked[: query.top]
        ],
    )


def _scan(
    root: Path,
    executions: typing.Optional[Path] = None,
    *,
    missing_root_ok: bool = True,
) -> tuple[int, dict[str, int]]:
    """Scan once without following links; tolerate files removed during the scan."""
    total = 0
    workflows: dict[str, int] = {}
    pending: list[tuple[Path, typing.Optional[str]]] = [(root, None)]
    while pending:
        directory, execution_id = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if stat.S_ISDIR(info.st_mode):
                        child_id = execution_id
                        if directory == executions:
                            child_id = entry.name
                            workflows[child_id] = 0
                        pending.append((Path(entry.path), child_id))
                    elif stat.S_ISREG(info.st_mode):
                        total += info.st_size
                        if execution_id is not None:
                            workflows[execution_id] += info.st_size
        except FileNotFoundError:
            if directory == root and not missing_root_ok:
                raise
            continue
    return total, workflows
