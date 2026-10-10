"""Bounded, replay-recorded prompt context from the attached target environment."""

from typing import Final, Optional

from vectorops.runner.api import composition
from vectorops.runner.api.llm.environment_tools import environment
from vectorops.runner.environment import models
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.protocol import TargetEnvironment
from vectorops.runner.runner.activity_context import ActivityExecutionContext

CONTEXT_BYTES: Final = 65536
SKILL_LIMIT: Final = 32


async def _read_optional(target: TargetEnvironment, path: str) -> Optional[str]:
    try:
        result = await target.read_file(
            models.ReadRequest(path=path, limit=CONTEXT_BYTES)
        )
    except EnvironmentError as exc:
        if exc.code == ErrorCode.MISSING:
            return None
        raise
    text = result.content.decode("utf-8")
    if not result.eof:
        text += "\n[Truncated: read the remaining instructions with read_file.]"
    return text


@composition.activity(kind="builtin.context", version=1)
async def load_context(ctx: ActivityExecutionContext) -> str:
    """Record environment details, root instructions, and bounded skill paths once."""
    target = environment(ctx)
    description = await target.describe()
    parts = [
        "## Target environment",
        f"Platform: {description.platform}",
        f"Target: {description.identity}",
        "Paths are relative to the attached target, not the runner workspace.",
        "Process execution is not a read-only sandbox; follow role restrictions.",
    ]
    instructions = await _read_optional(target, "AGENTS.md")
    if instructions is not None:
        parts.extend(["## Repository instructions: AGENTS.md", instructions])
    try:
        skills = await target.list_dir(
            models.ListRequest(path=".vocode/skills", limit=SKILL_LIMIT)
        )
    except EnvironmentError as exc:
        if exc.code != ErrorCode.MISSING:
            raise
    else:
        parts.append("## Available skill directories")
        parts.append("Read the relevant directory's SKILL.md before using a skill.")
        for entry in sorted(skills.entries, key=lambda item: item.path):
            if entry.kind == "directory":
                parts.append(entry.path)
        if skills.truncated:
            parts.append(
                "[Skill listing truncated; use list_files for focused discovery.]"
            )
    return "\n\n".join(parts)
