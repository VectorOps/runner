"""Bounded, replay-recorded prompt context from the attached target environment."""

import asyncio
from pathlib import Path
import sys
from typing import Final, Optional

from vectorops.runner import skills
from vectorops.runner.api import composition
from vectorops.runner.api.llm.environment_tools import environment
from vectorops.runner.environment import paths
from vectorops.runner.environment.errors import EnvironmentError
from vectorops.runner.runner.activity_context import ActivityExecutionContext

CONTEXT_BYTES: Final = 65536


async def _read_optional(
    root: Path, path: str, limit: int = CONTEXT_BYTES
) -> Optional[str]:
    def read() -> bytes:
        with paths.open_regular(paths.resolve_path(root, path)) as stream:
            return stream.read(limit + 1)

    try:
        content = await asyncio.to_thread(read)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise EnvironmentError.from_os_error(exc) from exc
    text = content[:limit].decode("utf-8")
    if len(content) > limit:
        text += "\n[Truncated: read the remaining instructions with read_file.]"
    return text


@composition.activity_wrapper(kind="builtin.context", version=1)
async def load_context(call: composition.ActivityCall) -> str:
    """Record environment details, root instructions, and a bounded skill catalog."""
    configuration = composition.workflow_context().project.configuration

    async def invoke(ctx: ActivityExecutionContext) -> str:
        options = skills.Settings.model_validate(configuration.get("skills", {}))
        return await _load_context(ctx, options)

    return await call.run(input={}, invoke=invoke)


async def _load_context(ctx: ActivityExecutionContext, options: skills.Settings) -> str:
    target = environment(ctx)
    parts = [
        "## Target environment",
        f"Platform: {sys.platform}",
        f"Target: {target.root.as_uri()}",
        "Paths are relative to the attached target, not the runner workspace.",
        "Process execution is not a read-only sandbox; follow role restrictions.",
    ]
    paths.check_limit(CONTEXT_BYTES, target.config.max_read_bytes)
    instructions = await _read_optional(target.root, "AGENTS.md")
    if instructions is not None:
        parts.extend(["## Repository instructions: AGENTS.md", instructions])
    catalog = await skills.discover_skills(
        target.root, options.directories, include_builtin=options.include_builtin
    )
    skill_context = catalog.render()
    if skill_context:
        parts.append(skill_context)
    return "\n\n".join(parts)
