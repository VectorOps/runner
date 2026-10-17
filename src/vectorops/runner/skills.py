"""Validated, bounded project skill catalogs from local project files."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Final

import pydantic
import yaml

from vectorops.runner.environment import paths
from vectorops.runner.environment.errors import EnvironmentError

SKILL_LIMIT: Final = 32
SKILL_BYTES: Final = 65536
logger = logging.getLogger(__name__)


class Skill(pydantic.BaseModel):
    name: str = pydantic.Field(strict=True, min_length=1, max_length=64)
    description: str = pydantic.Field(strict=True, min_length=1, max_length=1024)
    path: str

    @pydantic.field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not all(ch.islower() or ch.isdigit() or ch in "-_" for ch in value):
            raise ValueError("Skill names require lowercase letters, digits, - or _")
        return value

    @classmethod
    def from_document(cls, path: str, content: bytes) -> "Skill":
        """Parse only a complete YAML header, leaving the skill body on demand."""
        lines = content.splitlines()
        if not lines or lines[0].strip() != b"---":
            raise ValueError("Missing YAML frontmatter")
        end = next(
            (index for index in range(1, len(lines)) if lines[index].strip() == b"---"),
            None,
        )
        if end is None:
            raise ValueError("Incomplete YAML frontmatter within the read limit")
        metadata = yaml.safe_load(b"\n".join(lines[1:end]).decode("utf-8"))
        if not isinstance(metadata, dict):
            raise ValueError("Skill frontmatter must be a mapping")
        return cls.model_validate(
            {
                "name": metadata.get("name"),
                "description": metadata.get("description"),
                "path": path,
            }
        )


class SkillCatalog(pydantic.BaseModel):
    skills: list[Skill] = pydantic.Field(default_factory=list)
    truncated: bool = False

    def render(self) -> str:
        if not self.skills and not self.truncated:
            return ""
        lines = [
            "## Available project skills",
            "Skills provide reusable expertise and workflows. Use an appropriate "
            "skill when it matches the task. Before using it, read its SKILL.md "
            "with read_file and follow its instructions.",
        ]
        lines.extend(
            f"- {skill.name}: {' '.join(skill.description.split())} (file: {skill.path})"
            for skill in self.skills
        )
        if self.truncated:
            lines.append(
                "[Skill listing truncated; use list_files for focused discovery.]"
            )
        return "\n".join(lines)


async def discover_skills(root: Path) -> SkillCatalog:
    """Inspect direct skill directories, skipping invalid or unreadable files."""

    def listing() -> tuple[list[str], bool]:
        entries: list[str] = []
        with os.scandir(paths.resolve_path(root, ".vocode/skills")) as iterator:
            for index, entry in enumerate(iterator):
                if index == SKILL_LIMIT:
                    return entries, True
                if entry.is_dir(follow_symlinks=False):
                    entries.append(f".vocode/skills/{entry.name}/SKILL.md")
        return entries, False

    def read(path: str) -> bytes:
        with paths.open_regular(paths.resolve_path(root, path)) as stream:
            return stream.read(SKILL_BYTES)

    try:
        entries, truncated = await asyncio.to_thread(listing)
    except FileNotFoundError:
        return SkillCatalog()
    except (EnvironmentError, OSError) as exc:
        logger.warning("Cannot discover project skills: %s", exc)
        return SkillCatalog()
    skills = []
    for path in sorted(entries):
        try:
            content = await asyncio.to_thread(read, path)
            skill = Skill.from_document(path, content)
        except (EnvironmentError, OSError, ValueError, yaml.YAMLError) as exc:
            logger.warning("Ignoring skill at %s: %s", path, exc)
            continue
        skills.append(skill)
    return SkillCatalog(skills=skills, truncated=truncated)
