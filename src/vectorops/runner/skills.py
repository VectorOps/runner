"""Validated, bounded skill catalogs from project, configured, and bundled files."""

import asyncio
from collections.abc import Sequence
import heapq
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
BUNDLED_SKILLS: Final = Path(__file__).parent / "bundled_skills"
logger = logging.getLogger(__name__)


class Settings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    directories: list[str] = pydantic.Field(default_factory=list, max_length=32)
    include_builtin: bool = True

    @pydantic.field_validator("directories", mode="after")
    @classmethod
    def validate_directories(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or "\0" in value for value in values):
            raise ValueError("skill directories must be nonblank paths without NULs")
        return values


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
            "## Available skills",
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


async def discover_skills(
    root: Path,
    directories: Sequence[str] = (),
    *,
    include_builtin: bool = True,
) -> SkillCatalog:
    """Merge project, configured, then bundled skills; first valid name wins."""

    def listing(directory: str) -> tuple[list[Path], bool]:
        base = paths.resolve_path(root, directory, allow_external=True)
        with os.scandir(base) as iterator:
            entries = heapq.nsmallest(
                SKILL_LIMIT + 1,
                (entry.name for entry in iterator if entry.is_dir()),
            )
        return [base / name / "SKILL.md" for name in entries[:SKILL_LIMIT]], len(
            entries
        ) > SKILL_LIMIT

    def read(path: Path) -> Skill:
        path = path.resolve()
        display_path = (
            str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
        )
        with paths.open_regular(path) as stream:
            return Skill.from_document(display_path, stream.read(SKILL_BYTES))

    root = await asyncio.to_thread(root.resolve)
    sources = [".voc/skills", *directories]
    if include_builtin:
        sources.append(str(BUNDLED_SKILLS))
    catalog = SkillCatalog()
    names: set[str] = set()
    for directory in dict.fromkeys(sources):
        try:
            entries, truncated = await asyncio.to_thread(listing, directory)
        except FileNotFoundError:
            continue
        except (EnvironmentError, OSError) as exc:
            logger.warning("Cannot discover skills at %s: %s", directory, exc)
            continue
        catalog.truncated |= truncated
        for path in entries:
            try:
                skill = await asyncio.to_thread(read, path)
            except (EnvironmentError, OSError, ValueError, yaml.YAMLError) as exc:
                logger.warning("Ignoring skill at %s: %s", path, exc)
                continue
            if skill.name in names:
                continue
            if len(catalog.skills) == SKILL_LIMIT:
                catalog.truncated = True
                return catalog
            names.add(skill.name)
            catalog.skills.append(skill)
    return catalog
