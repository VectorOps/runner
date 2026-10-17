import asyncio
from pathlib import Path
from unittest.mock import patch

import pydantic
import pytest
import yaml

from vectorops.runner import skills
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode


@pytest.fixture
async def target(tmp_path: Path) -> Path:
    return tmp_path


def write_skill(tmp_path: Path, directory: str, content: str) -> Path:
    path = tmp_path / ".vocode" / "skills" / directory / "SKILL.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def document(name: str, description: str = "Review code.") -> str:
    return f"---\nname: {name}\ndescription: {description}\n---\nDetailed instructions."


@pytest.mark.parametrize("name", ["review", "review-code_2", "a" * 64])
def test_valid_skill_names(name: str) -> None:
    skill = skills.Skill.from_document("SKILL.md", document(name).encode())
    assert skill.name == name
    assert skill.description == "Review code."
    assert skill.path == "SKILL.md"


@pytest.mark.parametrize(
    "metadata",
    [
        {"name": "", "description": "Review"},
        {"name": "Upper", "description": "Review"},
        {"name": "two words", "description": "Review"},
        {"name": "a/b", "description": "Review"},
        {"name": "a" * 65, "description": "Review"},
        {"name": 123, "description": "Review"},
        {"name": "review", "description": ""},
        {"name": "review", "description": "x" * 1025},
        {"name": "review", "description": True},
        {"name": "review"},
        {"description": "Review"},
    ],
)
def test_invalid_skill_metadata(metadata: dict[str, pydantic.JsonValue]) -> None:
    content = f"---\n{yaml.safe_dump(metadata)}---\nBody"
    with pytest.raises(pydantic.ValidationError):
        skills.Skill.from_document("SKILL.md", content.encode())


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"No frontmatter",
        b"---\nname: review",
        b"---\n- list\n---",
        b"---\n---",
        b"---\nname: [\n---",
        b"---\nname: !!python/object:object {}\n---",
        b"---\nname: \xff\n---",
    ],
)
def test_invalid_documents(content: bytes) -> None:
    with pytest.raises((ValueError, yaml.YAMLError)):
        skills.Skill.from_document("SKILL.md", content)


def test_multiline_description_and_extra_metadata() -> None:
    skill = skills.Skill.from_document(
        "SKILL.md",
        b"---\r\nname: review\r\ndescription: |\r\n  Review code\r\n"
        b"  with focused tests.\r\nallowed-tools: [read_file]\r\n---\r\nBody",
    )
    rendered = skills.SkillCatalog(skills=[skill]).render()
    assert "- review: Review code with focused tests. (file: SKILL.md)" in rendered
    assert "Body" not in rendered
    assert "allowed-tools" not in rendered


async def test_discovery_skips_invalid_and_sorts(
    tmp_path: Path, target: Path, caplog: pytest.LogCaptureFixture
) -> None:
    write_skill(tmp_path, "z-last", document("last"))
    write_skill(tmp_path, "a-first", document("first"))
    write_skill(tmp_path, "broken", "---\nname: [\n---")
    write_skill(tmp_path, "missing", document("missing")).unlink()
    write_skill(tmp_path, "binary", document("binary")).write_bytes(b"---\n\xff\n---")
    (tmp_path / ".vocode/skills/not-a-directory").write_text(document("ignored"))
    catalog = await skills.discover_skills(target)
    assert [skill.name for skill in catalog.skills] == ["first", "last"]
    assert [skill.path for skill in catalog.skills] == [
        ".vocode/skills/a-first/SKILL.md",
        ".vocode/skills/z-last/SKILL.md",
    ]
    assert not catalog.truncated
    assert caplog.text.count("Ignoring skill at") == 3


async def test_absent_and_empty_catalog(tmp_path: Path, target: Path) -> None:
    assert (await skills.discover_skills(target)).render() == ""
    (tmp_path / ".vocode/skills").mkdir(parents=True)
    assert (await skills.discover_skills(target)).render() == ""
    write_skill(tmp_path, "invalid", "Not a skill")
    assert (await skills.discover_skills(target)).render() == ""


async def test_discovery_limits(tmp_path: Path, target: Path) -> None:
    for index in range(skills.SKILL_LIMIT + 1):
        write_skill(tmp_path, f"skill-{index:02}", document(f"skill-{index:02}"))
    catalog = await skills.discover_skills(target)
    assert len(catalog.skills) == skills.SKILL_LIMIT
    assert catalog.truncated
    assert "Skill listing truncated" in catalog.render()


async def test_bounded_reads_only_require_complete_header(
    tmp_path: Path, target: Path
) -> None:
    write_skill(
        tmp_path, "large-body", document("large-body") + "x" * skills.SKILL_BYTES
    )
    write_skill(
        tmp_path,
        "large-header",
        "---\nname: large-header\ndescription: " + "x" * skills.SKILL_BYTES + "\n---",
    )
    catalog = await skills.discover_skills(target)
    assert [skill.name for skill in catalog.skills] == ["large-body"]


@pytest.mark.parametrize(
    "operation", ["os.scandir", "vectorops.runner.skills.paths.open_regular"]
)
async def test_environment_errors_are_skipped(
    tmp_path: Path,
    target: Path,
    caplog: pytest.LogCaptureFixture,
    operation: str,
) -> None:
    write_skill(tmp_path, "review", document("review"))
    with patch(
        operation,
        side_effect=EnvironmentError(ErrorCode.DENIED, "Access denied"),
    ):
        assert (await skills.discover_skills(target)).render() == ""
    assert "Access denied" in caplog.text


async def test_cancellation_propagates(target: Path) -> None:
    with patch("os.scandir", side_effect=asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await skills.discover_skills(target)
