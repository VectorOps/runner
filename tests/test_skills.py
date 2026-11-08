import asyncio
from pathlib import Path
import re
from unittest.mock import patch

import pydantic
import pytest
import yaml

from vectorops.runner import skills
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture
async def target(tmp_path: Path) -> Path:
    return tmp_path


def write_skill(tmp_path: Path, directory: str, content: str) -> Path:
    path = tmp_path / ".voc" / "skills" / directory / "SKILL.md"
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
    (tmp_path / ".voc/skills/not-a-directory").write_text(document("ignored"))
    catalog = await skills.discover_skills(target, include_builtin=False)
    assert [skill.name for skill in catalog.skills] == ["first", "last"]
    assert [skill.path for skill in catalog.skills] == [
        ".voc/skills/a-first/SKILL.md",
        ".voc/skills/z-last/SKILL.md",
    ]
    assert not catalog.truncated
    assert caplog.text.count("Ignoring skill at") == 3


async def test_absent_and_empty_catalog(tmp_path: Path, target: Path) -> None:
    assert (await skills.discover_skills(target, include_builtin=False)).render() == ""
    (tmp_path / ".voc/skills").mkdir(parents=True)
    assert (await skills.discover_skills(target, include_builtin=False)).render() == ""
    write_skill(tmp_path, "invalid", "Not a skill")
    assert (await skills.discover_skills(target, include_builtin=False)).render() == ""


async def test_discovery_limits(tmp_path: Path, target: Path) -> None:
    for index in range(skills.SKILL_LIMIT + 1):
        write_skill(tmp_path, f"skill-{index:02}", document(f"skill-{index:02}"))
    catalog = await skills.discover_skills(target, include_builtin=False)
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
    catalog = await skills.discover_skills(target, include_builtin=False)
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
        assert (
            await skills.discover_skills(target, include_builtin=False)
        ).render() == ""
    assert "Access denied" in caplog.text


async def test_cancellation_propagates(target: Path) -> None:
    with patch("os.scandir", side_effect=asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await skills.discover_skills(target)


@pytest.mark.parametrize("directory", ["", " ", "bad\0path"])
def test_invalid_skill_directories(directory: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        skills.Settings(directories=[directory])


async def test_external_sources_and_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "target"
    external = tmp_path / "external"
    bundled = tmp_path / "bundled"
    local = write_skill(target, "review", document("review", "Project."))
    write_skill(external, "review", document("review", "External."))
    shared = write_skill(external, "shared", document("shared", "External."))
    write_skill(bundled, "shared", document("shared", "Bundled."))
    builtin = write_skill(bundled, "builtin", document("builtin"))
    monkeypatch.setattr(skills, "BUNDLED_SKILLS", bundled / ".voc/skills")
    catalog = await skills.discover_skills(
        target,
        ["../missing", "../external/.voc/skills", str(external / ".voc/skills")],
    )
    assert [(s.name, s.path) for s in catalog.skills] == [
        ("review", str(local.relative_to(target))),
        ("shared", str(shared)),
        ("builtin", str(builtin)),
    ]
    assert [s.description for s in catalog.skills] == [
        "Project.",
        "External.",
        "Review code.",
    ]


async def test_home_relative_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    path = write_skill(tmp_path, "shared", document("shared"))
    target = tmp_path / "target"
    target.mkdir()
    catalog = await skills.discover_skills(
        target, ["~/.voc/skills"], include_builtin=False
    )
    assert [skill.path for skill in catalog.skills] == [str(path)]


@pytest.mark.parametrize("link_root", [False, True])
async def test_external_symlink(tmp_path: Path, link_root: bool) -> None:
    target = tmp_path / "target"
    external = write_skill(tmp_path / "external", "shared", document("shared"))
    local = target / ".voc/skills"
    link = local if link_root else local / "shared"
    link.parent.mkdir(parents=True)
    link.symlink_to(
        external.parent.parent if link_root else external.parent,
        target_is_directory=True,
    )
    catalog = await skills.discover_skills(target, include_builtin=False)
    assert [skill.path for skill in catalog.skills] == [str(external)]


async def test_sources_share_catalog_limit(tmp_path: Path) -> None:
    target = tmp_path / "target"
    for index in reversed(range(skills.SKILL_LIMIT + 2)):
        write_skill(target, f"s-{index:02}", document(f"s-{index:02}"))
    external = tmp_path / "external"
    write_skill(external, "shared", document("shared"))
    catalog = await skills.discover_skills(target, [str(external / ".voc/skills")])
    assert [skill.name for skill in catalog.skills] == [
        f"s-{index:02}" for index in range(skills.SKILL_LIMIT)
    ]
    assert catalog.truncated


async def test_builtin_skill_is_available_without_project_skills(
    tmp_path: Path,
) -> None:
    catalog = await skills.discover_skills(tmp_path)
    skill = next(
        skill for skill in catalog.skills if skill.name == "workflow-authoring"
    )
    assert Path(skill.path).is_absolute()
    assert Path(skill.path).is_file()
    assert not Path(skill.path).is_relative_to(tmp_path)


async def test_legacy_skill_directory_requires_explicit_configuration(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / ".vocode/skills/legacy/SKILL.md"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(document("legacy"))
    current = write_skill(tmp_path, "review", document("review"))
    catalog = await skills.discover_skills(tmp_path, include_builtin=False)
    assert [skill.path for skill in catalog.skills] == [
        str(current.relative_to(tmp_path))
    ]
    configured = await skills.discover_skills(
        tmp_path, [".vocode/skills"], include_builtin=False
    )
    assert [skill.name for skill in configured.skills] == ["review", "legacy"]


async def test_invalid_project_skill_does_not_shadow_builtin(tmp_path: Path) -> None:
    write_skill(tmp_path, "workflow-authoring", "Missing frontmatter")
    catalog = await skills.discover_skills(tmp_path)
    assert [skill.name for skill in catalog.skills] == ["workflow-authoring"]
    assert (await skills.discover_skills(tmp_path, include_builtin=False)).skills == []


async def test_invalid_file_symlink_does_not_hide_other_skills(tmp_path: Path) -> None:
    broken = write_skill(tmp_path, "broken", document("broken"))
    broken.unlink()
    broken.symlink_to(broken.name)
    write_skill(tmp_path, "valid", document("valid"))
    catalog = await skills.discover_skills(tmp_path, include_builtin=False)
    assert [skill.name for skill in catalog.skills] == ["valid"]


async def test_workflow_authoring_example_runs_and_replays(tmp_path: Path) -> None:
    content = (skills.BUNDLED_SKILLS / "workflow-authoring/SKILL.md").read_text()
    examples = re.findall(r"```python\n(.*?)```", content, re.DOTALL)
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "greeting.py").write_text(examples[0])
    runner = WorkflowRunner(tmp_path)
    descriptor = await (await runner.project()).resolver.describe("greeting:main")
    descriptor.validate_input({"name": "Ada"})
    result = await runner.run(descriptor.reference, input={"name": "Ada"})
    assert isinstance(result, dict)
    assert result["message"] == "Hello, Ada!"
    assert runner.last_execution_id is not None
    assert await WorkflowRunner(tmp_path).resume(runner.last_execution_id) == result
