import asyncio
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.fixtures import sources
from vectorops.runner.api import composition
from vectorops.runner.references import WorkflowReference
from vectorops.runner.runner.resolver import WorkflowResolver

SOURCE = sources.read("resolver.py")


@pytest.mark.parametrize(
    "reference", ["foo/bar:buzz", "python:package.module:main", "builtin:chat"]
)
def test_canonical_reference(reference: str) -> None:
    assert str(WorkflowReference.parse(reference)) == reference


@pytest.mark.parametrize(
    "reference",
    [
        "foo",
        "foo.py:main",
        "foo:",
        ":main",
        "foo:bad.name",
        "/foo:main",
        "../foo:main",
        "foo/../bar:main",
        "./foo:main",
        "foo//bar:main",
        "foo\\bar:main",
        "workspace:foo:main",
        "python::main",
        "python:a-b:main",
        "builtin:",
        "builtin:chat:main",
        "builtin:../chat",
        "builtin:chat.py",
        "builtin:chat/main",
        "builtin: chat",
        "builtin:chat\\main",
    ],
)
def test_invalid_reference(reference: str) -> None:
    with pytest.raises(ValueError):
        WorkflowReference.parse(reference)


async def test_metadata_and_unchanged_source_cache(tmp_path: Path) -> None:
    path = tmp_path / "math.py"
    path.write_text(SOURCE)
    resolver = WorkflowResolver(tmp_path)
    entry = await resolver.resolve("math:double")
    descriptor = await resolver.describe("math:double")
    assert descriptor.name == "Double"
    assert descriptor.description == "Double a number"
    assert (descriptor.kind, descriptor.version) == ("math.double", 2)
    assert descriptor.input_schema["required"] == ["value"]
    assert "ctx" not in descriptor.input_schema["properties"]
    descriptor.validate_input({"value": 3})
    with patch.object(Path, "read_bytes", side_effect=AssertionError("source reread")):
        assert await resolver.resolve("math:double") is entry
        assert await resolver.describe("math:double") == descriptor
        assert (await resolver.describe("math:plain")).name == "plain"


async def test_content_hash_refresh_and_invalidation(tmp_path: Path) -> None:
    path = tmp_path / "math.py"
    path.write_text(SOURCE)
    resolver = WorkflowResolver(tmp_path)
    entry = await resolver.resolve("math:double")
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    assert await resolver.resolve("math:double") is entry
    path.write_text(SOURCE.replace("Double a number", "Updated summary"))
    updated = await resolver.resolve("math:double")
    assert updated is not entry
    assert (await resolver.describe("math:double")).description == "Updated summary"
    assert isinstance(entry, composition.WorkflowDefinition)
    assert entry.description == "Double a number"
    await resolver.invalidate("math:double")
    assert await resolver.resolve("math:double") is not updated
    await resolver.invalidate()


async def test_new_deleted_and_broken_sources(tmp_path: Path) -> None:
    resolver = WorkflowResolver(tmp_path)
    path = tmp_path / "math.py"
    with pytest.raises(FileNotFoundError):
        await resolver.describe("math:double")
    path.write_text(SOURCE)
    assert (await resolver.describe("math:double")).name == "Double"
    path.write_text(sources.read("broken_update.py"))
    with pytest.raises(RuntimeError, match="broken update"):
        await resolver.describe("math:double")
    path.unlink()
    with pytest.raises(FileNotFoundError):
        await resolver.resolve("math:double")
    path.write_text(SOURCE)
    assert (await resolver.describe("math:double")).name == "Double"


async def test_concurrent_resolutions_share_import(tmp_path: Path) -> None:
    (tmp_path / "math.py").write_text(SOURCE)
    resolver = WorkflowResolver(tmp_path)
    with patch.object(
        Path, "read_bytes", autospec=True, side_effect=Path.read_bytes
    ) as read:
        entries = await asyncio.gather(
            *(resolver.resolve("math:double") for _ in range(10))
        )
    assert read.call_count == 1
    assert all(entry is entries[0] for entry in entries)


async def test_contained_symlinks_keep_requested_descriptor_reference(
    tmp_path: Path,
) -> None:
    (tmp_path / "math.py").write_text(SOURCE)
    (tmp_path / "alias.py").symlink_to(tmp_path / "math.py")
    resolver = WorkflowResolver(tmp_path)
    assert (await resolver.describe("math:double")).reference == "math:double"
    assert (await resolver.describe("alias:double")).reference == "alias:double"
    assert await resolver.resolve("math:double") is await resolver.resolve(
        "alias:double"
    )


async def test_same_size_update_does_not_use_timestamp_bytecode_cache(
    tmp_path: Path,
) -> None:
    path = tmp_path / "value.py"
    path.write_text(sources.read("return_one.py"))
    resolver = WorkflowResolver(tmp_path)
    first = await resolver.resolve("value:main")
    stat = path.stat()
    path.write_text(sources.read("return_two.py"))
    assert path.stat().st_size == stat.st_size
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    second = await resolver.resolve("value:main")
    assert await first(None) == 1
    assert await second(None) == 2


async def test_builtin_resolution_and_descriptor(tmp_path: Path) -> None:
    resolver = WorkflowResolver(tmp_path / "absent")
    with patch.object(Path, "read_bytes", side_effect=AssertionError("local read")):
        entry = await resolver.resolve("builtin:agent")
        descriptor = await resolver.describe("builtin:agent")
        assert descriptor.reference == "builtin:agent"
        assert descriptor.name == "Coding agent"
        assert descriptor.kind == "builtin.agent"
        descriptor.validate_input({})
        descriptor.validate_input({"model_tier": "weak"})
        descriptor.input_schema.clear()
        assert (await resolver.describe("builtin:agent")).input_schema
        await resolver.invalidate("builtin:agent")
        await resolver.invalidate()
        assert await resolver.resolve("builtin:agent") is entry


async def test_unknown_builtin_never_falls_back(tmp_path: Path) -> None:
    (tmp_path / "builtin.py").write_text(SOURCE)
    resolver = WorkflowResolver(tmp_path)
    for operation in (resolver.resolve, resolver.describe):
        with pytest.raises(
            ValueError, match="unknown built-in workflow: builtin:double"
        ):
            await operation("builtin:double")


async def test_builtin_catalog_merged_before_pagination(tmp_path: Path) -> None:
    (tmp_path / "aaa.py").write_text(SOURCE)
    (tmp_path / "zzz.py").write_text(SOURCE)
    os.utime(tmp_path / "aaa.py", ns=(1_000_000_000, 1_000_000_000))
    os.utime(tmp_path / "zzz.py", ns=(2_000_000_000, 2_000_000_000))
    resolver = WorkflowResolver(tmp_path)
    first = await resolver.discover(limit=1)
    assert [item.reference for item in first.workflows] == ["zzz:double"]
    assert first.next_after == "zzz:double"
    second = await resolver.discover(after=first.next_after, limit=2)
    assert [item.reference for item in second.workflows] == [
        "aaa:double",
        "builtin:agent",
    ]
    assert second.next_after == "builtin:agent"
    last = await resolver.discover(after=second.next_after, limit=1)
    assert [item.reference for item in last.workflows] == ["builtin:helper"]
    assert last.next_after is None
    assert not (await resolver.discover(after="builtin:helper")).workflows


async def test_catalog_modified_time_ties_and_refresh(tmp_path: Path) -> None:
    for name in ("zzz", "aaa"):
        path = tmp_path / f"{name}.py"
        path.write_text(SOURCE)
        os.utime(path, ns=(1_000_000_000, 1_000_000_000))
    resolver = WorkflowResolver(tmp_path)
    first = await resolver.discover(limit=1)
    assert [item.reference for item in first.workflows] == ["aaa:double"]
    second = await resolver.discover(after=first.next_after, limit=1)
    assert [item.reference for item in second.workflows] == ["zzz:double"]
    entry = await resolver.resolve("zzz:double")
    os.utime(tmp_path / "zzz.py", ns=(2_000_000_000, 2_000_000_000))
    updated = await resolver.discover(limit=1)
    assert [item.reference for item in updated.workflows] == ["zzz:double"]
    assert await resolver.resolve("zzz:double") is entry


async def test_catalog_unavailable_cursor(tmp_path: Path) -> None:
    path = tmp_path / "math.py"
    path.write_text(SOURCE)
    resolver = WorkflowResolver(tmp_path)
    first = await resolver.discover(limit=1)
    path.unlink()
    with pytest.raises(ValueError, match="restart pagination"):
        await resolver.discover(after=first.next_after)


async def test_builtin_catalog_without_local_directory(tmp_path: Path) -> None:
    catalog = await WorkflowResolver(tmp_path / "absent").discover()
    assert [item.reference for item in catalog.workflows] == [
        "builtin:agent",
        "builtin:helper",
    ]
    assert catalog.errors == []
