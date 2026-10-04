import asyncio
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from vectorops.runner.api import composition
from vectorops.runner.references import WorkflowReference
from vectorops.runner.runner.resolver import WorkflowResolver

SOURCE = """
from vectorops.runner.api.composition import workflow

@workflow(name="Double", description="Double a number", kind="math.double", version=2)
async def double(ctx, value: int, scale: int = 2) -> int:
    return value * scale

async def plain(ctx, text: str = "hello") -> str:
    return text
"""


@pytest.mark.parametrize("reference", ["foo/bar:buzz", "python:package.module:main"])
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
    path.write_text("raise RuntimeError('broken update')")
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
    path.write_text("async def main(ctx): return 1\n")
    resolver = WorkflowResolver(tmp_path)
    first = await resolver.resolve("value:main")
    stat = path.stat()
    path.write_text("async def main(ctx): return 2\n")
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    second = await resolver.resolve("value:main")
    assert await first(None) == 1
    assert await second(None) == 2
