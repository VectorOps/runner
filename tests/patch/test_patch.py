import asyncio
from pathlib import Path

import pytest

from vectorops.runner.environment import models as filesystem
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService
from vectorops.runner.patch import common, search_replace, v4a


def envelope(body: str) -> str:
    return f"*** Begin Patch\n{body}\n*** End Patch"


def block(path: str, search: str, replace: str) -> str:
    return (
        f"```text\n{path}\n<<<<<<< SEARCH\n"
        + (search + "\n" if search else "")
        + "=======\n"
        + (replace + "\n" if replace else "")
        + ">>>>>>> REPLACE\n```"
    )


@pytest.fixture
async def environment(tmp_path: Path) -> LocalEnvironment:
    return await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )


@pytest.mark.parametrize("format", ["v4a", "patch"])
async def test_failed_hunk_leaves_whole_file_unchanged(
    tmp_path: Path, environment: LocalEnvironment, format: str
) -> None:
    (tmp_path / "file").write_text("one\ntwo\n")
    text = (
        envelope(
            "*** Update File: file\n@@\n-one\n+ONE\n@@\n-missing\n+value\n*** Add File: good\n+ok"
        )
        if format == "v4a"
        else "\n".join(
            [
                block("file", "one", "ONE"),
                block("file", "missing", "value"),
                block("good", "", "ok"),
            ]
        )
    )
    result = await PatchService(environment).apply(text, format=format)
    assert result.is_error
    assert (tmp_path / "file").read_text() == "one\ntwo\n"
    assert [change.path for change in result.changes] == ["good"]
    assert result.errors[0].chunk


@pytest.mark.parametrize("format", ["v4a", "patch"])
async def test_malformed_block_invalidates_file(
    tmp_path: Path, environment: LocalEnvironment, format: str
) -> None:
    (tmp_path / "file").write_text("old\n")
    text = (
        envelope(
            "*** Update File: file\n@@\n-old\n+new\n@@\ninvalid\n*** Add File: good\n+ok"
        )
        if format == "v4a"
        else "\n".join(
            [
                block("file", "old", "new"),
                "```text\n./file\nbroken\n```",
                block("good", "", "ok"),
            ]
        )
    )
    result = await PatchService(environment).apply(text, format=format)
    assert result.is_error
    assert (tmp_path / "file").read_text() == "old\n"
    assert (tmp_path / "good").exists()


def test_stacked_anchors() -> None:
    text = envelope(
        "*** Update File: file\n@@ class Second:\n@@ def method():\n-        value = 1\n+        value = 2"
    )
    operations, errors = v4a.parse(text)
    assert not errors
    content = "class First:\n    def method():\n        value = 1\nclass Second:\n    def method():\n        value = 1\n"
    assert (
        operations[0].plan(content).content
        == content.rsplit("value = 1", 1)[0] + "value = 2\n"
    )


@pytest.mark.parametrize("original", ["old", "old\n", "old\r\n", "old\n\n"])
@pytest.mark.parametrize("format", ["v4a", "patch"])
def test_line_endings(original: str, format: str) -> None:
    if format == "v4a":
        operations, errors = v4a.parse(
            envelope("*** Update File: file\n@@\n-old\n+new")
        )
        change = operations[0].plan(original)
    else:
        replacements, errors = search_replace.parse(block("file", "old", "new"))
        change = replacements[0].plan(original)
    assert not errors
    assert change.content == original.replace("old", "new")


def test_whitespace_fallback_and_eof() -> None:
    operations, errors = v4a.parse(
        envelope("*** Update File: file\n@@\n-old\n+new\n*** End of File")
    )
    assert not errors
    assert operations[0].plan("  old\n  old\n").content == "  old\nnew\n"


def test_search_ambiguity_and_delete_validation() -> None:
    operations, _ = search_replace.parse(block("file", "one", "ONE"))
    with pytest.raises(common.PatchFailure, match="ambiguous"):
        operations[0].plan("one\none\n")
    operations, _ = search_replace.parse(block("file", "one", ""))
    with pytest.raises(common.PatchFailure, match="entire file"):
        operations[0].plan("one\ntwo\n")
    assert operations[0].plan("one\n").content is None


@pytest.mark.parametrize(
    "path", ["../file", "a/../file", "/file", "C:\\file", ".", "~file"]
)
def test_paths(path: str) -> None:
    assert v4a.parse(envelope(f"*** Add File: {path}\n+content"))[1]
    assert search_replace.parse(block(path, "", "content"))[1]


async def test_reverse_update_add_and_move(
    tmp_path: Path, environment: LocalEnvironment
) -> None:
    service = PatchService(environment)
    add = envelope("*** Add File: old\n+one")
    move = envelope("*** Update File: old\n*** Move to: nested/new\n@@\n-one\n+two")
    assert not (await service.apply(add)).is_error
    assert not (await service.apply(move)).is_error
    assert not (tmp_path / "old").exists()
    result = await service.apply(move, reverse=True)
    assert not result.is_error
    assert (tmp_path / "old").read_text() == "one\n"
    assert not (tmp_path / "nested/new").exists()
    result = await service.apply(add, reverse=True)
    assert not result.is_error
    assert result.changes[0].kind == "deleted"
    assert not (tmp_path / "old").exists()


async def test_reverse_delete_is_rejected(
    environment: LocalEnvironment, tmp_path: Path
) -> None:
    result = await PatchService(environment).apply(
        envelope("*** Delete File: file"), reverse=True
    )
    assert result.is_error
    assert "before-image" in result.errors[0].msg
    assert not (tmp_path / "file").exists()


def test_reverse_search_sequence() -> None:
    text = block("file", "one", "two") + "\n" + block("file", "two", "three")
    operations, errors = search_replace.parse(text, reverse=True)
    assert not errors
    assert operations[0].plan("three\n").content == "one\n"


async def test_overwrite_and_duplicate_rejected(
    environment: LocalEnvironment, tmp_path: Path
) -> None:
    (tmp_path / "file").write_text("original")
    service = PatchService(environment)
    assert (await service.apply(envelope("*** Add File: file\n+new"))).is_error
    result = await service.apply(
        envelope(
            "*** Update File: file\n@@\n-original\n+new\n*** Update File: ./file\n@@\n-new\n+bad"
        )
    )
    assert result.is_error
    assert (tmp_path / "file").read_text() == "original"


@pytest.mark.parametrize(
    "text",
    [
        "*** Begin Patch\n*** End Patch",
        "*** Begin Patch\n*** Update File: file\n*** End Patch",
    ],
)
async def test_empty_operations_rejected(
    environment: LocalEnvironment, text: str
) -> None:
    assert (await PatchService(environment).apply(text)).is_error


async def test_move_conflict(environment: LocalEnvironment, tmp_path: Path) -> None:
    (tmp_path / "file").write_text("old\n")
    (tmp_path / "destination").write_text("keep\n")
    result = await PatchService(environment).apply(
        envelope("*** Update File: file\n*** Move to: destination\n@@\n-old\n+new")
    )
    assert result.is_error
    assert (tmp_path / "file").read_text() == "old\n"
    assert (tmp_path / "destination").read_text() == "keep\n"


async def test_bounded_reads_and_invalid_text(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            max_read_bytes=2,
            max_write_bytes=20,
        )
    )
    service = PatchService(environment)
    (tmp_path / "file").write_bytes(b"old\r\n")
    text = envelope("*** Update File: file\n@@\n-old\n+new")
    assert not (await service.apply(text)).is_error
    assert (tmp_path / "file").read_bytes() == b"new\r\n"
    (tmp_path / "file").write_bytes(b"\xff")
    assert (await service.apply(text)).is_error
    (tmp_path / "file").write_bytes(b"x" * 21)
    assert (await service.apply(text)).is_error
    assert (await PatchService(environment, max_patch_bytes=1).apply(text)).is_error


class FailingEnvironment(LocalEnvironment):
    fail_remove: bool = False
    cancel_write: bool = False

    async def write_file(self, request: filesystem.WriteRequest) -> None:
        if self.cancel_write:
            raise asyncio.CancelledError
        await super().write_file(request)

    async def remove(self, request: filesystem.RemoveRequest) -> None:
        if self.fail_remove:
            raise EnvironmentError(
                ErrorCode.IO, "remove failed", may_have_occurred=True
            )
        await super().remove(request)


async def test_move_io_failure_reports_completed_write(tmp_path: Path) -> None:
    environment = await FailingEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    environment.fail_remove = True
    (tmp_path / "old").write_text("one\n")
    result = await PatchService(environment).apply(
        envelope("*** Update File: old\n*** Move to: new\n@@\n-one\n+two")
    )
    assert result.is_error
    assert result.errors[0].may_have_occurred
    assert [(change.path, change.kind) for change in result.changes] == [
        ("new", "created")
    ]
    assert (tmp_path / "old").read_text() == "one\n"


async def test_cancellation_propagates(tmp_path: Path) -> None:
    environment = await FailingEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    environment.cancel_write = True
    with pytest.raises(asyncio.CancelledError):
        await PatchService(environment).apply(envelope("*** Add File: file\n+value"))
