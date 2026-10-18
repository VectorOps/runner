import asyncio
from pathlib import Path

import pytest

from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
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
async def environment(tmp_path: Path) -> Path:
    return tmp_path


@pytest.mark.parametrize("format", ["v4a", "patch"])
async def test_failed_hunk_leaves_whole_file_unchanged(
    tmp_path: Path, environment: Path, format: str
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
    tmp_path: Path, environment: Path, format: str
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


async def test_context_only_sections_in_branching_patch(
    tmp_path: Path, environment: Path
) -> None:
    text = envelope("""*** Update File: frontend/tests/branching.test.tsx
@@
 vitest.test.each([false, true])(
@@
   },
 );
+
+vitest.test("failed workflow stop exposes a retry control", () => {
+  const stopped = item(
+    "failed-stop",
+    null,
+    "root",
+    { kind: "workflow.complete" },
+    null,
+  );
+  stopped.step = {
+    ...(stopped.step as Record<string, Json>),
+    outcome: {
+      kind: "failed",
+      error: { type: "WorkflowError", message: "Workflow stopped" },
+    },
+  };
+  const view = testing.render(
+    <StepBranchControls step={steps.stepView(stopped)} actions={actions()} />,
+  );
+  vitest
+    .expect(view.getByRole("button", { name: "Retry this step" }))
+    .toBeTruthy();
+});
\x20
 vitest.test(
@@
-      view.getByRole("button", { name: "Restart this step" }),
+      view.getByRole("button", { name: "Retry this step" }),
@@
-      .expect(view.getByRole("button", { name: "Restart this step" }))
+      .expect(view.getByRole("button", { name: "Retry this step" }))
@@
-      view.getByRole("button", { name: "Restart this step" }),
+      view.getByRole("button", { name: "Retry this step" }),
@@
-    testing.fireEvent.click(view.getByRole("button", { name: "Restart step" }));
+    testing.fireEvent.click(view.getByRole("button", { name: "Retry step" }));""")
    original = "\n".join(
        [
            "  },",
            ");",
            "vitest.test.each([false, true])(",
            "  cases,",
            "  },",
            ");",
            "",
            "vitest.test(",
            '      view.getByRole("button", { name: "Restart this step" }),',
            '      .expect(view.getByRole("button", { name: "Restart this step" }))',
            '      view.getByRole("button", { name: "Restart this step" }),',
            '    testing.fireEvent.click(view.getByRole("button", { name: "Restart step" }));',
            "",
        ]
    )
    path = tmp_path / "frontend/tests/branching.test.tsx"
    path.parent.mkdir(parents=True)
    path.write_text(original)
    result = await PatchService(environment).apply(text)
    assert not result.is_error
    insertion = "\n".join(
        line[1:] for line in text.splitlines() if line.startswith("+")
    ).split("\n      view.getByRole", 1)[0]
    expected = original.replace("Restart", "Retry").replace(
        ");\n\nvitest.test(", f");\n{insertion}\n\nvitest.test(", 1
    )
    assert path.read_text() == expected
    result = await PatchService(environment).apply(text, reverse=True)
    assert not result.is_error
    assert path.read_text() == original


@pytest.mark.parametrize("eof", ["", "\n*** End of File"])
def test_context_only_section_preserves_whitespace(eof: str) -> None:
    operations, errors = v4a.parse(
        envelope(f"*** Update File: file\n@@\n context{eof}")
    )
    assert not errors
    original = "  context  \r\n"
    assert operations[0].plan(original).content == original


@pytest.mark.parametrize(
    "original", ["old\n", "old\ncontext\n", "context\nold\nmissing\n"]
)
async def test_context_only_sections_require_ordered_matches(
    tmp_path: Path, environment: Path, original: str
) -> None:
    path = tmp_path / "file"
    path.write_text(original)
    result = await PatchService(environment).apply(
        envelope("*** Update File: file\n@@\n context\n@@\n-old\n+new\n@@\n final")
    )
    assert result.is_error
    assert "context did not match" in result.errors[0].msg
    assert path.read_text() == original


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


@pytest.mark.parametrize("path", ["C:\\file", ".", "/", "~", "bad\0file"])
def test_paths(path: str) -> None:
    assert v4a.parse(envelope(f"*** Add File: {path}\n+content"))[1]
    assert search_replace.parse(block(path, "", "content"))[1]


async def test_reverse_update_add_and_move(tmp_path: Path, environment: Path) -> None:
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


async def test_reverse_delete_is_rejected(environment: Path, tmp_path: Path) -> None:
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
    environment: Path, tmp_path: Path
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


async def test_empty_operations_rejected(
    environment: Path,
) -> None:
    assert (await PatchService(environment).apply(envelope(""))).is_error


@pytest.mark.parametrize("body", ["", "\n@@", "\n@@ anchor", "\n@@\n*** End of File"])
async def test_empty_updates_are_noops(
    tmp_path: Path, environment: Path, body: str
) -> None:
    path = tmp_path / "file"
    path.write_text("anchor\nold\n")
    modified = path.stat().st_mtime_ns
    text = envelope(f"*** Update File: file{body}")
    result = await PatchService(environment).apply(text)
    assert not result.is_error
    assert not result.changes
    assert path.read_text() == "anchor\nold\n"
    assert path.stat().st_mtime_ns == modified
    path.unlink()
    assert (await PatchService(environment).apply(text)).is_error


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_fuzzy_edits_preserve_context(newline: str) -> None:
    original = newline.join(
        ["  keep  ", "  old", "\tmiddle\t", "remove", "  tail  ", ""]
    )
    text = envelope(
        "*** Update File: file\n@@\n keep\n-old\n+new\n middle\n-remove\n+replacement\n tail"
    )
    operations, errors = v4a.parse(text)
    assert not errors
    expected = original.replace("  old", "new").replace("remove", "replacement")
    assert operations[0].plan(original).content == expected
    reverse, errors = v4a.parse(text, reverse=True)
    assert not errors
    assert reverse[0].plan(expected).content == expected.replace("new", "old").replace(
        "replacement", "remove"
    )


@pytest.mark.parametrize(
    "body, original, expected",
    [
        ("@@\n+zero", "one\ntwo\n", "zero\none\ntwo\n"),
        ("@@\n+zero", "", "zero"),
        ("@@\n-one\n+ONE\n@@\n+inserted", "one\ntwo\n", "ONE\ninserted\ntwo\n"),
        ("@@\n one\n@@\n+inserted", "one\ntwo\n", "one\ninserted\ntwo\n"),
        ("@@ anchor\n@@\n+inserted", "anchor\ntwo\n", "anchor\ninserted\ntwo\n"),
        ("@@\n+last\n*** End of File", "one\ntwo\n", "one\ntwo\nlast\n"),
        ("@@\n-one\n+ONE\n+extra\n@@\n-two\n+TWO", "one\ntwo\n", "ONE\nextra\nTWO\n"),
    ],
)
def test_insertions_and_original_cursor(
    body: str, original: str, expected: str
) -> None:
    text = envelope(f"*** Update File: file\n{body}")
    operations, errors = v4a.parse(text)
    assert not errors
    assert operations[0].plan(original).content == expected
    reverse, errors = v4a.parse(text, reverse=True)
    assert not errors
    assert reverse[0].plan(expected).content == original


@pytest.mark.parametrize(
    "body, original, expected",
    [
        ("@@ missing\n-old\n+new", "old\n", "new\n"),
        (
            "@@ anchor\n-old\n+new\n@@ anchor\n-next\n+NEXT",
            "anchor\nold\nnext\n",
            "anchor\nnew\nNEXT\n",
        ),
        ("@@   foo\n-old\n+new", "foo\nold\n  foo\nold\n", "foo\nold\n  foo\nnew\n"),
    ],
)
def test_anchor_hints(body: str, original: str, expected: str) -> None:
    operations, errors = v4a.parse(envelope(f"*** Update File: file\n{body}"))
    assert not errors
    assert operations[0].plan(original).content == expected


def test_matching_prefers_exact_then_rstrip_then_strip() -> None:
    assert v4a.find_context(["  old  ", "old  ", "old"], ["old"], 0) == 2
    assert v4a.find_context(["  old  ", "old  "], ["old"], 0) == 1
    assert v4a.find_context(["  old  "], ["old"], 0) == 0


@pytest.mark.parametrize("body", ["", "\n@@", "\n@@\n*** End of File"])
@pytest.mark.parametrize("payload", [b"\x00\xff\xfe\r\n\x80", b"one\r\ntwo\n", b""])
async def test_binary_move_roundtrip(tmp_path: Path, body: str, payload: bytes) -> None:
    environment = tmp_path
    source = tmp_path / "old"
    destination = tmp_path / "nested/new"
    source.write_bytes(payload)
    text = envelope(f"*** Update File: old\n*** Move to: nested/new{body}")
    service = PatchService(environment)
    result = await service.apply(text)
    assert not result.is_error
    assert not source.exists()
    assert destination.read_bytes() == payload
    result = await service.apply(text, reverse=True)
    assert not result.is_error
    assert not destination.exists()
    assert source.read_bytes() == payload


async def test_binary_move_safety(tmp_path: Path, environment: Path) -> None:
    source = tmp_path / "old"
    destination = tmp_path / "new"
    payload = b"\xff\x00\x80"
    source.write_bytes(payload)
    destination.write_bytes(b"keep")
    service = PatchService(environment)
    for body in [
        "*** Move to: new",
        "*** Move to: ./old",
        "*** Move to: new\n@@\n-old\n+new",
    ]:
        result = await service.apply(envelope(f"*** Update File: old\n{body}"))
        assert result.is_error
        assert source.read_bytes() == payload
        assert destination.read_bytes() == b"keep"
    assert (await service.apply(envelope("*** Add File: old\n+text"))).is_error
    assert source.read_bytes() == payload


async def test_binary_move_conflicting_destinations(
    tmp_path: Path, environment: Path
) -> None:
    (tmp_path / "old").write_bytes(b"\xff")
    result = await PatchService(environment).apply(
        envelope("*** Update File: old\n*** Move to: new\n*** Add File: new\n+text")
    )
    assert result.is_error
    assert not result.changes
    assert (tmp_path / "old").read_bytes() == b"\xff"
    assert not (tmp_path / "new").exists()


async def test_move_conflict(environment: Path, tmp_path: Path) -> None:
    (tmp_path / "file").write_text("old\n")
    (tmp_path / "destination").write_text("keep\n")
    result = await PatchService(environment).apply(
        envelope("*** Update File: file\n*** Move to: destination\n@@\n-old\n+new")
    )
    assert result.is_error
    assert (tmp_path / "file").read_text() == "old\n"
    assert (tmp_path / "destination").read_text() == "keep\n"


async def test_bounded_reads_and_invalid_text(tmp_path: Path) -> None:
    environment = tmp_path
    service = PatchService(environment, max_write_bytes=20)
    (tmp_path / "file").write_bytes(b"old\r\n")
    text = envelope("*** Update File: file\n@@\n-old\n+new")
    assert not (await service.apply(text)).is_error
    assert (tmp_path / "file").read_bytes() == b"new\r\n"
    (tmp_path / "file").write_bytes(b"\xff")
    assert (await service.apply(text)).is_error
    (tmp_path / "file").write_bytes(b"x" * 21)
    assert (await service.apply(text)).is_error
    assert (await PatchService(environment, max_patch_bytes=1).apply(text)).is_error


class FailingService(PatchService):
    fail_remove: bool = False
    cancel_write: bool = False

    def _write(self, destination: str, content: bytes, *, creating: bool) -> None:
        if self.cancel_write:
            raise asyncio.CancelledError
        super()._write(destination, content, creating=creating)

    def _remove(self, path: str) -> None:
        if self.fail_remove:
            raise EnvironmentError(
                ErrorCode.IO, "remove failed", may_have_occurred=True
            )
        super()._remove(path)


@pytest.mark.parametrize("binary", [False, True])
async def test_move_io_failure_reports_completed_write(
    tmp_path: Path, binary: bool
) -> None:
    service = FailingService(tmp_path)
    service.fail_remove = True
    payload = b"\xff\x00\x80" if binary else b"one\n"
    (tmp_path / "old").write_bytes(payload)
    body = "" if binary else "\n@@\n-one\n+two"
    result = await service.apply(
        envelope(f"*** Update File: old\n*** Move to: new{body}")
    )
    assert result.is_error
    assert result.errors[0].may_have_occurred
    assert [(change.path, change.kind) for change in result.changes] == [
        ("new", "created")
    ]
    assert (tmp_path / "old").read_bytes() == payload
    assert (tmp_path / "new").read_bytes() == (payload if binary else b"two\n")


async def test_binary_move_limits_and_cancellation(tmp_path: Path) -> None:
    source = tmp_path / "old"
    destination = tmp_path / "new"
    text = envelope("*** Update File: old\n*** Move to: new")
    service = FailingService(tmp_path, max_write_bytes=3)
    source.write_bytes(b"\xff" * 4)
    result = await service.apply(text)
    assert result.is_error
    assert source.read_bytes() == b"\xff" * 4
    assert not destination.exists()
    source.write_bytes(b"\xff" * 3)
    service.cancel_write = True
    with pytest.raises(asyncio.CancelledError):
        await service.apply(text)
    assert source.read_bytes() == b"\xff" * 3
    assert not destination.exists()


async def test_cancellation_propagates(tmp_path: Path) -> None:
    service = FailingService(tmp_path)
    service.cancel_write = True
    with pytest.raises(asyncio.CancelledError):
        await service.apply(envelope("*** Add File: file\n+value"))
