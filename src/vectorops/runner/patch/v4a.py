"""File-oriented v4a parsing and forward, whitespace-tolerant matching."""

from typing import Final, Literal, Optional
import typing
import re

import pydantic

from . import common, models

INSTRUCTION: Final = """Use *** Begin Patch and *** End Patch around file operations.
Use *** Add File: path with every content line prefixed by +;
*** Delete File: path with no body; or *** Update File: path with hunks.
An update may be empty or have *** Move to: destination after its header.
Moves without text hunks preserve binary payloads byte-for-byte.
Start hunks with @@, optionally followed by a class/function search hint.
Consecutive labeled @@ lines narrow the search in order.
Prefix context with a space, removals with -, and additions with +.
Use enough context to locate the edit. Order hunks top-to-bottom.
*** End of File after the final hunk prefers matching at EOF.
Paths may be target-relative, absolute, or home-relative, including outside the target root.
Each file may appear only once.
Adds and moves cannot overwrite existing destinations.
An optional outer ```patch fence is accepted.
Reverse swaps additions/removals and move paths; reversing Delete File is
unsupported without a saved before-image. Reverse is not a general undo.
"""

HEADER: Final = re.compile(r"^\*\*\* (Add|Update|Delete) File: (.+)$")


class Edit(models.Model):
    old_index: int
    new_index: int
    old_lines: list[str]
    new_lines: list[str]


class Chunk(models.Model):
    old_lines: list[str] = pydantic.Field(default_factory=list)
    new_lines: list[str] = pydantic.Field(default_factory=list)
    anchors: list[str] = pydantic.Field(default_factory=list)
    edits: list[Edit] = pydantic.Field(default_factory=list)
    source: str
    eof: bool = False


class Operation(models.Model):
    path: str
    kind: Literal["Add", "Update", "Delete"]
    destination: Optional[str] = None
    source: str
    chunks: list[Chunk] = pydantic.Field(default_factory=list)
    added: list[str] = pydantic.Field(default_factory=list)
    reverse: bool = False

    def plan(self, content: Optional[typing.Union[str, bytes]]) -> models.FileChange:
        """Match original sections, then apply only edits, preserving context bytes."""
        if self.kind == "Delete" and self.reverse:
            raise common.PatchFailure(
                "Cannot reverse Delete File without a before-image"
            )
        if self.kind == "Add":
            added = common.join_text(self.added, "\n", True)
            if self.reverse:
                expected = (
                    added.encode("utf-8") if isinstance(content, bytes) else added
                )
                if content != expected:
                    raise common.PatchFailure("Reverse add does not match current file")
                return models.FileChange(path=self.path, content=None)
            if content is not None:
                raise common.PatchFailure("Cannot add an existing file")
            return models.FileChange(path=self.path, content=added)
        if content is None:
            raise common.PatchFailure("File does not exist")
        if self.kind == "Delete":
            return models.FileChange(path=self.path, content=None)
        if not any(
            chunk.old_lines or chunk.new_lines or chunk.anchors for chunk in self.chunks
        ):
            return models.FileChange(
                path=self.path, content=content, destination=self.destination
            )
        lines, newline, trailing = common.split_text(common.decode_text(content))
        cursor = 0
        edits: list[tuple[int, list[str], list[str]]] = []
        for chunk in self.chunks:
            old = chunk.new_lines if self.reverse else chunk.old_lines
            start = cursor
            for anchor in chunk.anchors:
                start = advance_anchor(lines, anchor, start)
            if not old:
                position = len(lines) if chunk.eof else start
            else:
                position = None
                if chunk.eof:
                    position = find_context(
                        lines, old, max(start, len(lines) - len(old))
                    )
                if position is None:
                    position = find_context(lines, old, start)
            if position is None:
                raise common.PatchFailure(
                    "Patch context did not match", chunk=chunk.source
                )
            for edit in chunk.edits:
                offset = edit.new_index if self.reverse else edit.old_index
                removed = edit.new_lines if self.reverse else edit.old_lines
                inserted = edit.old_lines if self.reverse else edit.new_lines
                edits.append((position + offset, removed, inserted))
            cursor = position + len(old)
        result: list[str] = []
        cursor = 0
        for position, removed, inserted in edits:
            if position < cursor:
                raise common.PatchFailure("Patch edits overlap")
            result.extend(lines[cursor:position])
            result.extend(inserted)
            cursor = position + len(removed)
        result.extend(lines[cursor:])
        return models.FileChange(
            path=self.path,
            content=common.join_text(result, newline, trailing),
            destination=self.destination,
        )


def advance_anchor(lines: list[str], anchor: str, start: int) -> int:
    """Use exact then stripped skip-ahead hints, ignoring missing or prior hints."""
    for normalize in (str, str.strip):
        expected = normalize(anchor)
        if any(normalize(line) == expected for line in lines[:start]):
            continue
        for index in range(start, len(lines)):
            if normalize(lines[index]) == expected:
                return index + 1
    return start


def find_context(lines: list[str], context: list[str], start: int) -> Optional[int]:
    for normalize in (str, str.rstrip, str.strip):
        expected = [normalize(line) for line in context]
        for index in range(start, len(lines) - len(context) + 1):
            if [
                normalize(line) for line in lines[index : index + len(context)]
            ] == expected:
                return index
    return None


def parse_chunks(body: list[str]) -> list[Chunk]:
    """Retain stacked anchors and fail the entire operation on malformed hunks."""
    chunks: list[Chunk] = []
    anchors: list[str] = []
    raw: list[str] = []
    old: list[str] = []
    new: list[str] = []
    edits: list[Edit] = []
    removed: list[str] = []
    inserted: list[str] = []
    has_body = False
    ended = False

    def finish_edit() -> None:
        nonlocal removed, inserted
        if removed or inserted:
            edits.append(
                Edit(
                    old_index=len(old) - len(removed),
                    new_index=len(new) - len(inserted),
                    old_lines=removed,
                    new_lines=inserted,
                )
            )
            removed, inserted = [], []

    def finish(eof: bool = False) -> None:
        nonlocal anchors, raw, old, new, edits, has_body
        finish_edit()
        chunks.append(
            Chunk(
                old_lines=old,
                new_lines=new,
                anchors=anchors,
                edits=edits,
                source="\n".join(raw),
                eof=eof,
            )
        )
        anchors, raw, old, new = [], [], [], []
        edits = []
        has_body = False

    for line in body:
        if ended:
            raise common.PatchFailure("End of File must terminate the update")
        if line == "@@" or line.startswith("@@ "):
            if has_body:
                finish()
            raw.append(line)
            if line[2:].strip():
                anchors.append(line[3:])
        elif line == "*** End of File":
            raw.append(line)
            finish(eof=True)
            ended = True
        else:
            raw.append(line)
            has_body = True
            if line.startswith("-"):
                old.append(line[1:])
                removed.append(line[1:])
            elif line.startswith("+"):
                new.append(line[1:])
                inserted.append(line[1:])
            elif line.startswith(" ") or line == "":
                finish_edit()
                old.append(line[1:])
                new.append(line[1:])
            else:
                raise common.PatchFailure(
                    f"Invalid update line: {line!r}", chunk="\n".join(raw)
                )
    if raw:
        finish()
    return chunks


def parse(
    text: str, *, reverse: bool = False
) -> tuple[list[Operation], list[models.PatchError]]:
    lines = text.splitlines()
    if lines and lines[0].strip() == "```patch":
        if lines[-1].strip() != "```":
            return [], [models.PatchError(msg="Missing closing patch fence")]
        lines = lines[1:-1]
    if (
        not lines
        or lines[0].strip() != "*** Begin Patch"
        or lines[-1].strip() != "*** End Patch"
    ):
        return [], [models.PatchError(msg="Missing patch envelope")]
    operations: list[Operation] = []
    errors: list[models.PatchError] = []
    seen: set[str] = set()
    invalid: set[str] = set()
    index = 1
    while index < len(lines) - 1:
        start = index
        match = HEADER.fullmatch(lines[index].strip())
        index += 1
        if match is None:
            errors.append(
                models.PatchError(msg="Invalid file header", chunk=lines[start])
            )
            continue
        kind, path = match.groups()
        while index < len(lines) - 1 and HEADER.fullmatch(lines[index].strip()) is None:
            index += 1
        body = lines[start + 1 : index]
        source = "\n".join(lines[start:index])
        try:
            path = common.validate_path(path.strip())
            if path in seen:
                raise common.PatchFailure("Each file may appear only once")
            seen.add(path)
            destination = None
            chunks: list[Chunk] = []
            added: list[str] = []
            if kind == "Add":
                if any(not line.startswith("+") for line in body):
                    raise common.PatchFailure("Add File lines must start with +")
                added = [line[1:] for line in body]
            elif kind == "Delete":
                if body:
                    raise common.PatchFailure("Delete File must not contain a body")
            else:
                if body and body[0].startswith("*** Move to: "):
                    destination = common.validate_path(body.pop(0)[13:].strip())
                    if destination == path:
                        raise common.PatchFailure("Move destination equals source")
                chunks = parse_chunks(body)
            operations.append(
                Operation(
                    path=path,
                    kind=(
                        "Add"
                        if kind == "Add"
                        else "Delete" if kind == "Delete" else "Update"
                    ),
                    destination=destination,
                    source=source,
                    chunks=chunks,
                    added=added,
                    reverse=reverse,
                )
            )
        except (common.PatchFailure, pydantic.ValidationError) as exc:
            invalid.add(path)
            errors.append(models.PatchError(msg=str(exc), filename=path, chunk=source))
    operations = [op for op in operations if op.path not in invalid]
    if reverse:
        operations = [
            (
                op.model_copy(update={"path": op.destination, "destination": op.path})
                if op.destination
                else op
            )
            for op in operations
        ]
    if not operations and not errors:
        errors.append(models.PatchError(msg="Empty patch"))
    return operations, errors
