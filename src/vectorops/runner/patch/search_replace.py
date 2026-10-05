"""Exact SEARCH/REPLACE blocks with all-or-nothing planning per file."""

from typing import Final, Optional

from . import common, models

INSTRUCTION: Final = """Emit fenced SEARCH/REPLACE blocks, without prose:
```text
relative/path
<<<<<<< SEARCH
exact existing lines
=======
replacement lines
>>>>>>> REPLACE
```
Multiple blocks for a file are applied in order to an in-memory copy.
SEARCH must match exactly once. Empty SEARCH creates a new file.
Empty REPLACE deletes a file only when SEARCH matches its entire contents;
delete blocks cannot be combined with other blocks for that file.
Adds cannot overwrite existing files. New files have no implicit final newline.
Use an explicit blank content line to represent a final newline in new files.
Reverse applies blocks in reverse order with SEARCH and REPLACE swapped.
"""


class Block(models.Model):
    search: list[str]
    replace: list[str]
    source: str


class Operation(models.Model):
    path: str
    destination: Optional[str] = None
    source: str
    blocks: list[Block]

    def plan(self, content: Optional[str]) -> models.FileChange:
        """Apply blocks in memory, emitting no change on any failure."""
        lines, newline, trailing = common.split_text(content or "")
        exists = content is not None
        for block in self.blocks:
            if not block.search:
                if exists:
                    raise common.PatchFailure(
                        "Cannot add an existing file", chunk=block.source
                    )
                lines, newline, trailing = common.split_text("\n".join(block.replace))
                exists = True
            elif not block.replace:
                if len(self.blocks) != 1:
                    raise common.PatchFailure(
                        "Delete cannot be combined with other blocks",
                        chunk=block.source,
                    )
                if not exists or lines != block.search:
                    raise common.PatchFailure(
                        "Delete SEARCH must match the entire file", chunk=block.source
                    )
                return models.FileChange(path=self.path, content=None)
            else:
                matches = [
                    index
                    for index in range(len(lines) - len(block.search) + 1)
                    if lines[index : index + len(block.search)] == block.search
                ]
                if not exists or not matches:
                    raise common.PatchFailure(
                        "SEARCH did not match", chunk=block.source
                    )
                if len(matches) != 1:
                    raise common.PatchFailure(
                        "SEARCH is ambiguous; include more context", chunk=block.source
                    )
                start = matches[0]
                lines[start : start + len(block.search)] = block.replace
        return models.FileChange(
            path=self.path, content=common.join_text(lines, newline, trailing)
        )


def parse(
    text: str, *, reverse: bool = False
) -> tuple[list[Operation], list[models.PatchError]]:
    lines = text.splitlines()
    grouped: dict[str, list[Block]] = {}
    errors: list[models.PatchError] = []
    invalid: set[str] = set()
    index = 0
    while index < len(lines):
        if not lines[index].strip():
            index += 1
            continue
        if not lines[index].startswith("```"):
            errors.append(
                models.PatchError(
                    msg="Unexpected content outside a block", chunk=lines[index]
                )
            )
            index += 1
            continue
        start = index
        index += 1
        path = lines[index].strip() if index < len(lines) else ""
        end = next(
            (i for i in range(index + 1, len(lines)) if lines[i] == "```"), len(lines)
        )
        source = "\n".join(lines[start : min(end + 1, len(lines))])
        body = lines[index + 1 : end]
        index = end + 1
        try:
            path = common.validate_path(path)
            if end == len(lines):
                raise common.PatchFailure("Missing closing code fence")
            if not body or body[0] != "<<<<<<< SEARCH" or body[-1] != ">>>>>>> REPLACE":
                raise common.PatchFailure("Missing SEARCH/REPLACE markers")
            separators = [i for i, line in enumerate(body) if line == "======="]
            if len(separators) != 1:
                raise common.PatchFailure("Expected one ======= separator")
            middle = separators[0]
            search, replace = body[1:middle], body[middle + 1 : -1]
            if not search and not replace:
                raise common.PatchFailure("Empty SEARCH/REPLACE block")
            grouped.setdefault(path, []).append(
                Block(search=search, replace=replace, source=source)
            )
        except common.PatchFailure as exc:
            invalid.add(path)
            errors.append(models.PatchError(msg=str(exc), filename=path, chunk=source))
    operations: list[Operation] = []
    for path, blocks in grouped.items():
        if path in invalid:
            continue
        source = "\n".join(block.source for block in blocks)
        if reverse:
            blocks = [
                Block(search=b.replace, replace=b.search, source=b.source)
                for b in reversed(blocks)
            ]
        operations.append(Operation(path=path, source=source, blocks=blocks))
    if not operations and not errors:
        errors.append(models.PatchError(msg="No SEARCH/REPLACE blocks found"))
    return operations, errors
