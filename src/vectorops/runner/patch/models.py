"""Public patch results and concrete file changes, independent of patch syntax."""

from typing import Literal, Optional

import pydantic


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class PatchError(Model):
    msg: str
    filename: Optional[str] = None
    chunk: Optional[str] = None
    may_have_occurred: bool = False


class FileChange(Model):
    path: str
    content: Optional[str]
    destination: Optional[str] = None


class AppliedChange(Model):
    path: str
    kind: Literal["created", "updated", "deleted"]


class PatchResult(Model):
    changes: list[AppliedChange] = pydantic.Field(default_factory=list)
    errors: list[PatchError] = pydantic.Field(default_factory=list)

    @property
    def is_error(self) -> bool:
        return bool(self.errors)

    def summary(self) -> str:
        lines = ["SUCCESS"]
        lines.extend(f"* {change.path}: {change.kind}" for change in self.changes)
        if not self.changes:
            lines.append("None.")
        lines.extend(["", "ERRORS"])
        for error in self.errors:
            lines.append(f"[{error.filename or 'global'}]: {error.msg}")
            if error.may_have_occurred:
                lines.append("Mutation outcome is uncertain; read the affected files.")
            if error.chunk is not None:
                lines.extend(["Chunk:", error.chunk])
        if not self.errors:
            lines.append("None.")
        else:
            lines.append(
                "Read affected files before retrying. For parse or match failures, "
                "resubmit the entire corrected file operation, not only its failed hunk. "
                "Independent successful files are not rolled back."
            )
        return "\n".join(lines)
