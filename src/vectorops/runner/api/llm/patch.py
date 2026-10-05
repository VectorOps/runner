"""Apply-patch tool binding using the standard approval and replay machinery."""

from typing import Optional

import connect
import pydantic

from vectorops.runner.patch import PatchService, get_system_instruction
from vectorops.runner.runner.activity_context import ActivityExecutionContext

from . import tools


class PatchArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    text: str = pydantic.Field(min_length=1)
    reverse: bool = False

    @pydantic.field_validator("text")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Patch text must not be blank")
        return value


def apply_patch_tool(
    service: PatchService,
    *,
    format: str = "v4a",
    approval: Optional[tools.ToolApproval] = None,
) -> tools.Tool[PatchArguments]:
    instruction = get_system_instruction(format)

    async def invoke(
        context: ActivityExecutionContext, arguments: PatchArguments
    ) -> tools.ToolResponse:
        result = await service.apply(
            arguments.text, format=format, reverse=arguments.reverse
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.summary())], is_error=result.is_error
        )

    return tools.Tool(
        name="apply_patch",
        description=(
            "Apply a repository patch. Parse and match failures leave the entire "
            "affected file unchanged, for both supported formats. Independent files "
            "may succeed and are not rolled back. Filesystem failures, especially "
            "during moves, can leave partial changes. SUCCESS lists completed I/O; "
            "ERRORS lists failures. After errors read affected files before retrying "
            "complete corrected file operations. Do not retry successful files. "
            "Calls are serialized within this service, not against external writers.\n\n"
            + instruction
        ),
        arguments=PatchArguments,
        invoke=invoke,
        approval=approval,
    )
