"""Apply-patch tool binding using the standard approval and replay machinery."""

from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api.tool_registry import ToolConfig, register_tool
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService, get_system_instruction
from vectorops.runner.runner.activity_context import ActivityExecutionContext

from vectorops.runner.api import tool_contracts as tools
from .environment import environment


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
    service: Optional[PatchService] = None,
    *,
    format: str = "v4a",
    approval: Optional[tools.ToolApproval] = None,
) -> tools.Tool[PatchArguments]:
    instruction = get_system_instruction(format)
    bound_environment: Optional[LocalEnvironment] = None

    def patch_service(context: ActivityExecutionContext) -> PatchService:
        nonlocal service, bound_environment
        if service is None:
            bound_environment = environment(context)
            service = PatchService(
                bound_environment.root,
                max_write_bytes=bound_environment.config.max_write_bytes,
            )
        elif (
            bound_environment is not None
            and context.environment is not bound_environment
        ):
            raise tools.ToolExecutionError(
                "Patch bindings cannot be shared across target environments"
            )
        return service

    async def auth_context(
        context: ActivityExecutionContext, arguments: PatchArguments
    ) -> tools.AuthContext:
        scope = await patch_service(context).scope(
            arguments.text, format=format, reverse=arguments.reverse
        )
        return tools.AuthContext(properties={"scope": scope})

    async def invoke(
        context: ActivityExecutionContext, arguments: PatchArguments
    ) -> tools.ToolResponse:
        result = await patch_service(context).apply(
            arguments.text, format=format, reverse=arguments.reverse
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.summary())], is_error=result.is_error
        )

    return tools.Tool(
        name="apply_patch",
        description=(
            "Apply a patch using target-relative, absolute, or home-relative file paths, including outside the target root. "
            "Parse and match failures leave the entire "
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
        contextual_auth_context=auth_context,
    )


class PatchConfig(ToolConfig):
    format: Literal["v4a", "patch"] = "v4a"


@register_tool("apply_patch", config=PatchConfig)
def configured_patch(config: PatchConfig) -> tools.Tool[PatchArguments]:
    return apply_patch_tool(format=config.format)
