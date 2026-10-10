"""Importable tool factories for registry and configuration integration tests."""

import connect
import pydantic

from vectorops.runner.api import tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext


class EchoConfig(tools.ToolConfig):
    prefix: str = "echo"


class EchoArguments(pydantic.BaseModel):
    text: str


@tools.register_tool("test_echo", config=EchoConfig)
async def echo(config: EchoConfig) -> tools.ToolBinding:
    async def invoke(
        ctx: ActivityExecutionContext, args: EchoArguments
    ) -> tools.ToolResponse:
        return tools.ToolResponse(
            content=[connect.TextBlock(text=f"{config.prefix}: {args.text}")]
        )

    return tools.Tool("test_echo", "Echo supplied text", EchoArguments, invoke)
