"""Interactive coding agent with an explicit child-workflow allowlist."""

import pydantic

from vectorops.runner.api import composition, llm, tools
from vectorops.runner.api.chat_input import LEGACY_IMAGE_BYTES, MAX_IMAGE_BYTES
from vectorops.runner.api.configuration import get_config
from vectorops.runner.api.inbox import Inbox
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.workflows import context, prompts, settings
from vectorops.runner.workflows.tools import default_collections


@composition.workflow(
    name="Coding agent",
    description="Plan, investigate, edit, and test with explicit helper delegation.",
    kind="builtin.agent",
    version=6,
)
async def agent(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    """Retain one conversation and plan, refreshing model settings each turn."""
    options = settings.AgentSettings.model_validate(
        await get_config(ctx, "workflows.agent", {}, model=settings.AgentSettings)
    )
    prompt_context = await context.load_context()
    defaults = default_collections(
        options.agents,
        include_mcp=ctx.version >= 3,
        include_web_fetch=ctx.version >= 4,
    )
    bindings: list[tools.ToolBinding] = []
    selected = None
    if ctx.version < 3:
        bindings = await tools.load_collection(
            options.tool_collection, defaults=defaults
        )
    else:
        selected = await tools.load_tool_set(options.tool_collection, defaults=defaults)
    conversation = llm.LLM(
        "agent",
        profile="strong",
        default_compaction=llm.Compaction() if ctx.version >= 5 else None,
        system_prompt=options.compose_prompt(prompts.AGENT, prompt_context),
        inbox=Inbox("steer", workflow_id=ctx.workflow.id),
    )
    while True:
        conversation.add_message(
            await llm.request_user_message(
                max_image_bytes=(
                    MAX_IMAGE_BYTES if ctx.version >= 6 else LEGACY_IMAGE_BYTES
                )
            )
        )
        await conversation.run(tools=bindings, tool_set=selected)
