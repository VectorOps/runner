"""Interactive coding agent with an explicit child-workflow allowlist."""

import pydantic

from vectorops.runner.api import composition, llm, tools
from vectorops.runner.api import inbox
from vectorops.runner.api.configuration import get_config
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.workflows import context, prompts, settings
from vectorops.runner.workflows.tools import default_collections


@composition.workflow(
    name="Coding agent",
    description="Plan, investigate, edit, and test with explicit helper delegation.",
    kind="builtin.agent",
    version=2,
)
async def agent(
    ctx: WorkflowExecutionContext,
    model_tier: settings.ModelTier = settings.ModelTier.STRONG,
) -> pydantic.JsonValue:
    """Retain one conversation and plan, refreshing model settings each turn."""
    options = settings.AgentSettings.model_validate(
        await get_config(ctx, "workflows.agent", {}, model=settings.AgentSettings)
    )
    prompt_context = await context.load_context()
    bindings = await tools.load_collection(
        options.tool_collection, defaults=default_collections(options.agents)
    )
    conversation = llm.LLM(
        "agent",
        profile=model_tier.value,
        system_prompt=options.compose_prompt(prompts.AGENT, prompt_context),
        inbox=inbox.Inbox("steer", workflow_id=ctx.workflow.id),
    )
    while True:
        conversation.add_message(await llm.request_user_message())
        await conversation.run(tools=bindings)
