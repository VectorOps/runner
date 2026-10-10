"""Persistent user-input and LLM conversation loop."""

import pydantic

from vectorops.runner.api import composition, llm
from vectorops.runner.api.configuration import get_config
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.workflows import settings


@composition.workflow(
    name="Chat",
    description="Chat with an LLM, retaining conversation history between messages.",
    kind="builtin.chat",
)
async def chat(
    ctx: WorkflowExecutionContext,
    model_tier: settings.ModelTier = settings.ModelTier.STRONG,
) -> pydantic.JsonValue:
    """Retain conversation history while selecting current settings for each turn."""
    conversation = llm.LLM(
        "chat",
        system_prompt="You are a helpful assistant. Be clear, accurate, and concise.",
    )
    while True:
        conversation.add_message(await llm.request_user_message())
        defaults = settings.Settings.model_validate(
            await get_config(ctx, "workflows.defaults", {}, model=settings.Settings)
        )
        conversation.set_settings(defaults.llm_overrides(model_tier))
        await conversation.run()
