"""Persistent user-input and LLM conversation loop."""

import pydantic

from vectorops.runner.api import composition, llm
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
    """Snapshot shared defaults, then alternate durable input and LLM turns."""
    defaults = await settings.load_settings()
    conversation = llm.LLM(
        "chat",
        defaults.llm_overrides(model_tier),
        system_prompt="You are a helpful assistant. Be clear, accurate, and concise.",
    )
    while True:
        conversation.add_message(await llm.request_user_message())
        await conversation.run()
