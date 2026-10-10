"""Non-interactive helper for narrowly scoped research and evidence collection."""

from typing import Annotated

import pydantic

from vectorops.runner.api import composition, llm, tools
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.workflows import context, prompts, settings
from vectorops.runner.workflows.tools import default_collections

Task = Annotated[str, pydantic.Field(min_length=1, max_length=65536, pattern=r"\S")]


@composition.workflow(
    name="Helper",
    description=(
        "Cost-efficient helper for bounded read-only research, summaries, comparisons, "
        "test planning, and error triage. Supply exact instructions and a deliverable. "
        "Validate its evidence and interpretations; do not delegate file changes."
    ),
    kind="builtin.helper",
)
async def helper(ctx: WorkflowExecutionContext, task: Task) -> pydantic.JsonValue:
    """Run one delegated task without interactive input or recursive delegation."""
    defaults = await settings.load_settings()
    options = await settings.load_helper_settings()
    prompt_context = await context.load_context()
    bindings = await tools.load_collection(
        options.tool_collection, defaults=default_collections([])
    )
    conversation = llm.LLM(
        "helper",
        options.llm_overrides(defaults),
        system_prompt=prompts.HELPER + "\n\n" + prompt_context,
    )
    conversation.add_user_message(task)
    response = await conversation.run(tools=bindings, max_rounds=options.max_rounds)
    return "\n".join(block.text for block in response.content if block.type == "text")
