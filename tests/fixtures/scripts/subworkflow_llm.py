from collections.abc import AsyncGenerator

import connect
import pydantic

from vectorops.runner.api import llm
from vectorops.runner.api.composition import final
from vectorops.runner.api.llm.models import Settings
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem


class Provider:
    def __init__(self) -> None:
        self.calls = 0

    async def stream(
        self,
        ctx: ActivityExecutionContext,
        settings: Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        self.calls += 1
        content: list[
            connect.TextBlock | connect.ReasoningBlock | connect.ToolCallBlock
        ]
        if self.calls == 1:
            content = [
                connect.ToolCallBlock(
                    id=str(value),
                    name="subworkflow",
                    arguments={"reference": "child:double", "input": {"value": value}},
                )
                for value in (3, 4)
            ]
        else:
            content = [connect.TextBlock(text="done")]
        yield final(
            connect.AssistantMessage(
                content=content,
                finish_reason="tool_call" if self.calls == 1 else "stop",
            ).model_dump(mode="json")
        )


async def main(ctx: object) -> pydantic.JsonValue:
    tool = await llm.subworkflow_tool(["child:double"])
    conversation = llm.LLM("agent", model="test", backend=Provider())
    conversation.add_user_message("Double 3 and 4")
    answer = await conversation.run(tools=[tool])
    return answer.model_dump(mode="json")
