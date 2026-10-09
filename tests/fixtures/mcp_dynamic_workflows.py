"""Scripted LLM rounds exercising MCP search, activation, and concrete approval."""

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Optional

import connect
import pydantic

from vectorops.runner.api import composition, llm, mcp
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.streaming import StreamItem


@dataclass
class Probe:
    requests: list[connect.GenerateRequest] = field(default_factory=list)
    activated: asyncio.Event = field(default_factory=asyncio.Event)
    release: Optional[asyncio.Event] = None


probe = Probe()


class Provider:
    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: llm.Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        probe.requests.append(request.model_copy(deep=True))
        results = [item for item in request.messages if item.role == "tool"]
        if not results:
            assert len(request.tools) == 1
            response = connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="search",
                        name=request.tools[0].name,
                        arguments={"query": "echo"},
                    )
                ],
                finish_reason="tool_call",
            )
        elif len(results) == 1 and len(request.tools) == 2:
            probe.activated.set()
            if probe.release is not None:
                await probe.release.wait()
            response = connect.AssistantMessage(
                content=[
                    connect.ToolCallBlock(
                        id="echo",
                        name=request.tools[1].name,
                        arguments={"value": "dynamic"},
                    )
                ],
                finish_reason="tool_call",
            )
        else:
            response = connect.AssistantMessage(
                content=[connect.TextBlock(text="done")], finish_reason="stop"
            )
        yield composition.final(response.model_dump(mode="json"))


async def workflow(
    ctx: WorkflowExecutionContext, excluded: bool = False
) -> pydantic.JsonValue:
    selected = await mcp.dynamic(
        [mcp.Selection(server="test", exclude=["echo"] if excluded else [])]
    )
    agent = llm.LLM("dynamic", model="test", backend=Provider())
    agent.add_user_message("Find and use echo")
    result = await agent.run(tool_set=selected)
    assert len(selected.bindings) == 1
    return result.model_dump(mode="json")
