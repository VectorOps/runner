"""Explicit registration adapter for configured MCP tools."""

from vectorops.runner.api import mcp
from vectorops.runner.api.tool_contracts import ToolBinding
from vectorops.runner.api.tool_registry import register_tool


@register_tool("mcp", config=mcp.ToolSelectionConfig, tool_set=mcp.configured_tool_set)
async def configured_tools(config: mcp.ToolSelectionConfig) -> list[ToolBinding]:
    return await mcp.configured_tools(config)
