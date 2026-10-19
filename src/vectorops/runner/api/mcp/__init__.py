"""Workflow-facing MCP APIs without implicit tool registration."""

from vectorops.runner.mcp.discovery import Discovery as Discovery, Ranking as Ranking
from vectorops.runner.mcp.transport import MCPError as MCPError

from . import content as _content, discovery as _discovery
from .configuration import servers as servers

Selection = _discovery.Selection
ToolSelectionConfig = _discovery.ToolSelectionConfig
configured_tools = _discovery.configured_tools
configured_tool_set = _discovery.configured_tool_set
discover = _discovery.discover
dynamic = _discovery.dynamic
tools = _discovery.tools
Content = _content.Content
discover_content = _content.discover_content
get_prompt = _content.get_prompt
read_resource = _content.read_resource
list_prompts = _content.list_prompts
list_resources = _content.list_resources

__all__ = [
    "Content",
    "Discovery",
    "MCPError",
    "Ranking",
    "Selection",
    "ToolSelectionConfig",
    "configured_tool_set",
    "configured_tools",
    "discover",
    "discover_content",
    "dynamic",
    "get_prompt",
    "list_prompts",
    "list_resources",
    "read_resource",
    "servers",
    "tools",
]
