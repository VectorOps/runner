import subprocess
import sys

import pytest

from vectorops.runner.api import mcp
from vectorops.runner.api.mcp import configuration, content, discovery


@pytest.mark.parametrize(
    "module",
    [
        "vectorops.runner.api.mcp",
        "vectorops.runner.api.mcp.configuration",
        "vectorops.runner.api.mcp.content",
        "vectorops.runner.api.mcp.discovery",
    ],
)
def test_api_import_does_not_register_tools(module: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import importlib; importlib.import_module({module!r}); "
            "from vectorops.runner.api import tool_registry; "
            "assert not tool_registry.registrations(); "
            "from vectorops.runner.api import builtin_tools; "
            "builtin_tools.register_builtins(); "
            "first = tool_registry.registrations(); "
            "assert first['mcp'].module == 'vectorops.runner.api.builtin_tools.mcp'; "
            "builtin_tools.register_builtins(); "
            "assert first == tool_registry.registrations()",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_facade_preserves_implementation_identity() -> None:
    assert mcp.discover is discovery.discover
    assert mcp.dynamic is discovery.dynamic
    assert mcp.tools is discovery.tools
    assert mcp.configured_tools is discovery.configured_tools
    assert mcp.configured_tool_set is discovery.configured_tool_set
    assert mcp.ToolSelectionConfig is discovery.ToolSelectionConfig
    assert mcp.Selection is discovery.Selection
    assert mcp.Content is content.Content
    assert mcp.discover_content is content.discover_content
    assert mcp.get_prompt is content.get_prompt
    assert mcp.read_resource is content.read_resource
    assert mcp.list_prompts is content.list_prompts
    assert mcp.list_resources is content.list_resources
    assert mcp.servers is configuration.servers
