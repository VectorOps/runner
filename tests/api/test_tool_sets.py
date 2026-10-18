from pathlib import Path
from unittest.mock import patch

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from tests.mcp.test_discovery import catalog, search
from vectorops.runner.api import mcp, tools, tool_sets
from vectorops.runner.mcp import models
from vectorops.runner.mcp.discovery import Discovery
from vectorops.runner.mcp.runtime import MCPRuntime


async def test_combined_tool_sets_keep_static_tools_and_advance_independently() -> None:
    first, second = catalog("alpha"), catalog("beta")
    static = tool_sets.Static((tools.read_file_tool(),))
    selected = tool_sets.Combined((static, first, second))
    advanced = selected.advance([await search(first, "alpha")])
    assert len(selected.bindings) == 3
    assert [binding.definition.name for binding in advanced.bindings] == [
        "read_file",
        first.bindings[0].definition.name,
        "alpha",
        second.bindings[0].definition.name,
    ]
    both = advanced.advance([await search(second, "beta")])
    assert len(both.bindings) == 5
    assert static.advance([]) is static


async def test_combined_tool_sets_reject_collisions_after_activation() -> None:
    selected = catalog("read_file")
    combined = tool_sets.Combined(
        (tool_sets.Static((tools.read_file_tool(),)), selected)
    )
    assert len(combined.bindings) == 2
    with pytest.raises(ValueError, match="duplicate"):
        combined.advance([await search(selected, "read_file")]).bindings


@pytest.mark.parametrize(
    "config",
    [
        "",
        '[mcp.servers.disabled]\nkind = "stdio"\ncommand = "unused"\nenabled = false\n',
        '[mcp.servers.empty]\nkind = "stdio"\ncommand = "unused"\ntools = []\n',
        '[mcp.servers.excluded]\nkind = "stdio"\ncommand = "unused"\nexclude = ["*"]\n',
    ],
)
async def test_empty_mcp_collection_is_a_noop(
    tmp_path: Path, history: MemoryHistory, config: str
) -> None:
    (tmp_path / "config.toml").write_text(config)

    async def workflow() -> pydantic.JsonValue:
        selected = await tools.load_tool_set(
            "agent",
            defaults={"agent": tools.ToolCollection(tools=["read_file", "mcp"])},
        )
        assert [binding.definition.name for binding in selected.bindings] == [
            "read_file"
        ]
        return None

    with patch.object(
        MCPRuntime, "discover_current", side_effect=AssertionError("MCP I/O")
    ):
        first = await replay.run(history, workflow, workspace=tmp_path)
        assert first.outcome.kind == "completed"
        assert (
            await replay.run(history, workflow, first.id, workspace=tmp_path)
        ).outcome == first.outcome


async def test_dynamic_loader_validates_duplicates(
    tmp_path: Path, history: MemoryHistory
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.tools.collections.invalid]\ntools = ["a", "b"]\n'
        'config.a.factory = "read_file"\nconfig.b.factory = "read_file"\n'
    )

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ValueError, match="duplicate"):
            await tools.load_tool_set("invalid")
        return None

    assert (
        await replay.run(history, workflow, workspace=tmp_path)
    ).outcome.kind == "completed"


@pytest.mark.parametrize("mode", ["static", "dynamic"])
async def test_configured_large_catalog_and_activation_limit(mode: str) -> None:
    settings = models.Settings.model_validate(
        {
            "mode": mode,
            "max_active": 2,
            "servers": {"large": {"kind": "stdio", "command": "unused"}},
        }
    )
    connection = settings.server("large")
    snapshot = models.Catalog(
        server_name="large",
        server=connection,
        protocol_version="2025-03-26",
        tools=[
            models.Descriptor(
                remote_name=f"tool_{index}",
                name=models.tool_name("large", connection, f"tool_{index}"),
                input_schema={"type": "object"},
            )
            for index in range(150)
        ],
    )
    with (
        patch.object(mcp, "_settings", return_value=settings),
        patch.object(mcp, "_discover", return_value=snapshot),
    ):
        selected = await mcp.configured_tool_set(mcp.ToolSelectionConfig())
        assert len(selected.bindings) == (150 if mode == "static" else 1)
        if mode == "dynamic":
            assert isinstance(selected, Discovery)
            activated = selected.advance([await search(selected, "")])
            assert len(activated.bindings) == 3
        assert len(await mcp.configured_tools(mcp.ToolSelectionConfig())) == 150


async def test_explicit_selection_cannot_bypass_server_filters() -> None:
    settings = models.Settings.model_validate(
        {
            "servers": {
                "disabled": {"kind": "stdio", "command": "unused", "enabled": False},
            }
        }
    )
    with (
        patch.object(mcp, "_settings", return_value=settings),
        patch.object(
            mcp, "_discover", side_effect=AssertionError("disabled discovery")
        ),
    ):
        selected = await mcp.configured_tool_set(
            mcp.ToolSelectionConfig(selections=[mcp.Selection(server="disabled")])
        )
        assert not selected.bindings
