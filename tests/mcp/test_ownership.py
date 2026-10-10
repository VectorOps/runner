from unittest.mock import AsyncMock, patch

import pytest

from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import content, models
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.mcp.transport import MCPError


def test_content_selection_is_owned_and_detached() -> None:
    catalog = content.ContentCatalog(
        server_name="test",
        server=models.StdioServer(command="unused"),
        prompts=[content.Prompt(name="one"), content.Prompt(name="two")],
        resources=[content.Resource(uri="file:///one", name="one")],
    )
    selected = catalog.select(prompts=["one"], resources=["*"])
    assert [item.name for item in selected.prompts] == ["one"]
    assert selected.resources == catalog.resources
    selected.prompts.clear()
    assert len(catalog.prompts) == 2
    assert not catalog.select().prompts
    assert not catalog.select().resources


async def test_current_discovery_resolves_overrides_inside_runtime() -> None:
    pool = SharedPool(AuthenticationService())
    runtime = MCPRuntime(pool=pool)
    base = models.StdioServer(command="unused")
    effective = base.model_copy(update={"roots": [models.Root(uri="file:///updated")]})
    catalog = models.Catalog(
        server_name="test", server=effective, protocol_version="test", tools=[]
    )
    contents = content.ContentCatalog(server_name="test", server=effective)
    try:
        with (
            patch.object(pool, "effective", return_value=effective) as resolve,
            patch.object(
                runtime, "discover", new_callable=AsyncMock, return_value=catalog
            ) as discover,
            patch.object(
                runtime,
                "discover_content",
                new_callable=AsyncMock,
                return_value=contents,
            ) as discover_content,
        ):
            assert await runtime.discover_current("test", base) == catalog
            discover.assert_awaited_once_with("test", effective)
            assert await runtime.discover_current_content("test", base) == contents
            discover_content.assert_awaited_once_with("test", effective)
            assert resolve.call_count == 2
    finally:
        await runtime.close()
        await pool.close()


async def test_recorded_discovery_does_not_apply_current_overrides() -> None:
    pool = SharedPool(AuthenticationService())
    runtime = MCPRuntime(pool=pool)
    pinned = models.StdioServer(command="unused")
    try:
        with (
            patch.object(
                pool, "effective", side_effect=AssertionError("rebind recorded catalog")
            ),
            patch.object(
                runtime, "_connection", side_effect=RuntimeError("stop before I/O")
            ),
        ):
            with pytest.raises(MCPError, match="discovery failed"):
                await runtime.discover("test", pinned)
    finally:
        await runtime.close()
        await pool.close()
