"""Shared recorded MCP settings lookup."""

from vectorops.runner.api import composition
from vectorops.runner.api.configuration import get_config
from vectorops.runner.mcp import models


async def settings(component: str) -> models.Settings:
    return models.Settings.model_validate(
        await get_config(
            composition.workflow_context(),
            component,
            {},
            model=models.Settings,
            contract_digest=models.Settings.configuration_contract,
        )
    )


async def servers(*, component: str = "mcp") -> list[str]:
    resolved = await settings(component)
    return sorted(resolved.servers)
