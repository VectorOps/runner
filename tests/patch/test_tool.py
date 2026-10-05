from pathlib import Path

import pydantic
import pytest

from vectorops.runner.api.llm import tools
from vectorops.runner.api.llm.patch import apply_patch_tool
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService


async def test_definition_and_validation(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    service = PatchService(environment)
    binding = apply_patch_tool(service, format="patch")
    assert binding.definition.name == "apply_patch"
    assert "entire affected file unchanged" in binding.definition.description
    binding.validate({"text": "patch"})
    invalid_arguments: list[dict[str, pydantic.JsonValue]] = [
        {"text": " "},
        {"text": "patch", "reverse": "yes"},
        {"text": "patch", "format": "patch"},
    ]
    for arguments in invalid_arguments:
        with pytest.raises(tools.ToolExecutionError):
            binding.validate(arguments)
    with pytest.raises(ValueError, match="Unsupported"):
        apply_patch_tool(service, format="unified")
