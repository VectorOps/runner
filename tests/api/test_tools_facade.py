from pathlib import Path

import pytest

from vectorops.runner.api import llm, tools
from vectorops.runner.api.llm import patch, subworkflows
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService


def test_exports_preserve_implementation_identity() -> None:
    expected = {
        "Tool": llm.Tool,
        "ToolBinding": llm.ToolBinding,
        "WorkflowToolBinding": llm.WorkflowToolBinding,
        "ToolExecutionError": llm.ToolExecutionError,
        "ToolResponse": llm.ToolResponse,
        "ToolApproval": llm.ToolApproval,
        "ToolApprovalResponse": llm.ToolApprovalResponse,
        "invoke_tool": llm.invoke_tool,
        "apply_patch_tool": patch.apply_patch_tool,
        "subworkflow_tool": subworkflows.subworkflow_tool,
    }
    assert set(tools.__all__) == set(expected)
    for name, implementation in expected.items():
        assert vars(tools)[name] is implementation
    assert tools.subworkflow_tool is llm.subworkflow_tool


async def test_patch_factory_through_facade(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        approval = tools.ToolApproval()
        binding: tools.ToolBinding = tools.apply_patch_tool(
            PatchService(environment), approval=approval
        )
        assert binding.definition.name == "apply_patch"
        assert binding.approval is approval
        binding.validate({"text": "*** Begin Patch\n*** End Patch"})
        with pytest.raises(tools.ToolExecutionError):
            binding.validate({"text": " "})
        assert list(tmp_path.iterdir()) == []
    finally:
        await environment.close()
