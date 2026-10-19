from pathlib import Path

import pytest

from vectorops.runner.api import llm, tools
from vectorops.runner.api import tool_collections, tool_contracts, tool_registry
from vectorops.runner.api.builtin_tools import worktrees
import vectorops.runner.api.llm.list_files as list_files
import vectorops.runner.api.llm.environment_tools as environment_tools
import vectorops.runner.api.llm.patch as patch
import vectorops.runner.api.llm.plan as plan
import vectorops.runner.api.llm.read_file as read_file
import vectorops.runner.api.llm.subworkflows as subworkflows
import vectorops.runner.api.llm.web_fetch as web_fetch
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService


def test_exports_preserve_implementation_identity() -> None:
    expected = {
        "AuthContext": tool_contracts.AuthContext,
        "ToolConfig": tool_registry.ToolConfig,
        "register_tool": tool_registry.register_tool,
        "ToolCollection": tool_collections.ToolCollection,
        "load_collection": tool_collections.load_collection,
        "load_tool_set": tool_collections.load_tool_set,
        "list_files_tool": list_files.list_files_tool,
        "Tool": llm.Tool,
        "ToolBinding": llm.ToolBinding,
        "ToolSet": llm.ToolSet,
        "WorkflowToolBinding": llm.WorkflowToolBinding,
        "ToolExecutionError": llm.ToolExecutionError,
        "ToolResponse": llm.ToolResponse,
        "ToolApproval": llm.ToolApproval,
        "ToolApprovalResponse": llm.ToolApprovalResponse,
        "invoke_tool": llm.invoke_tool,
        "apply_patch_tool": patch.apply_patch_tool,
        "subworkflow_tool": subworkflows.subworkflow_tool,
        "subagent_tool": subworkflows.subagent_tool,
        "exec_tool": environment_tools.exec_tool,
        "poll_process_tool": environment_tools.poll_process_tool,
        "write_stdin_tool": environment_tools.write_stdin_tool,
        "kill_process_tool": environment_tools.kill_process_tool,
        "list_processes_tool": environment_tools.list_processes_tool,
        "read_file_tool": read_file.read_file_tool,
        "update_plan_tool": plan.update_plan_tool,
        "web_fetch_tool": web_fetch.web_fetch_tool,
        "worktree_create_tool": worktrees.worktree_create_tool,
        "worktree_list_tool": worktrees.worktree_list_tool,
        "worktree_delete_tool": worktrees.worktree_delete_tool,
    }
    assert set(tools.__all__) == set(expected)
    for name, implementation in expected.items():
        assert vars(tools)[name] is implementation
    assert tools.subworkflow_tool is llm.subworkflow_tool
    assert tools.subagent_tool is llm.subagent_tool


async def test_patch_factory_through_facade(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    try:
        approval = tools.ToolApproval()
        binding: tools.ToolBinding = tools.apply_patch_tool(
            PatchService(environment.root), approval=approval
        )
        assert binding.definition.name == "apply_patch"
        assert binding.approval is approval
        binding.validate({"text": "*** Begin Patch\n*** End Patch"})
        with pytest.raises(tools.ToolExecutionError):
            binding.validate({"text": " "})
        assert list(tmp_path.iterdir()) == []
    finally:
        await environment.processes.close()
