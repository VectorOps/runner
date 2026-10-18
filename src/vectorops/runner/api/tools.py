"""Workflow-facing tool factories and contracts, without resource initialization."""

from . import tool_contracts as _tools
from .tool_execution import invoke_tool as invoke_tool
from .builtin_tools import environment_tools as _environment
from .builtin_tools.patch import apply_patch_tool as apply_patch_tool
from .builtin_tools.subworkflows import (
    subagent_tool as subagent_tool,
    subworkflow_tool as subworkflow_tool,
)
from .builtin_tools.read_file import read_file_tool as read_file_tool
from .builtin_tools.plan import update_plan_tool as update_plan_tool
from .builtin_tools.web_fetch import web_fetch_tool as web_fetch_tool
from .builtin_tools.list_files import list_files_tool as list_files_tool
from .mcp import configured_tools as _configured_mcp_tools
from .tool_registry import ToolConfig as ToolConfig, register_tool as register_tool
from .tool_collections import (
    ToolCollection as ToolCollection,
    load_collection as load_collection,
    load_tool_set as load_tool_set,
)

exec_tool = _environment.exec_tool
poll_process_tool = _environment.poll_process_tool
write_stdin_tool = _environment.write_stdin_tool
kill_process_tool = _environment.kill_process_tool
list_processes_tool = _environment.list_processes_tool

Tool = _tools.Tool
ToolBinding = _tools.ToolBinding
ActivityToolBinding = _tools.ActivityToolBinding
ToolSet = _tools.ToolSet
WorkflowToolBinding = _tools.WorkflowToolBinding
ToolExecutionError = _tools.ToolExecutionError
ToolResponse = _tools.ToolResponse
ToolApproval = _tools.ToolApproval
ToolApprovalResponse = _tools.ToolApprovalResponse
AuthContext = _tools.AuthContext

__all__ = [
    "AuthContext",
    "ToolConfig",
    "register_tool",
    "ToolCollection",
    "load_collection",
    "load_tool_set",
    "list_files_tool",
    "Tool",
    "ToolBinding",
    "ToolSet",
    "WorkflowToolBinding",
    "ToolExecutionError",
    "ToolResponse",
    "ToolApproval",
    "ToolApprovalResponse",
    "invoke_tool",
    "apply_patch_tool",
    "subworkflow_tool",
    "subagent_tool",
    "exec_tool",
    "poll_process_tool",
    "write_stdin_tool",
    "kill_process_tool",
    "list_processes_tool",
    "read_file_tool",
    "update_plan_tool",
    "web_fetch_tool",
]
