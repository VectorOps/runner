"""Workflow-facing tool factories and contracts, without resource initialization."""

from .llm import tools as _tools
from .llm import environment_tools as _environment
from .llm.patch import apply_patch_tool as apply_patch_tool
from .llm.subworkflows import (
    subagent_tool as subagent_tool,
    subworkflow_tool as subworkflow_tool,
)
from .llm.read_file import read_file_tool as read_file_tool
from .llm.plan import update_plan_tool as update_plan_tool
from .llm.web_fetch import web_fetch_tool as web_fetch_tool

exec_tool = _environment.exec_tool
poll_process_tool = _environment.poll_process_tool
write_stdin_tool = _environment.write_stdin_tool
kill_process_tool = _environment.kill_process_tool
list_processes_tool = _environment.list_processes_tool

Tool = _tools.Tool
ToolBinding = _tools.ToolBinding
WorkflowToolBinding = _tools.WorkflowToolBinding
ToolExecutionError = _tools.ToolExecutionError
ToolResponse = _tools.ToolResponse
ToolApproval = _tools.ToolApproval
ToolApprovalResponse = _tools.ToolApprovalResponse
invoke_tool = _tools.invoke_tool

__all__ = [
    "Tool",
    "ToolBinding",
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
