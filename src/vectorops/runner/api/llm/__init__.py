"""Replay-aware LLM conversations and typed tool bindings."""

from typing import TYPE_CHECKING

from .api import LLM as LLM
from . import models
from .choice import ChoiceResult as ChoiceResult, ChoiceLimitError as ChoiceLimitError
from .configuration import resolve_settings as resolve_settings
from .provider import ConnectProvider as ConnectProvider, Provider as Provider
from . import tools
from . import input

if TYPE_CHECKING:
    from vectorops.runner.api.builtin_tools.subworkflows import (
        subagent_tool as subagent_tool,
        subworkflow_tool as subworkflow_tool,
    )


def __getattr__(name: str) -> object:
    if name in ("subagent_tool", "subworkflow_tool"):
        from vectorops.runner.api.builtin_tools import subworkflows

        return {
            "subagent_tool": subworkflows.subagent_tool,
            "subworkflow_tool": subworkflows.subworkflow_tool,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


Compaction = models.Compaction
Settings = models.Settings
SettingsOverrides = models.SettingsOverrides
RetryPolicy = models.RetryPolicy

Tool = tools.Tool
ToolBinding = tools.ToolBinding
ActivityToolBinding = tools.ActivityToolBinding
ToolSet = tools.ToolSet
WorkflowToolBinding = tools.WorkflowToolBinding
ToolExecutionError = tools.ToolExecutionError
ToolResponse = tools.ToolResponse
ToolApproval = tools.ToolApproval
ToolApprovalResponse = tools.ToolApprovalResponse
ToolApprovalRequest = tools.ToolApprovalRequest
invoke_tool = tools.invoke_tool

UserMessageInput = input.UserMessageInput
UserMessageRequest = input.UserMessageRequest
request_user_message = input.request_user_message
