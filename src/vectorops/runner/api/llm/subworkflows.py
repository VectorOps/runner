"""Compatibility exports for child-workflow tool adapters."""

from vectorops.runner.api.builtin_tools import subworkflows as _implementation

SubworkflowArguments = _implementation.SubworkflowArguments
SubworkflowTool = _implementation.SubworkflowTool
SubworkflowConfig = _implementation.SubworkflowConfig
subworkflow_tool = _implementation.subworkflow_tool
subagent_tool = _implementation.subagent_tool
configured_subagent = _implementation.configured_subagent
configured_subworkflow = _implementation.configured_subworkflow
