"""Compatibility exports for patch tools."""

from vectorops.runner.api.builtin_tools import patch as _implementation

PatchArguments = _implementation.PatchArguments
PatchConfig = _implementation.PatchConfig
apply_patch_tool = _implementation.apply_patch_tool
configured_patch = _implementation.configured_patch
