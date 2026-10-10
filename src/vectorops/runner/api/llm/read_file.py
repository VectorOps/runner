"""Compatibility exports for file tools."""

from vectorops.runner.api.builtin_tools import read_file as _implementation

ReadFileArguments = _implementation.ReadFileArguments
read_file_tool = _implementation.read_file_tool
