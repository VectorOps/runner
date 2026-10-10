"""Compatibility exports for target-environment tools."""

from vectorops.runner.api.builtin_tools import environment_tools as _implementation

environment = _implementation.environment
process_response = _implementation.process_response
exec_tool = _implementation.exec_tool
poll_process_tool = _implementation.poll_process_tool
StdinArguments = _implementation.StdinArguments
write_stdin_tool = _implementation.write_stdin_tool
kill_process_tool = _implementation.kill_process_tool
EmptyArguments = _implementation.EmptyArguments
list_processes_tool = _implementation.list_processes_tool
