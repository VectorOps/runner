from vectorops.runner.api import builtin_tools, tool_registry

builtin_tools.register_builtins()
first = tool_registry.registrations()
builtin_tools.register_builtins()
assert first == tool_registry.registrations()
assert {
    "exec",
    "read_file",
    "apply_patch",
    "update_plan",
    "run_agent",
    "mcp",
} <= first.keys()
