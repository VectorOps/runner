import json
import sys

match sys.argv[1]:
    case "llm":
        from vectorops.runner.api import llm
    case "environment":
        from vectorops.runner.api.builtin_tools.environment import environment
    case "read_file":
        from vectorops.runner.api.llm import read_file
    case "subagent_tool":
        from vectorops.runner.api.llm import subagent_tool
    case _:
        raise ValueError(sys.argv[1])

from vectorops.runner.api import tool_registry

print(json.dumps(sorted(tool_registry.registrations())))
