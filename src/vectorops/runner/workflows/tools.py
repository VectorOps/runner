"""Default named collections for standard workflows and compatibility helpers."""

from vectorops.runner.api import tools
from vectorops.runner.api.tools import list_files_tool as list_files_tool


def default_collections(
    agents: list[str], *, include_mcp: bool = True
) -> dict[str, tools.ToolCollection]:
    helper = ["read_file", "list_files", "exec", "poll_process"]
    agent = [
        *helper,
        "write_stdin",
        "kill_process",
        "list_processes",
        "apply_patch",
        "update_plan",
    ]
    if agents:
        agent.append("run_agent")
    if include_mcp:
        agent.append("mcp")
    return {
        "helper": tools.ToolCollection(tools=helper),
        "agent": tools.ToolCollection(
            tools=agent,
            config={"run_agent": {"references": list(agents)}} if agents else {},
        ),
    }


def helper_tools() -> list[tools.ToolBinding]:
    """No patching or delegation; exec remains governed by normal approval policy."""
    return [
        tools.read_file_tool(),
        list_files_tool(),
        tools.exec_tool(),
        tools.poll_process_tool(),
    ]


def agent_tools() -> list[tools.ToolBinding]:
    return [
        *helper_tools(),
        tools.write_stdin_tool(),
        tools.kill_process_tool(),
        tools.list_processes_tool(),
        tools.apply_patch_tool(),
        tools.update_plan_tool(),
    ]
