"""Agent process tools backed by the execution's attached target environment."""

import base64
import json
from typing import Optional

import connect
import pydantic

from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.api import tool_contracts as tools
from .environment import environment


def process_response(result: processes.ProcessResult) -> tools.ToolResponse:
    payload = result.model_dump(mode="json", exclude={"chunks"})
    chunks = []
    for chunk in result.chunks:
        try:
            data = chunk.data.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            data = base64.b64encode(chunk.data).decode("ascii")
            encoding = "base64"
        chunks.append({"stream": chunk.stream, "data": data, "encoding": encoding})
    payload["chunks"] = chunks
    return tools.ToolResponse(
        content=[connect.TextBlock(text=json.dumps(payload))],
        is_error=result.state in ("failed", "unknown")
        or result.error is not None
        or result.timed_out
        or result.returncode not in (None, 0),
    )


@register_tool("exec")
def exec_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[processes.ExecRequest]:
    async def execute(
        context: ActivityExecutionContext, arguments: processes.ExecRequest
    ) -> tools.ToolResponse:
        return process_response(await environment(context).exec(arguments))

    return tools.Tool(
        "exec",
        "Execute a command in the target environment. Provide exactly one of command.argv "
        "(an argument list for direct execution) or command.script (a shell script). "
        "command.kind is optional. Shell scripts require environment allow_shell. "
        "Returns bounded output and a process_id/cursor; poll running processes for more output. "
        "Cancellation of this call does not kill the process.",
        processes.ExecRequest,
        execute,
        approval=approval,
    )


@register_tool("poll_process")
def poll_process_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[processes.PollRequest]:
    async def execute(
        context: ActivityExecutionContext, arguments: processes.PollRequest
    ) -> tools.ToolResponse:
        return process_response(await environment(context).poll(arguments))

    return tools.Tool(
        "poll_process",
        "Collect process output after cursor; optionally wait for more output.",
        processes.PollRequest,
        execute,
        approval=approval,
    )


class StdinArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    process_id: str
    data: str = ""
    eof: bool = False


@register_tool("write_stdin")
def write_stdin_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[StdinArguments]:
    async def execute(
        context: ActivityExecutionContext, arguments: StdinArguments
    ) -> tools.ToolResponse:
        await environment(context).write_stdin(
            processes.StdinRequest(
                process_id=arguments.process_id,
                data=arguments.data.encode("utf-8"),
                eof=arguments.eof,
            )
        )
        return tools.ToolResponse(content=[connect.TextBlock(text="Input sent")])

    return tools.Tool(
        "write_stdin",
        "Send UTF-8 text to a process, optionally closing stdin with eof.",
        StdinArguments,
        execute,
        approval=approval,
    )


@register_tool("kill_process")
def kill_process_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[processes.SessionRequest]:
    async def execute(
        context: ActivityExecutionContext, arguments: processes.SessionRequest
    ) -> tools.ToolResponse:
        result = await environment(context).kill(arguments)
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.model_dump_json())]
        )

    return tools.Tool(
        "kill_process",
        "Terminate a process using its original termination scope.",
        processes.SessionRequest,
        execute,
        approval=approval,
    )


class EmptyArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")


@register_tool("list_processes")
def list_processes_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[EmptyArguments]:
    async def execute(
        context: ActivityExecutionContext, arguments: EmptyArguments
    ) -> tools.ToolResponse:
        results = await environment(context).list_processes()
        return tools.ToolResponse(
            content=[
                connect.TextBlock(
                    text=json.dumps(
                        [
                            result.model_dump(mode="json", exclude={"chunks"})
                            for result in results
                        ]
                    )
                )
            ]
        )

    return tools.Tool(
        "list_processes",
        "List retained process sessions in the target environment.",
        EmptyArguments,
        execute,
        approval=approval,
    )
