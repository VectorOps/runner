"""Command-line access to persistent workspace executions."""

import argparse
import asyncio
import json
from pathlib import Path
import sys
import typing

import pydantic

from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner import global_config
from vectorops.runner.authentication import models as am
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import daemon, provisioning
from vectorops.runner.control import cli as client_cli


class Arguments(argparse.Namespace):
    project: Path
    workspace_directory: typing.Optional[str]
    command: str
    workflow: str
    input: dict[str, pydantic.JsonValue]
    execution_id: typing.Optional[str]
    output: Path
    daemon_command: str
    port: int
    mode: typing.Literal["cancel", "drain"]
    host: str
    token_stdin: bool
    token_file: typing.Optional[Path]
    config_dir: typing.Optional[Path]


def _input(value: str) -> dict[str, pydantic.JsonValue]:
    try:
        parsed = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
            value
        )
        json.dumps(parsed, allow_nan=False)
        return parsed
    except ValueError as exc:
        raise argparse.ArgumentTypeError("input must be a finite JSON object") from exc


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="vops-runner", description=__doc__)
    result.add_argument(
        "--config-dir", type=Path, help="runner-global configuration directory"
    )
    result.add_argument(
        "--project",
        type=Path,
        default=Path.cwd(),
        help="project directory (default: current directory)",
    )
    result.add_argument(
        "--workspace-directory",
        help="relative workspace subdirectory (default: VOPS_RUNNER_WORKSPACE_DIRECTORY or .voc); empty uses the project directly",
    )
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "credentials-import",
        help="import one credential JSON object from stdin without echoing secrets",
    )
    start = commands.add_parser("start", help="start a new workflow in the foreground")
    start.add_argument(
        "workflow",
        help="workflow reference: builtin:name, path:callable, or python:module:callable",
    )
    start.add_argument(
        "--input",
        type=_input,
        default={},
        help="workflow keyword arguments as a JSON object",
    )
    start.add_argument(
        "--execution-id", help="identity for the new execution (default: generated)"
    )
    for name, help_text in (
        ("resume", "resume an execution using persisted configuration and heads"),
        ("status", "show persisted status and statistics for all retained steps"),
        ("export", "export the entire execution as JSONL"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument(
            "execution_id", nargs="?", help="execution ID (default: last execution)"
        )
        if name == "export":
            command.add_argument(
                "--output",
                required=True,
                type=Path,
                help="new JSONL file; existing files are not overwritten",
            )
    client_parser = commands.add_parser(
        "client", help="run commands against an existing runner"
    )
    client_cli.configure(client_parser)
    daemon_parser = commands.add_parser(
        "daemon", help="control a workspace runner daemon"
    )
    daemon_commands = daemon_parser.add_subparsers(dest="daemon_command", required=True)
    serve = daemon_commands.add_parser(
        "serve", help="serve authenticated loopback TCP in the foreground"
    )
    serve.add_argument(
        "--port", type=int, default=0, help="loopback port (default: OS-assigned)"
    )
    status = daemon_commands.add_parser(
        "status", help="print authenticated daemon status as JSON"
    )
    stop = daemon_commands.add_parser("stop", help="shut down the workspace daemon")
    stop.add_argument("--mode", choices=("cancel", "drain"), default="cancel")
    connect = daemon_commands.add_parser(
        "connect", help="attach and print live events as JSONL"
    )
    for command in (serve, status, stop, connect):
        tokens = command.add_mutually_exclusive_group()
        tokens.add_argument(
            "--token-stdin",
            action="store_true",
            help="read one bearer token line from piped stdin",
        )
        tokens.add_argument(
            "--token-file",
            type=Path,
            help="read bearer token from a harness-protected file",
        )
        if command is not serve:
            command.add_argument(
                "--port",
                type=int,
                required=True,
                help="daemon or SSH-forwarded loopback port",
            )
            command.add_argument(
                "--host", choices=("127.0.0.1", "::1"), default="127.0.0.1"
            )
    daemon_parser.set_defaults(port=0, mode="cancel", host="127.0.0.1")
    return result


async def dispatch(args: Arguments) -> None:
    if args.command == "client":
        await client_cli.dispatch(client_cli.Options(**vars(args)), args.config_dir)
        return
    configuration = global_config.GlobalConfiguration(args.config_dir)
    if args.command == "credentials-import":
        data = await asyncio.to_thread(sys.stdin.buffer.read, 262145)
        try:
            if len(data) > 262144:
                raise ValueError()
            credential = am.Credential.model_validate_json(data)
        except ValueError:
            raise ValueError("invalid credential input") from None
        await AuthenticationService(configuration).provision(credential)
        print(json.dumps({"status": "stored"}))
        return
    if args.command == "daemon":
        if not (0 if args.daemon_command == "serve" else 1) <= args.port <= 65535:
            raise ValueError("invalid loopback port")
        token = None
        if args.token_stdin or args.token_file is not None:
            token = await provisioning.read_token(
                token_stdin=args.token_stdin, token_file=args.token_file
            )
        await daemon.dispatch(
            args.project,
            args.daemon_command,
            workspace_directory=args.workspace_directory,
            token=token,
            config_dir=args.config_dir,
            port=args.port,
            host=args.host,
            mode=args.mode,
        )
        return
    runner = WorkflowRunner(
        args.project,
        authentication=AuthenticationService(configuration),
        workspace_directory=args.workspace_directory,
    )
    if args.command == "start":
        result = await runner.run(
            args.workflow,
            input=args.input,
            new_execution_id=args.execution_id,
        )
        print(
            json.dumps(
                {"execution_id": runner.last_execution_id, "result": result},
                allow_nan=False,
            )
        )
    elif args.command == "resume":
        result = await runner.resume(args.execution_id)
        print(
            json.dumps(
                {"execution_id": runner.last_execution_id, "result": result},
                allow_nan=False,
            )
        )
    elif args.command == "status":
        print((await runner.inspect_execution(args.execution_id)).model_dump_json())
    elif args.command == "export":
        await runner.export_execution(args.output, args.execution_id)
        print(json.dumps({"output": str(args.output)}))


def main(argv: typing.Optional[list[str]] = None) -> int:
    args = parser().parse_args(argv, namespace=Arguments())
    try:
        with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
            runner.run(dispatch(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"vops-runner: {exc}", file=sys.stderr)
        return 1
    return 0
