"""Command-line access to persistent workspace executions."""

import argparse
import asyncio
import json
from pathlib import Path
import sys
import typing

import pydantic

from vectorops.runner.runner.workflow import WorkflowRunner


class Arguments(argparse.Namespace):
    project: Path
    command: str
    workflow: str
    callable: str
    input: dict[str, pydantic.JsonValue]
    execution_id: typing.Optional[str]
    output: Path


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
        "--project",
        type=Path,
        default=Path.cwd(),
        help="workspace directory (default: current directory)",
    )
    commands = result.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start", help="start a new workflow in the foreground")
    start.add_argument(
        "workflow", help="workflow file name relative to workflows/, without .py"
    )
    start.add_argument(
        "--callable", default="workflow", help="async callable name (default: workflow)"
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
    return result


async def dispatch(args: Arguments) -> None:
    runner = WorkflowRunner(args.project)
    if args.command == "start":
        result = await runner.run(
            args.workflow,
            args.callable,
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
