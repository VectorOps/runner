"""Connection options for the remote command client."""

import argparse
import asyncio
from pathlib import Path
import typing

from vectorops.runner.control import client, commands, provisioning


class Options(argparse.Namespace):
    host: str
    port: int
    token_file: typing.Optional[Path]
    token_stdin: bool
    json: bool
    args: list[str]


def configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", choices=("127.0.0.1", "::1"), default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--json", action="store_true")
    tokens = parser.add_mutually_exclusive_group()
    tokens.add_argument("--token-file", type=Path)
    tokens.add_argument("--token-stdin", action="store_true")
    parser.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="workflow, execution, input, info, or shell; use 'help' for commands",
    )


async def dispatch(options: Options, config_dir: typing.Optional[Path]) -> None:
    args = commands.parser().parse_args(
        options.args or ["help"], namespace=commands.Arguments(json=options.json)
    )
    args.json = args.json or options.json
    if args.family == "help" or (
        args.family in ("workflow", "execution", "input") and args.action is None
    ):
        try:
            commands.parser().parse_args(
                ([args.family] if args.family != "help" else []) + ["--help"]
            )
        except SystemExit:
            return
    if not 1 <= options.port <= 65535:
        raise ValueError("invalid loopback port")
    if args.family == "shell" and options.token_stdin:
        raise ValueError(
            "shell requires token-file or configured authentication; stdin is reserved for commands"
        )
    token = None
    if options.token_stdin or options.token_file is not None:
        token = await provisioning.read_token(
            token_stdin=options.token_stdin, token_file=options.token_file
        )
    async with asyncio.timeout(15):
        connection = await client.RunnerClient.tcp(
            options.port, token, host=options.host, config_dir=config_dir
        )
    async with connection:
        if args.family == "shell":
            await commands.shell(connection, json_output=args.json)
        else:
            await commands.execute(connection, args)
