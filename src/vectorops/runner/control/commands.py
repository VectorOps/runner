"""Shared one-shot and interactive commands for an existing runner connection."""

import argparse
import asyncio
import json
from pathlib import Path
import shlex
import sys
from threading import Thread
import typing
from uuid import uuid4

import pydantic

from vectorops.runner.control import client, protocol as p, oauth_host
from vectorops.runner.runner import mailbox


class Arguments(argparse.Namespace):
    family: str
    action: typing.Optional[str] = None
    json: bool = False
    reference: str
    execution_id: str
    request_id: str
    after: typing.Optional[str]
    limit: int
    active: bool
    input: typing.Optional[str]
    input_file: typing.Optional[Path]
    text: typing.Optional[str]
    text_file: typing.Optional[Path]
    value: typing.Optional[str]
    json_file: typing.Optional[Path]
    idempotency_key: typing.Optional[str]
    workflow_id: typing.Optional[str]
    channel: typing.Optional[str]
    profile_action: typing.Optional[str] = None
    profile_name: str
    provider: str
    profile: typing.Optional[str]
    no_browser: bool


class CommandError(ValueError):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> typing.NoReturn:
        raise CommandError(f"{message}\n{self.format_usage().strip()}")


def parser() -> Parser:
    result = Parser(prog="client")
    result.add_argument(
        "--json", action="store_true", help="print machine-readable JSON"
    )
    families = result.add_subparsers(dest="family", required=True)
    families.add_parser("shell", help="open a connected command shell")
    families.add_parser("info", help="show runner information")
    families.add_parser("help", help="show commands")
    families.add_parser("exit", help="disconnect without stopping executions")
    auth = families.add_parser(
        "auth", help="manage runner-global authentication profiles"
    )
    auth_actions = auth.add_subparsers(dest="action")
    login = auth_actions.add_parser(
        "login", help="authenticate before starting workflows"
    )
    login.add_argument("provider")
    login.add_argument("--profile", help="credential profile (default: active profile)")
    login.add_argument(
        "--no-browser",
        action="store_true",
        help="create a login request for a separate host",
    )
    login.add_argument("--json", action="store_true", default=argparse.SUPPRESS)
    profiles = auth_actions.add_parser("profile", help="manage named profiles")
    profile_actions = profiles.add_subparsers(dest="profile_action")
    for name in ("list", "add", "switch", "delete"):
        action_parser = profile_actions.add_parser(name)
        action_parser.add_argument(
            "--json", action="store_true", default=argparse.SUPPRESS
        )
        if name != "list":
            action_parser.add_argument("profile_name")
    for family, actions in (
        ("workflow", ("list", "show")),
        ("execution", ("list", "show", "start", "stop", "resume", "delete", "watch")),
        ("input", ("list", "show", "send")),
    ):
        group = families.add_parser(family)
        commands = group.add_subparsers(dest="action")
        for action in actions:
            command = commands.add_parser(
                action,
                help=(
                    "permanently delete a non-running execution, including paused work"
                    if action == "delete"
                    else None
                ),
            )
            command.add_argument(
                "--json", action="store_true", default=argparse.SUPPRESS
            )
            if action == "list":
                command.add_argument("--after")
                command.add_argument("--limit", type=int, default=100)
                if family == "execution":
                    command.add_argument("--active", action="store_true")
                if family == "input":
                    command.add_argument("execution_id")
                    command.add_argument("--workflow-id")
                    command.add_argument("--channel")
            elif family == "workflow":
                command.add_argument("reference")
            elif family == "execution" and action == "start":
                command.add_argument("reference")
                source = command.add_mutually_exclusive_group()
                source.add_argument(
                    "--input", help="workflow arguments as a JSON object"
                )
                source.add_argument("--input-file", type=Path)
            else:
                command.add_argument("execution_id")
                if family == "input":
                    command.add_argument("request_id")
            if action in ("start", "resume", "send"):
                command.add_argument("--idempotency-key")
            if action == "send":
                source = command.add_mutually_exclusive_group(required=True)
                source.add_argument("--text")
                source.add_argument("--text-file", type=Path)
                source.add_argument(
                    "--value", help="response as JSON, including scalar values"
                )
                source.add_argument("--json-file", type=Path)
    return result


def _json(text: str) -> pydantic.JsonValue:
    value = p.JSON.validate_json(text)
    json.dumps(value, allow_nan=False)
    return value


async def _read(path: Path) -> str:
    def read() -> str:
        with path.open("rb") as stream:
            data = stream.read(p.MAX_FRAME + 1)
        if len(data) > p.MAX_FRAME:
            raise ValueError("input file exceeds the control frame size")
        return data.decode("utf-8")

    return await asyncio.to_thread(read)


def _emit(value: pydantic.BaseModel, args: Arguments, lines: list[str]) -> None:
    if args.json:
        print(value.model_dump_json())
    else:
        print("\n".join(lines))


def _more(after: typing.Optional[str]) -> list[str]:
    return [f"More results: use --after {after}"] if after is not None else []


def _compact(value: object, limit: int = 120) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _key(args: Arguments) -> str:
    key = args.idempotency_key if args.idempotency_key is not None else uuid4().hex
    print(f"Idempotency key: {key}", file=sys.stderr, flush=True)
    return key


async def execute(connection: client.RunnerClient, args: Arguments) -> None:
    """Dispatch typed client operations without replaying mutations on transport failure."""
    family, action = args.family, args.action
    if family == "auth":
        if action == "login":
            requirement = await connection.login(args.provider, args.profile)
            if args.no_browser:
                _emit(
                    requirement,
                    args,
                    [
                        f"Login requested: {requirement.id} ({requirement.provider}/{requirement.profile})."
                    ],
                )
            else:
                print(
                    f"Authenticating {requirement.provider}/{requirement.profile} in your browser...",
                    file=sys.stderr,
                    flush=True,
                )
                await oauth_host.authenticate(connection, requirement.id)
                if args.json:
                    print(
                        json.dumps(
                            {
                                "status": "authenticated",
                                "provider": requirement.provider,
                                "profile": requirement.profile,
                            }
                        )
                    )
                else:
                    print(
                        f"Authenticated {requirement.provider}/{requirement.profile}."
                    )
            return
        if action is None or args.profile_action is None:
            try:
                parser().parse_args(["auth", *([action] if action else []), "--help"])
            except SystemExit:
                pass
            return
        if args.profile_action == "list":
            listing = await connection.authentication_profiles()
            _emit(
                listing,
                args,
                [
                    "Authentication profiles:",
                    *(
                        f"  {name}"
                        + (" (active)" if name == listing.active_profile else "")
                        for name in listing.profiles
                    ),
                ],
            )
            return
        operations = {
            "add": connection.add_authentication_profile,
            "switch": connection.switch_authentication_profile,
            "delete": connection.delete_authentication_profile,
        }
        result = await operations[args.profile_action](args.profile_name)
        descriptions = {"add": "Added", "switch": "Switched to", "delete": "Deleted"}
        message = (
            f"{descriptions[args.profile_action]} authentication profile '{args.profile_name}'."
            if result.changed
            else f"No change to authentication profile '{args.profile_name}'."
        )
        _emit(result, args, [message, f"Active profile: {result.active_profile}"])
        return
    if family == "info":
        print(json.dumps(await connection.info(), indent=None if args.json else 2))
        return
    if family == "help" or action is None:
        if family in ("workflow", "execution", "input"):
            try:
                parser().parse_args([family, "--help"])
            except SystemExit:
                pass
        else:
            parser().print_help()
            print("Use workflow, execution, input, or auth with --help for commands.")
        return
    if family == "workflow":
        if action == "show":
            descriptor = await connection.describe_workflow(args.reference)
            print(descriptor.model_dump_json(indent=None if args.json else 2))
        else:
            page = await connection.workflows(
                p.ExecutionList(after=args.after, limit=args.limit)
            )
            lines = (
                ["Reference  Description"]
                if page.workflows
                else ["No workflows discovered."]
            )
            lines.extend(
                f"{item.descriptor.reference}  {_compact(item.descriptor.description or '')}"
                for item in page.workflows
            )
            lines.extend(
                f"Discovery error: {error.source}: {_compact(error.error)}"
                for error in page.errors
            )
            _emit(page, args, lines + _more(page.next_after))
        return
    if family == "execution":
        if action == "list":
            summaries = await connection.execution_summaries(
                p.SummaryQuery(
                    after=args.after, limit=args.limit, active_only=args.active
                )
            )
            lines = (
                ["Execution  Reference  Status  Active  Pending inputs  Auth wait"]
                if summaries.executions
                else ["No executions found."]
            )
            lines.extend(
                f"{item.execution_id}  {item.reference or '-'}  {item.status}  {item.active}  {item.pending_inputs}  {item.awaiting_authentication}"
                for item in summaries.executions
            )
            _emit(summaries, args, lines + _more(summaries.next_after))
        elif action == "show":
            summary = await connection.execution_summary(args.execution_id)
            print(summary.model_dump_json(indent=None if args.json else 2))
        elif action == "start":
            data = _json(
                await _read(args.input_file)
                if args.input_file is not None
                else args.input or "{}"
            )
            if not isinstance(data, dict):
                raise ValueError("workflow input must be a JSON object")
            accepted = await connection.start(
                p.Start(
                    reference=args.reference, input=data, idempotency_key=_key(args)
                )
            )
            _emit(
                accepted, args, [f"Start accepted: {accepted.execution_id} (starting)."]
            )
        elif action == "resume":
            accepted = await connection.resume(
                p.Resume(execution_id=args.execution_id, idempotency_key=_key(args))
            )
            _emit(
                accepted,
                args,
                [f"Resume accepted: {accepted.execution_id} (starting)."],
            )
        elif action == "stop":
            active = await connection.stop(args.execution_id)
            _emit(
                active,
                args,
                [f"Stop requested: {active.execution_id} ({active.status})."],
            )
        elif action == "delete":
            deleted = await connection.delete_execution(args.execution_id)
            _emit(
                deleted,
                args,
                [
                    (
                        f"Permanently deleted execution '{deleted.execution_id}'."
                        if deleted.deleted
                        else f"Execution '{deleted.execution_id}' is already missing; workspace records cleaned."
                    )
                ],
            )
        elif action == "watch":
            subscription = await connection.subscribe(
                p.Subscribe(execution_id=args.execution_id)
            )
            try:
                summary = await connection.execution_summary(args.execution_id)
                print(summary.model_dump_json())
                if not summary.active:
                    return
                while True:
                    event = await connection.next_event()
                    if event.subscription_id != subscription:
                        continue
                    print(event.model_dump_json(), flush=True)
                    if event.event.get("kind") in (
                        "execution.completed",
                        "execution.failed",
                        "execution.cancelled",
                        "execution.suspended",
                    ):
                        return
            finally:
                if not connection.closed.is_set():
                    await connection.unsubscribe(subscription)
        return
    if family == "input":
        if action == "list":
            requests = await connection.pending_inputs(
                mailbox.RequestQuery(
                    execution_id=args.execution_id,
                    workflow_id=args.workflow_id,
                    channel=args.channel,
                    after=args.after,
                    limit=args.limit,
                )
            )
            lines = (
                ["Request  Workflow  Channel  Type  Prompt"]
                if requests.requests
                else ["No pending inputs."]
            )
            lines.extend(
                f"{item.request_id}  {item.workflow_id}  {item.channel}  {item.response_type}  {_compact(item.payload)}"
                for item in requests.requests
            )
            _emit(requests, args, lines + _more(requests.next_after))
            return
        request = await connection.input_request(
            mailbox.RequestRef(
                execution_id=args.execution_id, request_id=args.request_id
            )
        )
        if action == "show":
            print(request.model_dump_json(indent=None if args.json else 2))
            return
        if args.text is not None:
            response: pydantic.JsonValue = args.text
        elif args.text_file is not None:
            response = await _read(args.text_file)
        elif args.json_file is not None:
            response = _json(await _read(args.json_file))
        else:
            assert args.value is not None
            response = _json(args.value)
        request.validate_response(response)
        answered = await connection.answer_input(
            mailbox.Answer(
                execution_id=args.execution_id,
                request_id=args.request_id,
                response=response,
                idempotency_key=_key(args),
            )
        )
        _emit(
            answered,
            args,
            [f"Input accepted: {answered.request_id} ({answered.status})."],
        )


async def _line(connection: client.RunnerClient) -> str:
    """Allow cancellation and disconnect without waiting for a blocking terminal read."""
    loop = asyncio.get_running_loop()
    result: asyncio.Future[str] = loop.create_future()

    def deliver(line: str, error: typing.Optional[Exception]) -> None:
        if result.done():
            return
        if error is not None:
            result.set_exception(error)
        else:
            result.set_result(line)

    def read() -> None:
        line = ""
        error = None
        try:
            line = input("runner> " if sys.stdin.isatty() else "")
        except Exception as exc:
            error = exc
        try:
            loop.call_soon_threadsafe(deliver, line, error)
        except RuntimeError:
            pass

    Thread(target=read, daemon=True).start()
    closed = asyncio.create_task(connection.closed.wait())
    try:
        await asyncio.wait((result, closed), return_when=asyncio.FIRST_COMPLETED)
        if connection.closed.is_set():
            raise connection.failure
        return result.result()
    finally:
        result.cancel()
        closed.cancel()
        await asyncio.gather(closed, return_exceptions=True)


async def shell(connection: client.RunnerClient, *, json_output: bool = False) -> None:
    """Read commands without blocking the connection's reader; EOF only disconnects."""
    print(f"Connected to runner {connection.instance_id}. Use /help or /exit.")
    while not connection.closed.is_set():
        try:
            line = await _line(connection)
        except EOFError:
            return
        try:
            words = shlex.split(line)
            if not words:
                continue
            words[0] = words[0].removeprefix("/")
            if words == ["exit"]:
                return
            args = parser().parse_args(words, namespace=Arguments(json=json_output))
            args.json = args.json or json_output
            if args.family in ("shell", "exit"):
                raise CommandError(
                    "use /exit to disconnect; nested shells are not supported"
                )
            await execute(connection, args)
        except SystemExit:
            continue
        except (ValueError, OSError, p.RemoteError, TimeoutError) as exc:
            print(f"Command error: {exc}", file=sys.stderr)
    raise connection.failure
