import asyncio
import json
from pathlib import Path
import secrets
from unittest.mock import patch

import pytest

from tests.fixtures import sources
from vectorops.runner import cli
from vectorops.runner.control import client, commands, protocol as p
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox

SOURCE = sources.read("control_catalog.py")


def prepare(path: Path) -> None:
    (path / "workflows").mkdir()
    (path / "workflows/flow.py").write_text(SOURCE)


async def pending(
    connection: client.RunnerClient, identifier: str
) -> mailbox.InputRequest:
    async with asyncio.timeout(5):
        while True:
            try:
                page = await connection.pending_inputs(
                    mailbox.RequestQuery(execution_id=identifier)
                )
                if page.requests:
                    return page.requests[0]
            except p.RemoteError as exc:
                if exc.code != "not_found":
                    raise
            await asyncio.sleep(0.01)


async def settled(connection: client.RunnerClient, identifier: str) -> None:
    async with asyncio.timeout(5):
        while await connection.current(identifier) is not None:
            await asyncio.sleep(0.01)


async def command(connection: client.RunnerClient, *words: str) -> None:
    await commands.execute(
        connection,
        commands.parser().parse_args(list(words), namespace=commands.Arguments()),
    )


async def test_catalog_dynamic_and_paged(tmp_path: Path) -> None:
    prepare(tmp_path)
    (tmp_path / "workflows/broken.py").write_text("this is invalid python")
    (tmp_path / "outside.py").write_text(sources.read("forbidden_import.py"))
    (tmp_path / "workflows/link.py").symlink_to(tmp_path / "outside.py")
    async with client.embedded(tmp_path) as connection:
        page = await connection.workflows(p.ExecutionList(limit=1))
        assert len(page.workflows) == 1 and page.next_after is not None
        assert {item.source for item in page.errors} == {"broken", "link"}
        rest = await connection.workflows(p.ExecutionList(after=page.next_after))
        references = [
            item.descriptor.reference for item in [*page.workflows, *rest.workflows]
        ]
        assert references == [
            "builtin:agent",
            "builtin:chat",
            "builtin:helper",
            "flow:approval",
            "flow:child",
            "flow:main",
        ]
        assert (await connection.describe_workflow("flow:plain")).name == "plain"
        (tmp_path / "workflows/generated.py").write_text(
            sources.read("catalog_generated.py")
        )
        assert "generated:main" in [
            item.descriptor.reference
            for item in (await connection.workflows()).workflows
        ]
        (tmp_path / "workflows/generated.py").unlink()
        assert "generated:main" not in [
            item.descriptor.reference
            for item in (await connection.workflows()).workflows
        ]


async def test_execution_summary_counts_child_input_and_auth(tmp_path: Path) -> None:
    prepare(tmp_path)
    token = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        try:
            async with await client.RunnerClient.tcp(
                await server.listen(), token
            ) as connection:
                accepted = await connection.start(
                    p.Start(reference="flow:main", idempotency_key="first")
                )
                request = await pending(connection, accepted.execution_id)
                summary = await connection.execution_summary(accepted.execution_id)
                assert summary.reference == "flow:main"
                assert summary.pending_inputs == 1 and summary.active
                assert summary.root_workflow_id != request.workflow_id
                page = await connection.workflows()
                root = next(
                    item
                    for item in page.workflows
                    if item.descriptor.reference == "flow:main"
                )
                assert root.active_executions == root.pending_inputs == 1
                with patch.object(
                    service.authentication, "waiting_for", return_value=True
                ):
                    assert (
                        await connection.execution_summary(accepted.execution_id)
                    ).awaiting_authentication
                answer = mailbox.Answer(
                    execution_id=accepted.execution_id,
                    request_id=request.request_id,
                    response="hello",
                    idempotency_key="reply",
                )
                await connection.answer_input(answer)
                await connection.answer_input(answer)
                await settled(connection, accepted.execution_id)
                summary = await connection.execution_summary(accepted.execution_id)
                assert (
                    summary.status == "completed"
                    and not summary.active
                    and summary.pending_inputs == 0
                )
                assert not (
                    await connection.execution_summaries(
                        p.SummaryQuery(active_only=True)
                    )
                ).executions
                assert len((await connection.execution_summaries()).executions) == 1
        finally:
            await server.aclose()


async def test_commands_start_list_send_and_shell(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prepare(tmp_path)
    async with client.embedded(tmp_path) as connection:
        await command(
            connection,
            "execution",
            "start",
            "flow:main",
            "--idempotency-key",
            "start",
            "--json",
        )
        identifier = json.loads(capsys.readouterr().out)["execution_id"]
        request = await pending(connection, identifier)
        await command(connection, "workflow", "list")
        assert "Pending inputs" in capsys.readouterr().out
        await command(connection, "execution", "list", "--active", "--json")
        assert (
            json.loads(capsys.readouterr().out)["executions"][0]["pending_inputs"] == 1
        )
        await command(connection, "input", "list", identifier)
        assert "ghidra.input" in capsys.readouterr().out
        await command(
            connection, "input", "show", identifier, request.request_id, "--json"
        )
        assert (
            json.loads(capsys.readouterr().out)["payload"]
            == "Waiting for Ghidra input..."
        )
        with pytest.raises(ValueError):
            await command(
                connection,
                "input",
                "send",
                identifier,
                request.request_id,
                "--value",
                "{}",
            )
        payload = tmp_path / "input.txt"
        payload.write_text("line one\nline two\n")
        await command(
            connection,
            "input",
            "send",
            identifier,
            request.request_id,
            "--text-file",
            str(payload),
            "--idempotency-key",
            "reply",
        )
        assert "Input accepted" in capsys.readouterr().out
        await settled(connection, identifier)
        await command(
            connection, "execution", "resume", identifier, "--idempotency-key", "resume"
        )
        await settled(connection, identifier)
        lines = iter(["/unknown", "/workflow", "/execution list --json", "/exit"])
        with patch("builtins.input", side_effect=lambda prompt: next(lines)):
            await commands.shell(connection)
        output = capsys.readouterr()
        assert "Command error:" in output.err
        assert '"executions"' in output.out
        assert not connection.closed.is_set()


async def test_json_response_and_idempotent_retry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prepare(tmp_path)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="flow:approval", idempotency_key="start")
        )
        request = await pending(connection, accepted.execution_id)
        source = tmp_path / "answer.json"
        source.write_text('{"approved": true}')
        for _ in range(2):
            await command(
                connection,
                "input",
                "send",
                accepted.execution_id,
                request.request_id,
                "--json-file",
                str(source),
                "--idempotency-key",
                "reply",
                "--json",
            )
            assert json.loads(capsys.readouterr().out)["response"] == {"approved": True}


async def test_cli_over_tcp_and_shell_exit_does_not_stop(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prepare(tmp_path)
    token = secrets.token_urlsafe(32)
    token_path = tmp_path / "token"
    token_path.write_text(token)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        try:
            port = await server.listen()
            args = cli.parser().parse_args(
                [
                    "client",
                    "--port",
                    str(port),
                    "--token-file",
                    str(token_path),
                    "--json",
                    "workflow",
                    "list",
                ],
                namespace=cli.Arguments(),
            )
            await cli.dispatch(args)
            assert len(json.loads(capsys.readouterr().out)["workflows"]) == 6
            async with await client.RunnerClient.tcp(port, token) as connection:
                accepted = await connection.start(
                    p.Start(reference="flow:main", idempotency_key="start")
                )
                await pending(connection, accepted.execution_id)
                args = cli.parser().parse_args(
                    [
                        "client",
                        "--port",
                        str(port),
                        "--token-file",
                        str(token_path),
                        "shell",
                    ],
                    namespace=cli.Arguments(),
                )
                with patch("builtins.input", return_value="/exit"):
                    await cli.dispatch(args)
                assert await connection.current(accepted.execution_id) is not None
                await command(connection, "execution", "stop", accepted.execution_id)
                await settled(connection, accepted.execution_id)
        finally:
            await server.aclose()


async def test_watch_unsubscribes_after_terminal_event(tmp_path: Path) -> None:
    prepare(tmp_path)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="flow:main", idempotency_key="start")
        )
        request = await pending(connection, accepted.execution_id)
        with patch.object(
            connection, "unsubscribe", wraps=connection.unsubscribe
        ) as unsubscribe:
            task = asyncio.create_task(
                command(connection, "execution", "watch", accepted.execution_id)
            )
            await asyncio.sleep(0.03)
            await connection.answer_input(
                mailbox.Answer(
                    execution_id=accepted.execution_id,
                    request_id=request.request_id,
                    response="done",
                    idempotency_key="answer",
                )
            )
            await asyncio.wait_for(task, 5)
            unsubscribe.assert_awaited_once()


async def test_shell_disconnect_while_waiting_for_input(tmp_path: Path) -> None:
    prepare(tmp_path)
    async with client.embedded(tmp_path) as connection:
        with patch("builtins.input", side_effect=EOFError):
            await commands.shell(connection)
        connection.closed.set()
        with pytest.raises(ConnectionError):
            await commands.shell(connection)


def test_command_parse_errors() -> None:
    with pytest.raises(commands.CommandError):
        commands.parser().parse_args(
            ["input", "send", "e", "r", "--text", "x", "--value", "{}"]
        )


async def test_summary_pagination_and_starting_state(tmp_path: Path) -> None:
    prepare(tmp_path)
    async with client.embedded(tmp_path) as connection:
        ids = []
        for index in range(3):
            accepted = await connection.start(
                p.Start(reference="flow:main", idempotency_key=str(index))
            )
            initial = await connection.execution_summary(accepted.execution_id)
            assert initial.status in ("starting", "running")
            ids.append(accepted.execution_id)
            await pending(connection, accepted.execution_id)
        first = await connection.execution_summaries(p.SummaryQuery(limit=1))
        assert first.executions[0].execution_id == min(ids)
        assert first.next_after == min(ids)
        rest = await connection.execution_summaries(
            p.SummaryQuery(after=first.next_after)
        )
        assert [item.execution_id for item in rest.executions] == sorted(ids)[1:]
        assert rest.next_after is None


async def test_catalog_reuses_unchanged_source_and_isolates_bad_callable(
    tmp_path: Path,
) -> None:
    prepare(tmp_path)
    (tmp_path / "workflows/bad_schema.py").write_text(sources.read("bad_schema.py.txt"))
    async with client.embedded(tmp_path) as connection:
        page = await connection.workflows()
        assert any(item.source == "bad_schema:invalid" for item in page.errors)
        assert "bad_schema:valid" in [
            item.descriptor.reference for item in page.workflows
        ]
        with patch.object(
            Path, "read_bytes", side_effect=AssertionError("unchanged workflow reread")
        ):
            assert (await connection.workflows()).workflows == page.workflows
