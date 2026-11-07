from pathlib import Path
import secrets
from typing import Literal

import pytest

from vectorops.runner import models
from vectorops.runner.control import client, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import execution_state, history_query as h
from vectorops.runner.runner.sqlite_history import SQLiteHistory


def step(index: int, content: str) -> models.WorkflowStep:
    return models.WorkflowStep(
        id=f"s{index}",
        workflow_id="root",
        previous_step_id=f"s{index - 1}" if index else None,
        operation=models.StepOperation(kind="test", name="test"),
        outcome=models.StepCompleted(result=content),
    )


async def seed(path: Path) -> SQLiteHistory:
    database = SQLiteHistory(path / "data/executions/exec/history.sqlite3")
    await database.save_workflow(models.Workflow(id="root", definition_ref="job:main"))
    await database.save_execution(
        execution_state.ExecutionRecord(
            id="exec", root_workflow_id="root", status="completed"
        )
    )
    return database


@pytest.mark.parametrize("tcp", [False, True])
@pytest.mark.parametrize("selection", ["head", "tail"])
async def test_large_history_pages_fit_without_losing_steps(
    tmp_path: Path, tcp: bool, selection: Literal["head", "tail"]
) -> None:
    database = await seed(tmp_path)
    content = '\U0001f600\n"' * 200_000
    for index in range(4):
        await database.append_step(step(index, content))
    query = h.HistoryQuery(
        execution_id="exec", workflow_id="root", selection=selection, limit=500
    )
    full = await database.query(query)
    assert not p.response_fits(full)
    token = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(await server.listen(), token)
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, token)
            async with connection:
                pages: list[h.HistoryPage] = []
                while True:
                    page = await connection.history(query)
                    assert page.items
                    assert p.response_fits(page)
                    assert [item.position for item in page.items] == sorted(
                        item.position for item in page.items
                    )
                    pages.append(page)
                    if len(pages) == 1:
                        assert len(page.items) < len(full.items)
                        assert page.next_cursor is not None
                        await database.append_step(step(4, "inserted after snapshot"))
                    if page.next_cursor is None:
                        break
                    assert len(pages) < len(full.items)
                    query = query.model_copy(update={"cursor": page.next_cursor})
                found = [item for page in pages for item in page.items]
                assert sorted(found, key=lambda item: item.position) == full.items
                if selection == "head":
                    assert found == full.items
                else:
                    assert [
                        item for page in reversed(pages) for item in page.items
                    ] == full.items
                empty = await connection.history(
                    h.HistoryQuery(execution_id="exec", workflow_id="missing")
                )
                assert empty.items == []
                assert empty.next_cursor is None
        finally:
            await server.aclose()


async def test_single_oversized_history_step_reports_error_without_disconnect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(p, "MAX_FRAME", 4096)
    database = await seed(tmp_path)
    await database.append_step(step(0, "x" * 5000))
    async with client.embedded(tmp_path) as connection:
        with pytest.raises(p.RemoteError) as error:
            await connection.history(h.HistoryQuery(execution_id="exec"))
        assert error.value.code == "frame_too_large"
        assert (await connection.execution("exec")).id == "exec"


def test_response_budget_includes_escaped_content_and_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = h.HistoryPage(
        execution_id="exec",
        items=[h.HistoryItem(position=1, step=step(0, '\U0001f600\n"'))],
    )
    packet = p.Response(id="\0" * p.MAX_REQUEST_ID, result=p.json_value(page))
    size = len(p.encode_packet(packet))
    monkeypatch.setattr(p, "MAX_FRAME", size)
    assert p.response_fits(page)
    monkeypatch.setattr(p, "MAX_FRAME", size - 1)
    assert not p.response_fits(page)
