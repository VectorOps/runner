from pathlib import Path
from typing import Optional

import pytest

from vectorops.runner import models
from vectorops.runner.runner import execution_state, history_query, mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


async def test_history_combined_filters_and_head_pagination(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    root = "root' OR 1=1 --"
    await database.save_workflow(models.Workflow(id=root, definition_ref="test:main"))
    await database.save_workflow(
        models.Workflow(id="child", definition_ref="test:main")
    )
    await database.save_execution(
        execution_state.ExecutionRecord(id="exec", root_workflow_id=root)
    )
    for identifier, workflow, previous in [
        ("a'", root, None),
        ("b'", root, "a'"),
        ("fork", root, "a'"),
        ("child", "child", None),
        ("c'", root, "b'"),
    ]:
        await database.append_step(
            models.WorkflowStep(
                id=identifier,
                workflow_id=workflow,
                previous_step_id=previous,
                operation=models.StepOperation(kind="test", name="test"),
                outcome=models.StepCompleted(),
            )
        )
    query = history_query.HistoryQuery(
        execution_id="exec",
        selection="head",
        workflow_id=root,
        head_step_id="c'",
        step_ids=["a'", "b'", "c'", "fork"],
        limit=1,
    )
    found: list[str] = []
    while True:
        page = await database.query(query)
        found.extend(item.step.id for item in page.items)
        if page.next_cursor is None:
            break
        query = query.model_copy(update={"cursor": page.next_cursor})
    assert found == ["a'", "b'", "c'"]
    assert not (
        await database.query(
            history_query.HistoryQuery(execution_id="exec", step_ids=[])
        )
    ).items
    assert not (
        await database.query(
            history_query.HistoryQuery(
                execution_id="exec", workflow_id="missing' OR 1=1 --"
            )
        )
    ).items
    page = await database.query(
        history_query.HistoryQuery(execution_id="exec", include_children=False)
    )
    assert [item.step.id for item in page.items] == ["a'", "b'", "fork", "c'"]
    with pytest.raises(KeyError):
        await database.query(
            history_query.HistoryQuery(execution_id="exec", head_step_id="missing")
        )


@pytest.mark.parametrize("workflow", [None, "root'"])
async def test_mailbox_filters_pagination_and_selective_cancel(
    tmp_path: Path, workflow: Optional[str]
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    for identifier in ("root'", "other"):
        await database.save_workflow(
            models.Workflow(id=identifier, definition_ref="test:main")
        )
    await database.save_execution(
        execution_state.ExecutionRecord(id="exec", root_workflow_id="root'")
    )
    for identifier, owner, channel in [
        ("a'", "root'", "user'"),
        ("b'", "root'", "user'"),
        ("c", "root'", "other"),
        ("d", "other", "other"),
    ]:
        await database.append_step(
            models.WorkflowStep(
                id=identifier,
                workflow_id=owner,
                operation=models.StepOperation(kind=mailbox.KIND, name="input"),
                outcome=models.StepPending(),
                deferred_delivery=True,
            )
        )
        await database.register_input(
            identifier, mailbox.InputSpec(channel=channel, request_type="text.v1")
        )
    query = mailbox.RequestQuery(
        execution_id="exec", workflow_id=workflow, channel="user'", limit=1
    )
    page = await database.list_inputs(query)
    assert [item.request_id for item in page.requests] == ["a'"]
    assert page.next_after == "a'"
    page = await database.list_inputs(
        query.model_copy(update={"after": page.next_after})
    )
    assert [item.request_id for item in page.requests] == ["b'"]
    assert page.next_after is None
    assert not (
        await database.list_inputs(query.model_copy(update={"channel": "x' OR 1=1 --"}))
    ).requests
    await database.cancel_inputs("a'")
    assert (await database.get_input("a'")).status == "cancelled"
    assert (await database.get_input("b'")).status == "pending"
    await database.answer_input(
        mailbox.Answer(
            execution_id="exec",
            request_id="b'",
            response="answer'",
            idempotency_key="key'",
        )
    )
    first = await database.first_input("root'", "text.v1")
    assert first is not None and first.request_id == "b'"
    assert await database.first_input("root'", "missing'") is None


async def test_workspace_bound_documents_receipts_and_index(tmp_path: Path) -> None:
    async with WorkspaceDatabase(tmp_path) as database:
        key = "x' OR 1=1 --"
        assert await database.load_document(key, lambda: "initial") == "initial"
        await database.save_document(key, "updated'")
        await database.update_document(key, lambda: "unused", lambda value: value + "!")
        assert await database.load_document(key, lambda: "unused") == "updated'!"
        await database.save_receipt(key, "fingerprint'", "result'")
        assert await database.receipt(key) == ("fingerprint'", "result'")
        assert await database.receipt("missing'") is None
        generation, _ = await database.index_status()
        assert await database.replace_index(generation, 1, [(key, "2"), ("later", "1")])
        assert await database.index_page(None, 1) == [key]
        assert await database.index_page(key, 1) == ["later"]
        assert await database.index_page(
            None, None, active=[key], active_only=True
        ) == [key]
