from pathlib import Path

import pytest

from tests.control.test_mailbox import completed, pending
from tests.fixtures import sources
from vectorops.runner import input_schema, models
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import history_query, mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory


def test_public_request_model_has_no_schema_fields() -> None:
    spec = mailbox.InputSpec(
        channel="test",
        response_type="json",
        request_schema={"type": "object"},
        response_schema={"type": "integer"},
        payload={"request_schema": "user data"},
    )
    stored = mailbox.StoredInputRequest(
        **spec.model_dump(),
        request_id="request",
        execution_id="execution",
        workflow_id="workflow",
    )
    public = stored.public()
    assert type(public) is mailbox.InputRequest
    assert "request_schema" not in mailbox.InputRequest.model_fields
    assert "response_schema" not in mailbox.InputRequest.model_fields
    assert "request_schema" not in public.model_dump()
    assert "response_schema" not in public.model_dump()
    assert public.payload == {"request_schema": "user data"}
    assert stored.specification() == spec
    with pytest.raises(input_schema.InputValidationError):
        stored.validate_response("wrong")


async def test_input_history_and_responses_are_schema_free(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(sources.read("input_contract.py"))
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            protocol.Start(reference="flow:main", idempotency_key="start")
        )
        request = (await pending(connection, accepted.execution_id))[0]
        assert "response_schema" not in request.model_dump()
        database = SQLiteHistory(
            tmp_path / "data/executions" / request.execution_id / "history.sqlite3"
        )
        spec = await database.get_input_spec(request.request_id)
        assert spec.response_schema == {"type": "integer"}
        step = await database.get_step(request.request_id)
        assert isinstance(step.operation, models.StepOperation)
        assert step.operation.version == 2
        assert "request_schema" not in step.operation.input
        assert "response_schema" not in step.operation.input
        assert step.operation.input["contract_digest"] == spec.contract_digest(
            spec.request_schema, spec.response_schema
        )
        assert await database.get_input(request.request_id) == request
    async with client.embedded(tmp_path) as connection:
        assert (await pending(connection, request.execution_id))[0] == request
        answer = mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response="invalid",
            idempotency_key="answer",
        )
        with pytest.raises(protocol.RemoteError, match="contract"):
            await connection.answer_input(answer)
        await connection.answer_input(answer.model_copy(update={"response": 42}))
        await completed(connection, request.execution_id)
        page = await connection.history(
            history_query.HistoryQuery(execution_id=request.execution_id)
        )
        assert "response_schema" not in page.model_dump_json()
        assert "request_schema" not in page.model_dump_json()
    export = tmp_path / "history.jsonl"
    await database.export_execution(export)
    assert "response_schema" not in export.read_text()
    assert "request_schema" not in export.read_text()
