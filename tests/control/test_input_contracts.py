from pathlib import Path

import pytest

from tests.control.test_mailbox import WORKFLOW, completed, pending
from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner import mailbox


async def test_approval_schema_error_survives_transport_and_restart(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as connection:
        accepted = await connection.start(
            p.Start(reference="flow:tool", idempotency_key="start")
        )
        original = (await pending(connection, accepted.execution_id))[0]
        assert original.request_type == "tool.approval.v1"
        assert "request_schema" not in original.model_dump()
        assert "response_schema" not in original.model_dump()
    async with client.embedded(tmp_path) as connection:
        request = (await pending(connection, accepted.execution_id))[0]
        assert request == original
        invalid = mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response={"approved": "yes"},
            idempotency_key="answer",
        )
        with pytest.raises(p.RemoteError) as error:
            await connection.answer_input(invalid)
        assert error.value.code == "input_validation"
        assert error.value.details == {
            "issues": [
                {
                    "path": ["response", "approved"],
                    "message": "must satisfy type",
                }
            ]
        }
        assert (await pending(connection, accepted.execution_id))[0] == original
        valid = invalid.model_copy(update={"response": {"approved": True}})
        receipt = await connection.answer_input(valid)
        assert await connection.answer_input(valid) == receipt
        await completed(connection, accepted.execution_id)
