from vectorops.runner.control import client
from vectorops.runner.runner import mailbox


async def reply(
    connection: client.RunnerClient, request: mailbox.InputRequest, text: str
) -> None:
    await connection.answer_input(
        mailbox.Answer(
            execution_id=request.execution_id,
            request_id=request.request_id,
            response={"content": [{"type": "text", "text": text}]},
            idempotency_key=request.request_id,
        )
    )
