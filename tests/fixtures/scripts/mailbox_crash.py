import asyncio
from pathlib import Path
import sys
from unittest.mock import patch

from vectorops.runner import models
from vectorops.runner.control import protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def main() -> None:
    async with RunnerService(Path(sys.argv[1])) as service:
        accepted = p.Accepted.model_validate(
            await service.dispatch(
                "executions.start",
                {"reference": f"flow:{sys.argv[3]}", "idempotency_key": "start"},
            )
        )
        query = mailbox.RequestQuery(execution_id=accepted.execution_id)
        while True:
            try:
                page = await service.runner.pending_inputs(query)
            except (FileNotFoundError, KeyError):
                await asyncio.sleep(0.01)
                continue
            if page.requests:
                break
            await asyncio.sleep(0.01)
        request = page.requests[0]
        if sys.argv[2] == "True":
            original = SQLiteHistory.update_step

            async def hold(self: SQLiteHistory, step: models.WorkflowStep) -> None:
                if (
                    step.operation.kind in (mailbox.KIND, "llm.tool")
                    and step.outcome.kind == "completed"
                ):
                    await asyncio.Event().wait()
                await original(self, step)

            patch.object(SQLiteHistory, "update_step", hold).start()
            await service.runner.answer_input(
                mailbox.Answer(
                    execution_id=request.execution_id,
                    request_id=request.request_id,
                    response=(
                        {"decision": "approve", "scope": "workflow"}
                        if sys.argv[3].startswith("tool")
                        else "before crash"
                    ),
                    idempotency_key="answer",
                )
            )
        print(request.model_dump_json(), flush=True)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
