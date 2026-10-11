import asyncio
import json
from pathlib import Path
import sys

from vectorops.runner.control import client, protocol as p
from tests.control.test_mailbox import pending


async def main() -> None:
    async with client.embedded(Path(sys.argv[1])) as connection:
        accepted = await asyncio.gather(
            *(
                connection.start(
                    p.Start(reference="flow:workflow", idempotency_key=str(i))
                )
                for i in range(3)
            )
        )
        requests = [
            (await pending(connection, item.execution_id))[0] for item in accepted
        ]
        print(
            json.dumps([request.model_dump(mode="json") for request in requests]),
            flush=True,
        )
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
