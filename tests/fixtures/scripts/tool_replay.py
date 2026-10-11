import connect
import pydantic
import sys

from tests.fixtures import sources
from vectorops.runner.api import tools


async def main(ctx: object) -> pydantic.JsonValue:
    read = await tools.invoke_tool(
        tools.read_file_tool(),
        connect.ToolCallBlock(
            id="read", name="read_file", arguments={"path": "sample.bin"}
        ),
    )
    process = await tools.invoke_tool(
        tools.exec_tool(),
        connect.ToolCallBlock(
            id="exec",
            name="exec",
            arguments={
                "command": {
                    "kind": "argv",
                    "argv": [
                        sys.executable,
                        str(sources.path("process.py")),
                        "print",
                        "hello",
                    ],
                },
                "yield_timeout_ms": 1000,
            },
        ),
    )
    return [read.model_dump(mode="json"), process.model_dump(mode="json")]
