"""Small stdio MCP server used for transport and replay integration tests."""

import json
import os
from pathlib import Path
import sys
import time


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_text(str(os.getpid()))
    for line in sys.stdin:
        request = json.loads(line)
        if "id" not in request:
            continue
        method = request["method"]
        if mode == "hang":
            time.sleep(60)
        if mode == "oversize":
            print("x" * 2000000, flush=True)
            continue
        if method == "initialize":
            result = {
                "protocolVersion": request["params"]["protocolVersion"],
                "capabilities": (
                    {"tools": {}}
                    if mode == "tools_only"
                    else {"tools": {}, "prompts": {}, "resources": {}}
                ),
                "serverInfo": {"name": "fixture", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Echo a value",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"value": {"type": "string"}},
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                    }
                ]
            }
            if mode == "cursor":
                result["tools"] = []
                result["nextCursor"] = "same"
        elif method == "prompts/list":
            result = {
                "prompts": [
                    {"name": "greet", "arguments": [{"name": "who", "required": True}]}
                ]
            }
            if mode == "content_cursor":
                result = {"prompts": [], "nextCursor": "same"}
        elif method == "resources/list":
            result = {
                "resources": [
                    {
                        "name": "picture",
                        "uri": "file:///picture.png",
                        "mimeType": "image/png",
                    }
                ]
            }
        elif method == "prompts/get":
            result = {
                "description": "A greeting",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": request["params"]["arguments"]["who"],
                        },
                    },
                    {"role": "assistant", "content": {"type": "text", "text": "hello"}},
                ],
            }
            if mode == "large_content":
                result["description"] = "x" * 2000000
        elif method == "resources/read":
            result = {
                "contents": [
                    {
                        "uri": request["params"]["uri"],
                        "mimeType": "image/png",
                        "blob": "aW1hZ2U=",
                    }
                ]
            }
        elif method == "tools/call":
            value = request["params"]["arguments"]["value"]
            if value == "wait":
                time.sleep(1)
            result = {
                "content": [{"type": "text", "text": value}],
                "structuredContent": {"value": value},
                "isError": value == "error",
            }
        else:
            result = {}
        print(
            json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}),
            flush=True,
        )


if __name__ == "__main__":
    main()
