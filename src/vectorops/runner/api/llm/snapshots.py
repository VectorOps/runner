"""Interpret partial provider snapshots for progress and payload-free estimates."""

from collections.abc import Iterator
from copy import deepcopy
from typing import Optional

import connect
import pydantic

from vectorops.runner.api.llm.models import LLMResponseError


class Accumulator:
    """Build detached partial snapshots while leaving terminal response validation to the provider."""

    def __init__(self) -> None:
        self.blocks: dict[int, dict[str, pydantic.JsonValue]] = {}
        self.snapshot: dict[str, pydantic.JsonValue] = {"content": []}

    def update(self, event: connect.StreamEvent) -> dict[str, pydantic.JsonValue]:
        if event.type == "response_start":
            self.snapshot.update(
                provider=event.provider,
                model=event.model,
                response_id=event.response_id,
            )
        elif event.type in ("text_start", "reasoning_start"):
            self.blocks[event.index] = {
                "type": "text" if event.type == "text_start" else "reasoning",
                "text": "",
            }
        elif event.type in ("text_delta", "reasoning_delta"):
            block = self.blocks.setdefault(
                event.index,
                {
                    "type": "text" if event.type == "text_delta" else "reasoning",
                    "text": "",
                },
            )
            text = block["text"]
            if not isinstance(text, str):
                raise LLMResponseError("invalid streaming text state")
            block["text"] = text + event.delta
        elif event.type == "text_end":
            self.blocks[event.index] = {"type": "text", "text": event.text}
        elif event.type == "reasoning_end":
            self.blocks[event.index] = {
                "type": "reasoning",
                "text": event.text,
                "signature": event.signature,
                "redacted": event.redacted,
            }
        elif event.type == "tool_call_start":
            self.blocks[event.index] = {
                "type": "tool_call",
                "id": event.id,
                "name": event.name,
                "arguments_text": "",
            }
        elif event.type == "tool_call_delta":
            block = self.blocks.setdefault(
                event.index, {"type": "tool_call", "arguments_text": ""}
            )
            arguments = block["arguments_text"]
            if not isinstance(arguments, str):
                raise LLMResponseError("invalid streaming tool state")
            block["arguments_text"] = arguments + event.delta
        elif event.type == "tool_call_end":
            self.blocks[event.index] = event.tool_call.model_dump(mode="json")
        elif event.type == "usage":
            self.snapshot["usage"] = event.usage.model_dump(mode="json")
        self.snapshot["content"] = [
            dict(self.blocks[index]) for index in sorted(self.blocks)
        ]
        return dict(self.snapshot)


def blocks(
    value: pydantic.JsonValue,
) -> Iterator[tuple[int, dict[str, pydantic.JsonValue]]]:
    if isinstance(value, list):
        for index, block in enumerate(value):
            if isinstance(block, dict):
                yield index, block


def output_fields(block: dict[str, pydantic.JsonValue]) -> tuple[str, ...]:
    kind = block.get("type")
    if kind in ("text", "reasoning"):
        return ("text",)
    if kind == "tool_call":
        return ("arguments_text", "arguments")
    return ()


def content(value: pydantic.JsonValue) -> Optional[pydantic.JsonValue]:
    if isinstance(value, str):
        return value
    result: list[pydantic.JsonValue] = []
    for _, block in blocks(value):
        if block.get("type") == "image":
            return None
        result.append(
            {
                key: block[key]
                for key in ("type", "text", "name", "id", "arguments", "arguments_text")
                if key in block
            }
        )
    return result


def usage(value: pydantic.JsonValue) -> Optional[connect.Usage]:
    if not isinstance(value, dict):
        return None
    try:
        result = connect.Usage.model_validate(
            {
                key: value[key]
                for key in (
                    "input_tokens",
                    "output_tokens",
                    "reasoning_tokens",
                    "cache_read_tokens",
                    "cache_write_tokens",
                    "total_tokens",
                    "completeness",
                )
                if key in value
            }
        )
    except pydantic.ValidationError:
        return None
    return result if result.completeness != "none" else None


class Progress:
    """Detect output changes without treating metadata as first-token or stall progress."""

    def __init__(self) -> None:
        self.values: dict[tuple[int, str], pydantic.JsonValue] = {}

    def update(self, value: pydantic.JsonValue) -> bool:
        if not isinstance(value, dict):
            return False
        changed = False
        for index, block in blocks(value.get("content")):
            for field in output_fields(block):
                output = block.get(field)
                if output is None or output == "":
                    continue
                key = (index, field)
                if key not in self.values or self.values[key] != output:
                    self.values[key] = deepcopy(output)
                    changed = True
        return changed
