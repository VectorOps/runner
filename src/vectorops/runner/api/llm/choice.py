"""Completion protocol and deterministic repair state for an LLM choice loop."""

from collections.abc import Mapping, Sequence
import json
from typing import Annotated, Final, Optional

import connect
import pydantic

FINISH_TOOL: Final = "__finish_choice__"
ChoiceName = Annotated[str, pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$")]


class ChoiceResult(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    answer: str = pydantic.Field(min_length=1, pattern=r"\S")
    choice: ChoiceName


class ChoiceLimitError(RuntimeError):
    """The model exhausted its completion repair budget."""


class Policy(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    choices: dict[ChoiceName, str] = pydantic.Field(min_length=1)
    max_retries: int = pydantic.Field(default=2, ge=0, le=20, strict=True)


class Completion:
    """Own choice validation and repairs, reconstructed from recorded responses."""

    def __init__(self, choices: Mapping[str, str], max_retries: int) -> None:
        self.policy = Policy(choices=dict(choices), max_retries=max_retries)
        self.repairs = 0

    def instruction(self) -> str:
        return (
            "Complete the user's task using the available tools as needed. "
            f"Finish by calling {FINISH_TOOL} exactly once, alone in its round. "
            "Put the complete user-facing response in answer and select one choice. "
            "Plain text alone does not complete this task. Available choices "
            "(names mapped to descriptions):\n" + json.dumps(self.policy.choices)
        )

    def definitions(self, tools: Sequence[connect.ToolSpec]) -> list[connect.ToolSpec]:
        if any(tool.name == FINISH_TOOL for tool in tools):
            raise ValueError(f"reserved choice tool name: {FINISH_TOOL}")
        schema = ChoiceResult.model_json_schema()
        schema["properties"]["choice"]["enum"] = list(self.policy.choices)
        finish = connect.ToolSpec.external(
            name=FINISH_TOOL,
            description="Return the complete answer and exactly one workflow choice.",
            input_schema=schema,
        )
        return [finish] if self.repairs else [*tools, finish]

    def handles(self, calls: Sequence[connect.ToolCallBlock]) -> bool:
        return bool(
            self.repairs or not calls or any(call.name == FINISH_TOOL for call in calls)
        )

    def accept(self, calls: Sequence[connect.ToolCallBlock]) -> ChoiceResult:
        if len(calls) != 1 or calls[0].name != FINISH_TOOL:
            raise ValueError(
                f"Call {FINISH_TOOL} exactly once without other tool calls."
            )
        result = ChoiceResult.model_validate(calls[0].arguments)
        if result.choice not in self.policy.choices:
            raise ValueError("choice must be one of the configured names")
        return result

    def feedback(
        self, calls: Sequence[connect.ToolCallBlock], error: Optional[str] = None
    ) -> list[connect.Message]:
        return [
            connect.ToolResultMessage(
                tool_call_id=call.id,
                tool_name=call.name,
                content=[connect.TextBlock(text=error or "Completion accepted.")],
                is_error=error is not None,
            )
            for call in calls
        ]

    def repair(self, reason: str) -> connect.UserMessage:
        if self.repairs >= self.policy.max_retries:
            raise ChoiceLimitError(
                f"LLM choice exceeded max_retries={self.policy.max_retries}: {reason}"
            )
        self.repairs += 1
        return connect.UserMessage(
            content=(
                f"Completion was rejected: {reason}\n"
                f"Do not repeat work or call ordinary tools. Call {FINISH_TOOL} alone "
                "with the complete answer and a valid choice. Allowed choices: "
                + json.dumps(list(self.policy.choices))
            )
        )
