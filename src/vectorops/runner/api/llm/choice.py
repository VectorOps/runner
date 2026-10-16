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


class Selection(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    choice: ChoiceName


class Policy(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    choices: dict[ChoiceName, str] = pydantic.Field(min_length=1)
    max_retries: int = pydantic.Field(default=2, ge=0, le=20, strict=True)


class Completion:
    """Own choice validation and repairs, reconstructed from recorded responses."""

    def __init__(self, choices: Mapping[str, str], max_retries: int) -> None:
        self.policy = Policy(choices=dict(choices), max_retries=max_retries)
        self.repairs = 0
        self.selected: Optional[str] = None

    def instruction(self) -> str:
        return (
            "Complete the user's task using the available tools as needed. "
            f"Before your final response, call {FINISH_TOOL} to select a workflow "
            "outcome. This tool only records the choice; it does not finish the task. "
            "You may continue using tools and update the choice if needed. "
            "Then provide the complete user-facing answer as a normal final "
            "assistant message, not as tool arguments. Available choices "
            "(names mapped to descriptions):\n" + json.dumps(self.policy.choices)
        )

    def definitions(self, tools: Sequence[connect.ToolSpec]) -> list[connect.ToolSpec]:
        if any(tool.name == FINISH_TOOL for tool in tools):
            raise ValueError(f"reserved choice tool name: {FINISH_TOOL}")
        schema = Selection.model_json_schema()
        schema["properties"]["choice"]["enum"] = list(self.policy.choices)
        finish = connect.ToolSpec.external(
            name=FINISH_TOOL,
            description="Select a workflow outcome, then continue to your final response.",
            input_schema=schema,
        )
        return [*tools, finish]

    def handles(self, call: connect.ToolCallBlock) -> bool:
        return call.name == FINISH_TOOL

    def select(self, call: connect.ToolCallBlock) -> connect.ToolResultMessage:
        error: Optional[str] = None
        try:
            selection = Selection.model_validate(call.arguments)
            if selection.choice not in self.policy.choices:
                raise ValueError("choice must be one of the configured names")
            self.selected = selection.choice
        except ValueError as exc:
            error = str(exc)
        return connect.ToolResultMessage(
            tool_call_id=call.id,
            tool_name=call.name,
            content=[
                connect.TextBlock(
                    text=error or "Choice recorded. Continue to your final response."
                )
            ],
            is_error=error is not None,
        )

    def accept(self, response: connect.AssistantMessage) -> ChoiceResult:
        if self.selected is None:
            raise ValueError(f"Select an outcome with {FINISH_TOOL} before finishing.")
        return ChoiceResult(
            answer="".join(
                block.text for block in response.content if block.type == "text"
            ),
            choice=self.selected,
        )

    def repair(self, reason: str) -> connect.UserMessage:
        if self.repairs >= self.policy.max_retries:
            raise ChoiceLimitError(
                f"LLM choice exceeded max_retries={self.policy.max_retries}: {reason}"
            )
        self.repairs += 1
        return connect.UserMessage(
            content=(
                f"Completion was rejected: {reason}\n"
                f"Use {FINISH_TOOL} to select a valid outcome if needed, then provide "
                "a nonblank final assistant answer outside tool arguments. "
                "Allowed choices: " + json.dumps(list(self.policy.choices))
            )
        )
