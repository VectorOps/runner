"""Measured conversation checkpoints and tool-safe prefix selection."""

from collections.abc import Sequence
from typing import Optional

import connect
import pydantic

from vectorops.runner.api.llm import models


class Generation(pydantic.BaseModel):
    response: connect.AssistantMessage
    message_id: str
    context_window: Optional[int] = None


class Checkpoint(pydantic.BaseModel):
    message_id: str
    index: int
    signature: str
    input_tokens: int
    output_tokens: int
    context_tokens: int
    context_window: Optional[int] = None


class Selection(pydantic.BaseModel):
    anchor_id: str
    cut: int
    prefix_tokens: int


def summary_tokens(
    response: connect.AssistantMessage,
    summary: models.Settings,
    generation: models.Settings,
) -> Optional[int]:
    if (
        response.usage.completeness == "final"
        and summary.model == generation.model
        and summary.provider == generation.provider
    ):
        return response.usage.output_tokens
    return None


def context_window(
    settings: models.Settings, policy: models.Compaction
) -> Optional[int]:
    if policy.context_window is not None:
        return policy.context_window
    try:
        return connect.default_model_registry.resolve(
            settings.model, settings.provider
        ).context_window
    except connect.RegistryError:
        return None


def input_tokens(usage: connect.Usage) -> Optional[int]:
    if usage.completeness != "final":
        return None
    return usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens


class History:
    """Keep derived context coordinates separate from immutable provider usage.

    A checkpoint is immediately before its assistant message. Its input usage
    therefore measures the prefix, not that message or later tool results.
    """

    def __init__(self) -> None:
        self.checkpoints: list[Checkpoint] = []
        self.has_summary = False
        self.compacted_anchor: Optional[str] = None

    def record(self, generation: Generation, index: int, signature: str) -> None:
        usage = generation.response.usage
        tokens = input_tokens(usage)
        if tokens is None:
            return
        self.checkpoints.append(
            Checkpoint(
                message_id=generation.message_id,
                index=index,
                signature=signature,
                input_tokens=tokens,
                output_tokens=usage.output_tokens,
                context_tokens=tokens,
                context_window=generation.context_window,
            )
        )

    def select(
        self,
        messages: Sequence[connect.Message],
        message_id: str,
        policy: models.Compaction,
        signature: str,
        *,
        force: bool = False,
    ) -> Optional[Selection]:
        """Retain at least the target at measured, complete-exchange boundaries.

        Never consume the anchor response or any newer messages. Unknown usage
        and changed request shapes are not bridged with character estimates.
        """
        anchor = next(
            (point for point in self.checkpoints if point.message_id == message_id),
            None,
        )
        if (
            anchor is None
            or anchor.signature != signature
            or self.compacted_anchor == message_id
        ):
            return None
        window = anchor.context_window
        if not force and (
            window is None
            or anchor.input_tokens < window * policy.trigger_threshold_ratio
        ):
            return None
        budget = policy.keep_recent_tokens
        if budget is None:
            if window is None:
                return None
            budget = max(1, int(window * policy.keep_recent_ratio))
        pending: set[str] = set()
        safe: set[int] = set()
        for index, message in enumerate(messages[: anchor.index + 1]):
            if not pending:
                safe.add(index)
            if message.role == "assistant":
                pending.update(
                    block.id for block in message.content if block.type == "tool_call"
                )
            elif message.role == "tool":
                pending.discard(message.tool_call_id)
        for point in reversed(self.checkpoints):
            if point.index > anchor.index:
                continue
            if point.signature != signature:
                break
            if point.index <= int(self.has_summary):
                break
            retained = (
                anchor.context_tokens + anchor.output_tokens - point.context_tokens
            )
            if retained >= budget and point.index in safe:
                return Selection(
                    anchor_id=message_id,
                    cut=point.index,
                    prefix_tokens=point.context_tokens,
                )
        return None

    def commit(self, selection: Selection, summary_tokens: Optional[int]) -> None:
        """Rebase retained checkpoints from the selected prefix, never a target budget."""
        retained = [point for point in self.checkpoints if point.index >= selection.cut]
        if summary_tokens is None:
            retained = []
        else:
            delta = selection.prefix_tokens - summary_tokens
            for point in retained:
                point.index += 1 - selection.cut
                point.context_tokens -= delta
        self.checkpoints = retained
        self.has_summary = True
        self.compacted_anchor = selection.anchor_id
