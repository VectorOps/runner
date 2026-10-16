"""Measured conversation checkpoints and tool-safe prefix selection."""

from collections.abc import Sequence
from dataclasses import dataclass
import json
from typing import Optional

import connect
import pydantic

from vectorops.runner.api.llm import configuration, models, observability
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import streaming

Generation = models.Generation


def _encoded(value: pydantic.JsonValue) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def summary_text(response: connect.AssistantMessage) -> str:
    text = "".join(block.text for block in response.content if block.type == "text")
    if not text.strip() or any(block.type == "tool_call" for block in response.content):
        raise models.LLMResponseError(
            "compaction requires a nonempty tool-free summary"
        )
    return text


def replacement(
    messages: Sequence[connect.Message], cut: int, text: str
) -> list[connect.Message]:
    return [
        connect.UserMessage(content="Summary of earlier conversation:\n" + text),
        *messages[cut:],
    ]


def safe_boundaries(messages: Sequence[connect.Message]) -> set[int]:
    pending: set[str] = set()
    safe = {0}
    for index, message in enumerate(messages, start=1):
        if message.role == "assistant":
            pending.update(
                block.id for block in message.content if block.type == "tool_call"
            )
        elif message.role == "tool":
            pending.discard(message.tool_call_id)
        if not pending:
            safe.add(index)
    return safe


def legacy_cut(
    messages: Sequence[connect.Message], policy: models.Compaction
) -> Optional[int]:
    transcript: list[pydantic.JsonValue] = [
        message.model_dump(mode="json") for message in messages
    ]
    if (
        policy.max_context_characters is None
        or len(_encoded(transcript)) <= policy.max_context_characters
    ):
        return None
    limit = len(messages) - policy.keep_recent_messages
    return max(
        (index for index in safe_boundaries(messages) if 0 < index <= limit),
        default=None,
    )


@dataclass(frozen=True)
class Summary:
    """Own summary request policy and live validation without committing conversation state."""

    request: connect.GenerateRequest
    settings: models.Settings
    generation_settings: models.Settings
    before: connect.GenerateRequest
    cut: int
    prefix_tokens: Optional[int] = None

    def finalize(
        self,
        item: streaming.ActivityFinal[pydantic.JsonValue],
        context: ActivityExecutionContext,
    ) -> streaming.ActivityFinal[pydantic.JsonValue]:
        response = connect.AssistantMessage.model_validate(item.value)
        if self.prefix_tokens is not None:
            summary_text(response)
            size = summary_tokens(response, self.settings, self.generation_settings)
            if size is not None and size >= self.prefix_tokens:
                raise models.LLMResponseError(
                    "compaction summary did not reduce the selected prefix"
                )
        text = "".join(block.text for block in response.content if block.type == "text")
        if text.strip() and not any(
            block.type == "tool_call" for block in response.content
        ):
            after = self.before.model_copy(
                update={"messages": replacement(self.before.messages, self.cut, text)}
            )
            assert item.metrics is not None
            metric = observability.Metrics.model_validate(item.metrics["llm"])
            metric.compaction = observability.compaction_savings(self.before, after)
            return streaming.ActivityFinal(
                item.value, {"llm": metric.model_dump(mode="json")}
            )
        return item


def legacy_summary(
    messages: Sequence[connect.Message],
    cut: int,
    instruction: str,
    settings: models.Settings,
    before: connect.GenerateRequest,
) -> Summary:
    prefix: list[pydantic.JsonValue] = [
        message.model_dump(mode="json") for message in messages[:cut]
    ]
    request = connect.GenerateRequest(
        system_prompt=instruction,
        messages=[connect.UserMessage(content=_encoded(prefix))],
        temperature=settings.temperature,
        max_output_tokens=settings.max_output_tokens,
        reasoning=settings.reasoning,
    )
    return Summary(request, settings, settings, before, cut)


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
    if response.usage.completeness == "final" and summary.model == generation.model:
        return response.usage.output_tokens
    return None


def context_window(
    settings: models.Settings, policy: models.Compaction
) -> Optional[int]:
    if policy.context_window is not None:
        return policy.context_window
    try:
        return connect.default_model_registry.resolve(settings.model).context_window
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

    def latest_anchor(self, index: Optional[int] = None) -> Optional[str]:
        if not self.checkpoints:
            return None
        latest = self.checkpoints[-1]
        return latest.message_id if index is None or latest.index == index else None

    def prepare(
        self,
        messages: Sequence[connect.Message],
        selection: Selection,
        policy: models.Compaction,
        generation: models.Settings,
        before: connect.GenerateRequest,
    ) -> Summary:
        values = generation.model_dump(mode="json")
        values["max_output_tokens"] = policy.summary_max_output_tokens
        values = configuration.merge_settings(
            values, policy.summary_settings.model_dump(mode="json", exclude_unset=True)
        )
        if values["max_output_tokens"] is None:
            values["max_output_tokens"] = policy.summary_max_output_tokens
        settings = models.Settings.model_validate(values)
        start = int(self.has_summary)
        previous = messages[0].model_dump(mode="json") if start else None
        transcript: list[pydantic.JsonValue] = [
            message.model_dump(mode="json")
            for message in messages[start : selection.cut]
        ]
        request = connect.GenerateRequest(
            system_prompt=policy.instruction
            + (
                "\nUpdate the previous summary with the transcript. Preserve task intent, "
                "constraints, decisions, and unfinished work; remove superseded details. "
                "Use sections: Goal, Constraints, Progress, Decisions, Next steps, Critical context."
            ),
            messages=[
                connect.UserMessage(
                    content=_encoded(
                        {"previous_summary": previous, "transcript": transcript}
                    )
                )
            ],
            temperature=settings.temperature,
            max_output_tokens=settings.max_output_tokens,
            reasoning=settings.reasoning,
        )
        return Summary(
            request,
            settings,
            generation,
            before,
            selection.cut,
            selection.prefix_tokens,
        )

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
        safe = safe_boundaries(messages[: anchor.index + 1])
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
