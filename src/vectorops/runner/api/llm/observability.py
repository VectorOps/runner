"""Payload-free LLM estimates, kept separate from provider-reported usage."""

from decimal import Decimal
import json
import math
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api.llm import models, snapshots
from vectorops.runner import usage as usage_models


class CompactionSavings(pydantic.BaseModel):
    before_input_tokens_estimate: Optional[int] = None
    after_input_tokens_estimate: Optional[int] = None
    saved_tokens_estimate: Optional[int] = None


class Metrics(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", allow_inf_nan=False)

    version: Literal[1] = 1
    token_estimation: Literal["json_characters_div_4"] = "json_characters_div_4"
    catalog_model: Optional[str] = None
    catalog_provider: Optional[str] = None
    input_tokens_estimate: Optional[int] = None
    output_tokens_estimate: Optional[int] = None
    context_window: Optional[int] = None
    model_max_output_tokens: Optional[int] = None
    output_budget: Optional[int] = None
    input_budget: Optional[int] = None
    context_remaining_estimate: Optional[int] = None
    remaining_output_headroom_estimate: Optional[int] = None
    context_overflow_estimate: Optional[int] = None
    provider_usage: Optional[connect.Usage] = None
    cost_estimate_usd: Optional[Decimal] = None
    cost_basis: Optional[Literal["provider_usage", "token_estimate"]] = None
    attempt: int = 1
    retries: int = 0
    compaction: Optional[CompactionSavings] = None

    def context(self, step_id: str) -> usage_models.Context:
        """Project measured context when final usage exists, preserving stored estimates."""
        values = self.model_dump(
            include=set(usage_models.Context.model_fields)
            - {"step_id", "input_tokens_source"}
        )
        source: Optional[Literal["provider_usage", "token_estimate"]] = (
            "token_estimate" if self.input_tokens_estimate is not None else None
        )
        provider = self.provider_usage
        if provider is not None and provider.completeness == "final":
            incoming = (
                provider.input_tokens
                + provider.cache_read_tokens
                + provider.cache_write_tokens
            )
            values["input_tokens_estimate"] = incoming
            source = "provider_usage"
            if self.context_window is not None:
                remaining = self.context_window - incoming
                values["context_remaining_estimate"] = max(0, remaining)
                values["context_overflow_estimate"] = max(
                    0, (self.output_budget or 0) - remaining
                )
                headroom = [remaining - provider.output_tokens]
                if self.output_budget is not None:
                    headroom.append(self.output_budget - provider.output_tokens)
                values["remaining_output_headroom_estimate"] = max(0, min(headroom))
        return usage_models.Context(
            step_id=step_id, input_tokens_source=source, **values
        )


def _tokens(value: pydantic.JsonValue) -> int:
    return math.ceil(
        len(json.dumps(value, ensure_ascii=False, separators=(",", ":"))) / 4
    )


def input_tokens(request: connect.GenerateRequest) -> Optional[int]:
    """Estimate text and schema overhead without counting provider metadata as context.

    Images have provider-specific accounting, so multimodal estimates are unknown
    rather than estimates of the size of their base64 transport representation.
    """
    messages: list[pydantic.JsonValue] = []
    for message in request.messages:
        content = snapshots.content(message.model_dump(mode="json")["content"])
        if content is None:
            return None
        entry: dict[str, pydantic.JsonValue] = {
            "role": message.role,
            "content": content,
        }
        if isinstance(message, connect.ToolResultMessage):
            entry.update(tool_call_id=message.tool_call_id, tool_name=message.tool_name)
        messages.append(entry)
    tools: list[pydantic.JsonValue] = [
        tool.model_dump(mode="json", include={"name", "description", "input_schema"})
        for tool in request.tools
    ]
    return _tokens(
        {"messages": messages, "system_prompt": request.system_prompt, "tools": tools}
    )


def compaction_savings(
    before: connect.GenerateRequest, after: connect.GenerateRequest
) -> CompactionSavings:
    first, last = input_tokens(before), input_tokens(after)
    return CompactionSavings(
        before_input_tokens_estimate=first,
        after_input_tokens_estimate=last,
        saved_tokens_estimate=None if first is None or last is None else first - last,
    )


def _cost(model: connect.ModelSpec, usage: connect.Usage) -> Optional[Decimal]:
    pricing = model.pricing
    if pricing is None:
        return None
    for tokens, rate in (
        (usage.input_tokens, pricing.input_per_million),
        (usage.output_tokens, pricing.output_per_million),
        (usage.cache_read_tokens, pricing.cache_read_per_million),
        (usage.cache_write_tokens, pricing.cache_write_per_million),
    ):
        if tokens and rate is None:
            return None
    cost = connect.estimate_cost(model, usage)
    return None if cost is None else cost.total_cost


class Observation:
    """Track one activity invocation; retries replace attempt-local observations."""

    def __init__(
        self, settings: models.Settings, request: connect.GenerateRequest
    ) -> None:
        try:
            self.model: Optional[connect.ModelSpec] = (
                connect.default_model_registry.resolve(
                    settings.model, settings.provider
                )
            )
        except connect.RegistryError:
            self.model = None
        self.base = Metrics(input_tokens_estimate=input_tokens(request))
        limits = [request.max_output_tokens]
        if self.model is not None:
            self.base.catalog_model = self.model.model
            self.base.catalog_provider = self.model.provider
            self.base.context_window = self.model.context_window
            self.base.model_max_output_tokens = self.model.max_output_tokens
            limits.append(self.model.max_output_tokens)
        known_limits = [limit for limit in limits if limit is not None]
        self.base.output_budget = min(known_limits) if known_limits else None
        if self.base.context_window is not None and self.base.output_budget is not None:
            self.base.input_budget = max(
                0, self.base.context_window - self.base.output_budget
            )

    def snapshot(
        self, value: pydantic.JsonValue, attempt: int
    ) -> dict[str, pydantic.JsonValue]:
        result = self.base.model_copy(deep=True)
        result.attempt, result.retries = attempt, attempt - 1
        if isinstance(value, dict):
            content = snapshots.content(value.get("content", []))
            result.output_tokens_estimate = (
                None if content is None else (_tokens(content) if content else 0)
            )
            result.provider_usage = snapshots.usage(value.get("usage"))
        output = result.output_tokens_estimate
        if result.provider_usage is not None:
            output = result.provider_usage.output_tokens
        headroom: list[int] = []
        if result.output_budget is not None and output is not None:
            headroom.append(result.output_budget - output)
        if (
            result.context_window is not None
            and result.input_tokens_estimate is not None
        ):
            remaining = result.context_window - result.input_tokens_estimate
            result.context_remaining_estimate = max(0, remaining)
            result.context_overflow_estimate = max(
                0, (result.output_budget or 0) - remaining
            )
            if output is not None:
                headroom.append(remaining - output)
        if headroom:
            result.remaining_output_headroom_estimate = max(0, min(headroom))
        usage = result.provider_usage
        basis: Literal["provider_usage", "token_estimate"] = "provider_usage"
        if (
            usage is None
            and result.input_tokens_estimate is not None
            and result.output_tokens_estimate is not None
        ):
            usage = connect.Usage(
                input_tokens=result.input_tokens_estimate,
                output_tokens=result.output_tokens_estimate,
            )
            basis = "token_estimate"
        if self.model is not None and usage is not None:
            result.cost_estimate_usd = _cost(self.model, usage)
            if result.cost_estimate_usd is not None:
                result.cost_basis = basis
        return {"llm": result.model_dump(mode="json")}
