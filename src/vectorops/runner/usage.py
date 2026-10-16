"""Serializable usage totals and context projections shared by runner APIs."""

from decimal import Decimal
from typing import Literal, Optional, Self

import pydantic


class Cost(pydantic.BaseModel):
    """Known estimated cost with compact, replaceable coverage indicators."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    cost_estimate_usd: Decimal = pydantic.Field(default=Decimal(0), ge=0)
    estimated_calls: int = pydantic.Field(default=0, ge=0)
    incomplete_calls: int = pydantic.Field(default=0, ge=0)

    @pydantic.model_validator(mode="before")
    @classmethod
    def legacy_coverage(cls, value: object) -> object:
        """Read earlier detailed counters conservatively without dropping stored sums."""
        if not isinstance(value, dict):
            return value
        result = dict(value)
        for target, sources in (
            ("estimated_calls", ("estimated_token_calls", "estimated_cost_calls")),
            (
                "incomplete_calls",
                (
                    "unknown_input_calls",
                    "unknown_output_calls",
                    "unknown_cost_calls",
                    "partial_provider_calls",
                    "unrecorded_attempts",
                ),
            ),
        ):
            counts = [result.pop(name) for name in sources if name in result]
            if counts:
                result.setdefault(target, sum(counts))
        return result

    def replace(self, previous: "Cost", current: "Cost") -> Self:
        values, old, new = (
            self.model_dump(),
            previous.model_dump(),
            current.model_dump(),
        )
        return type(self).model_validate(
            {
                name: values[name] - old[name] + new[name]
                for name in type(self).model_fields
            }
        )

    def since(self, baseline: Self) -> Self:
        values, old = self.model_dump(), baseline.model_dump()
        return type(self).model_validate(
            {name: max(0, values[name] - old[name]) for name in type(self).model_fields}
        )


class Totals(Cost):
    input_tokens: int = pydantic.Field(default=0, ge=0)
    output_tokens: int = pydantic.Field(default=0, ge=0)
    reasoning_tokens: int = pydantic.Field(default=0, ge=0)
    cache_read_tokens: int = pydantic.Field(default=0, ge=0)
    cache_write_tokens: int = pydantic.Field(default=0, ge=0)
    calls: int = pydantic.Field(default=0, ge=0)


class WorkflowUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    own: Totals = pydantic.Field(default_factory=Totals)
    subtree: Cost = pydantic.Field(default_factory=Cost)

    @pydantic.model_validator(mode="before")
    @classmethod
    def legacy_subtree(cls, value: object) -> object:
        if not isinstance(value, dict) or not isinstance(value.get("subtree"), dict):
            return value
        subtree = dict(value["subtree"])
        for name in Totals.model_fields.keys() - Cost.model_fields.keys():
            subtree.pop(name, None)
        return {**value, "subtree": subtree}


class Context(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    step_id: str
    input_tokens_source: Optional[Literal["provider_usage", "token_estimate"]] = None
    input_tokens_estimate: Optional[int] = None
    output_tokens_estimate: Optional[int] = None
    context_window: Optional[int] = None
    input_budget: Optional[int] = None
    output_budget: Optional[int] = None
    context_remaining_estimate: Optional[int] = None
    remaining_output_headroom_estimate: Optional[int] = None
    context_overflow_estimate: Optional[int] = None


class ExecutionUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    totals: Totals = pydantic.Field(default_factory=Totals)
    root_context: Optional[Context] = None


class RunnerUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    totals: Totals = pydantic.Field(default_factory=Totals)
    reset_at: Optional[str] = None
    generation: int = pydantic.Field(default=0, ge=0)
