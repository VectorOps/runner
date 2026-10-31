"""Serializable usage totals and context projections shared by runner APIs."""

from decimal import Decimal
from typing import Optional, Self

import pydantic


class Cost(pydantic.BaseModel):
    """Cost derived only from finalized usage, with explicit measurement coverage."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    cost_estimate_usd: Optional[Decimal] = pydantic.Field(default=None, ge=0)
    confirmed_calls: int = pydantic.Field(default=0, ge=0)
    priced_calls: int = pydantic.Field(default=0, ge=0)
    incomplete_calls: int = pydantic.Field(default=0, ge=0)

    def replace(self, previous: "Cost", current: "Cost") -> Self:
        values, old, new = (
            self.model_dump(),
            previous.model_dump(),
            current.model_dump(),
        )
        result = {
            name: (values[name] or 0) - (old[name] or 0) + (new[name] or 0)
            for name in type(self).model_fields
        }
        if result["priced_calls"] == 0:
            result["cost_estimate_usd"] = None
        return type(self).model_validate(result)

    def since(self, baseline: Self) -> Self:
        values, old = self.model_dump(), baseline.model_dump()
        result = {
            name: max(0, (values[name] or 0) - (old[name] or 0))
            for name in type(self).model_fields
        }
        if result["priced_calls"] == 0:
            result["cost_estimate_usd"] = None
        return type(self).model_validate(result)


class Totals(Cost):
    input_tokens: int = pydantic.Field(default=0, ge=0)
    output_tokens: int = pydantic.Field(default=0, ge=0)
    reasoning_tokens: int = pydantic.Field(default=0, ge=0)
    cache_read_tokens: int = pydantic.Field(default=0, ge=0)
    cache_write_tokens: int = pydantic.Field(default=0, ge=0)
    calls: int = pydantic.Field(default=0, ge=0)

    @pydantic.model_validator(mode="before")
    @classmethod
    def derived_input(cls, value: object) -> object:
        if isinstance(value, dict):
            return {
                name: item for name, item in value.items() if name != "all_input_tokens"
            }
        return value

    @pydantic.computed_field
    def all_input_tokens(self) -> int:
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens


class WorkflowUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    own: Totals = pydantic.Field(default_factory=Totals)
    subtree: Cost = pydantic.Field(default_factory=Cost)


class Context(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    step_id: str
    input_tokens: int = pydantic.Field(ge=0)
    output_tokens: int = pydantic.Field(ge=0)
    catalog_model: Optional[str] = None
    context_window: Optional[int] = None


class ExecutionUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    totals: Optional[Totals] = None
    root_context: Optional[Context] = None

    @classmethod
    def confirmed(cls, totals: Totals, context: Optional[Context]) -> Self:
        return cls(
            totals=totals if totals.confirmed_calls else None,
            root_context=context,
        )


class RunnerUsage(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    totals: Totals = pydantic.Field(default_factory=Totals)
    reset_at: Optional[str] = None
    generation: int = pydantic.Field(default=0, ge=0)
