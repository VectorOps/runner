"""Standard workflow defaults from components.workflows.defaults."""

from enum import Enum
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api import configuration, llm
from vectorops.runner.references import WorkflowReference


class ModelTier(str, Enum):
    STRONG = "strong"
    MEDIUM = "medium"
    WEAK = "weak"


ThinkingLevel = Literal["none", "minimal", "low", "medium", "high", "xhigh"]


class Settings(pydantic.BaseModel):
    """Unset options inherit components.llm rather than choosing a provider."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    thinking_level: Optional[ThinkingLevel] = None
    strong_model: Optional[str] = None
    medium_model: Optional[str] = None
    weak_model: Optional[str] = None

    @pydantic.field_validator(
        "strong_model", "medium_model", "weak_model", mode="after"
    )
    @classmethod
    def nonblank_model(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.strip():
            raise ValueError("model must not be blank")
        return value

    def llm_overrides(self, tier: ModelTier) -> llm.SettingsOverrides:
        """Translate a tier and shared thinking level into explicit LLM options."""
        model = {
            ModelTier.STRONG: self.strong_model,
            ModelTier.MEDIUM: self.medium_model,
            ModelTier.WEAK: self.weak_model,
        }[ModelTier(tier)]
        values: dict[str, pydantic.JsonValue] = {}
        if model is not None:
            values["model"] = model
        if self.thinking_level is not None:
            values["reasoning"] = connect.ReasoningConfig(
                effort=self.thinking_level
            ).model_dump(mode="json", exclude_unset=True)
        return llm.SettingsOverrides.model_validate(values)


async def load_settings() -> Settings:
    """Read a validated snapshot that remains stable when the workflow replays."""
    values = await configuration.get_validated("workflows.defaults", Settings)
    return Settings.model_validate(values)


class AgentSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    agents: list[str] = pydantic.Field(default_factory=lambda: ["builtin:helper"])
    tool_collection: str = pydantic.Field(
        default="agent", pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$"
    )

    @pydantic.field_validator("agents", mode="after")
    @classmethod
    def validate_agents(cls, values: list[str]) -> list[str]:
        references = [str(WorkflowReference.parse(value)) for value in values]
        if len(set(references)) != len(references):
            raise ValueError("agent references must be unique")
        return references


class HelperSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    thinking_level: Optional[ThinkingLevel] = None
    temperature: Optional[float] = pydantic.Field(default=None, ge=0, le=2)
    max_rounds: int = pydantic.Field(default=30, gt=0, strict=True)
    tool_collection: str = pydantic.Field(
        default="helper", pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$"
    )

    def llm_overrides(self, defaults: Settings) -> llm.SettingsOverrides:
        values = defaults.llm_overrides(ModelTier.WEAK).model_dump(exclude_unset=True)
        if self.thinking_level is not None:
            values["reasoning"] = {"effort": self.thinking_level}
        if self.temperature is not None:
            values["temperature"] = self.temperature
        return llm.SettingsOverrides.model_validate(values)


async def load_agent_settings() -> AgentSettings:
    values = await configuration.get_validated("workflows.agent", AgentSettings)
    return AgentSettings.model_validate(values)


async def load_helper_settings() -> HelperSettings:
    values = await configuration.get_validated("workflows.helper", HelperSettings)
    return HelperSettings.model_validate(values)
