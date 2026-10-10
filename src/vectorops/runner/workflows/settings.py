"""Standard workflow defaults from components.workflows.defaults."""

from enum import Enum
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api import configuration, llm


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
