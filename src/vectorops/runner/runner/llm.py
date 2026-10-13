"""One-shot runner LLM calls without tools, workflow activities, or replay state."""

from collections.abc import Callable
import typing
from typing import Optional

import connect

from vectorops.runner.api.llm import configuration, models
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.project import Configuration


class RunnerLLM:
    def __init__(
        self,
        configuration_source: Configuration,
        authentication: AuthenticationService,
        *,
        client_factory: Callable[[], connect.AsyncLLMClient] = connect.AsyncLLMClient,
    ) -> None:
        self.configuration = configuration_source
        self.authentication = authentication
        self.client_factory = client_factory

    async def generate(
        self,
        message: connect.UserMessage,
        *,
        system_prompt: Optional[str] = None,
        profile: str = "weak",
        **overrides: typing.Unpack[models.SettingsParameters],
    ) -> connect.AssistantMessage:
        """Resolve current workspace policy and make a single unrecorded request."""
        settings = configuration.interpret_configuration(
            self.configuration.get("llm", {}),
            configuration.explicit_options(**overrides),
            profile=profile,
        ).generation_settings()
        auth = await self.authentication.transport(
            settings.provider, settings.model, settings.credential_profile
        )
        request = connect.GenerateRequest(
            messages=[message],
            system_prompt=system_prompt,
            temperature=settings.temperature,
            max_output_tokens=settings.max_output_tokens,
            reasoning=settings.reasoning,
        )
        async with self.client_factory() as client:
            response = await client.generate(
                settings.model,
                request,
                provider=settings.provider,
                options=connect.RequestOptions(timeout=settings.timeout, auth=auth),
            )
        if response.finish_reason != "stop" or any(
            block.type == "tool_call" for block in response.content
        ):
            raise models.LLMResponseError(
                f"unsuccessful runner LLM response: {response.finish_reason}"
            )
        return response
