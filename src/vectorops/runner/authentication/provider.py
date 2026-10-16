"""Provider selection and connect transport authentication integration."""

from collections.abc import Mapping
from typing import Final, Optional

import connect

from vectorops.runner import model_reference

from . import contracts, models, oauth

_PROVIDER_NAMES: Final[dict[str, str]] = {
    "anthropic": "Anthropic",
    "gemini": "Google Gemini",
    "mcp": "MCP",
    "openai": "OpenAI",
    "openrouter": "OpenRouter",
}


def available(
    adapters: Mapping[str, oauth.OAuthAdapter],
) -> models.AuthenticationProviders:
    """Describe credential capabilities, not model availability or stored authorization."""
    names = dict(_PROVIDER_NAMES)
    names.update(
        {
            name: "ChatGPT" if name == "chatgpt" else name
            for name in adapters
            if not name.startswith("mcp-")
        }
    )
    return models.AuthenticationProviders(
        providers=[
            models.AuthenticationProvider(
                provider=name,
                display_name=names[name],
                credential_kinds=(
                    ["api_key", "oauth2"] if name in adapters else ["api_key"]
                ),
                oauth_login=name in adapters,
            )
            for name in sorted(names)
        ]
    )


def adapters() -> dict[str, oauth.OAuthAdapter]:
    return {"chatgpt": oauth.ChatGPT()}


async def transport(
    resolver: contracts.CredentialResolver,
    provider: Optional[str],
    model: str,
    profile: str = "default",
    execution_id: Optional[str] = None,
) -> "ProviderAuth":
    try:
        name = model_reference.provider(model, provider)
    except ValueError as exc:
        raise models.AuthenticationError(str(exc)) from None
    if name is None:
        raise models.AuthenticationError(
            "set model to <provider>/<model>, or supply a legacy provider setting"
        )
    selected = models.Profile(provider=name, profile=profile)
    value = await resolver.acquire(selected, execution_id)
    return ProviderAuth(resolver, selected, execution_id, value)


class ProviderAuth:
    def __init__(
        self,
        service: contracts.CredentialResolver,
        profile: models.Profile,
        execution_id: Optional[str],
        value: models.Credential,
    ) -> None:
        self.service, self.profile, self.execution_id, self.value = (
            service,
            profile,
            execution_id,
            value,
        )

    async def resolve(
        self, context: Optional[connect.AuthContext] = None
    ) -> connect.ResolvedAuth:
        value = self.value
        if value.kind == "oauth2":
            strategy: connect.TransportAuth = connect.ChatGPTAccessTokenAuth(
                value.secret, account_id=value.account_id
            )
        elif value.provider == "anthropic":
            strategy = connect.HeaderAPIKeyAuth(
                value.secret, header_name="x-api-key", prefix=""
            )
        elif value.provider == "gemini":
            strategy = connect.HeaderAPIKeyAuth(
                value.secret, header_name="x-goog-api-key", prefix=""
            )
        else:
            strategy = connect.BearerTokenAuth(value.secret)
        return await strategy.resolve(context)

    async def refresh(self, context: Optional[connect.AuthContext] = None) -> bool:
        if self.value.kind == "api_key":
            return False
        self.value = await self.service.acquire(
            self.profile, self.execution_id, rejected=self.value.secret
        )
        return True
