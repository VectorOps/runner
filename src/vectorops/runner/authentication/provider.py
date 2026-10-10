"""Provider selection and connect transport authentication integration."""

from typing import Optional

import connect

from . import contracts, models, oauth


def adapters() -> dict[str, oauth.OAuthAdapter]:
    return {"chatgpt": oauth.ChatGPT()}


async def transport(
    resolver: contracts.CredentialResolver,
    provider: Optional[str],
    model: str,
    profile: str = "default",
    execution_id: Optional[str] = None,
) -> "ProviderAuth":
    name = provider or model.partition("/")[0]
    if not provider and "/" not in model:
        raise models.AuthenticationError(
            "provider must be explicit or included in the model name"
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
