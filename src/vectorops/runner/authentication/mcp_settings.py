"""Non-secret MCP authentication strategies; secret values are store references."""

from typing import Annotated, Literal

import pydantic

from . import mcp_oauth


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class NoAuth(Model):
    kind: Literal["none"] = "none"


class HeaderAuth(Model):
    kind: Literal["header"] = "header"
    credential: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    header: str = pydantic.Field(default="Authorization", pattern=r"^[A-Za-z0-9-]+$")
    prefix: str = "Bearer "

    @pydantic.model_validator(mode="after")
    def safe_header(self) -> "HeaderAuth":
        if self.header.lower() in {
            "host",
            "content-length",
            "content-type",
            "transfer-encoding",
            "connection",
            "accept",
            "accept-encoding",
            "mcp-session-id",
            "mcp-protocol-version",
        }:
            raise ValueError("authentication cannot override protocol headers")
        if any(ord(char) < 32 or ord(char) > 126 for char in self.prefix):
            raise ValueError("invalid authentication prefix")
        return self


class AuthorizationCode(mcp_oauth.Settings):
    kind: Literal["authorization_code"] = "authorization_code"


class ClientCredentials(Model):
    kind: Literal["client_credentials"] = "client_credentials"
    token_endpoint: str
    resource: str
    client_id: str = pydantic.Field(min_length=1, max_length=1024)
    client_secret: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    token_auth_method: Literal["client_secret_basic", "client_secret_post"] = (
        "client_secret_basic"
    )
    scopes: list[str] = pydantic.Field(default_factory=list, max_length=64)
    profile: str = pydantic.Field(default="default", pattern=r"^[A-Za-z0-9_-]{1,64}$")

    @pydantic.field_validator("token_endpoint", "resource")
    @classmethod
    def url(cls, value: str) -> str:
        return mcp_oauth.endpoint(value)

    @pydantic.field_validator("scopes")
    @classmethod
    def scope_tokens(cls, values: list[str]) -> list[str]:
        return mcp_oauth.Settings.scope_tokens(values)


Authentication = Annotated[
    NoAuth | HeaderAuth | AuthorizationCode | ClientCredentials,
    pydantic.Field(discriminator="kind"),
]
