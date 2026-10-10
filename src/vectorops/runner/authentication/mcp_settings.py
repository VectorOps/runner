"""Non-secret MCP authentication strategies; secret values are store references."""

from typing import Annotated, Literal, Optional
from urllib.parse import urlsplit

import pydantic


def endpoint(value: str) -> str:
    parsed = urlsplit(value)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "OAuth endpoints must be absolute URLs without credentials, query, or fragment"
        )
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost", "::1")
    ):
        raise ValueError("OAuth endpoints require HTTPS except on loopback")
    if parsed.port == 0:
        raise ValueError("OAuth endpoint port must be nonzero")
    return value


def scope_tokens(values: list[str]) -> list[str]:
    for value in values:
        if not value or any(
            ord(char) < 33 or ord(char) > 126 or char in ('"', "\\") for char in value
        ):
            raise ValueError("OAuth scopes must be valid scope tokens")
    return sorted(set(values))


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class NoAuth(Model):
    kind: Literal["none"] = "none"


class Settings(Model):
    issuer: str
    resource: str
    client_id: str = pydantic.Field(min_length=1, max_length=1024)
    redirect_uri: str
    scopes: list[str] = pydantic.Field(default_factory=list, max_length=64)
    profile: str = pydantic.Field(default="default", pattern=r"^[A-Za-z0-9_-]{1,64}$")

    @pydantic.field_validator("issuer", "resource", "redirect_uri")
    @classmethod
    def valid_url(cls, value: str) -> str:
        return endpoint(value)

    @pydantic.field_validator("redirect_uri")
    @classmethod
    def loopback(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in ("127.0.0.1", "localhost", "::1")
            or not parsed.port
            or not parsed.path
        ):
            raise ValueError(
                "OAuth callback requires a loopback HTTP URL with explicit port and path"
            )
        return value

    @pydantic.field_validator("scopes")
    @classmethod
    def scope_tokens(cls, values: list[str]) -> list[str]:
        return scope_tokens(values)


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


class AuthorizationCode(Settings):
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
        return endpoint(value)

    @pydantic.field_validator("scopes")
    @classmethod
    def scope_tokens(cls, values: list[str]) -> list[str]:
        return scope_tokens(values)


Authentication = Annotated[
    NoAuth | HeaderAuth | AuthorizationCode | ClientCredentials,
    pydantic.Field(discriminator="kind"),
]


def selected(
    auth: Optional[Authentication],
    oauth: Optional[Settings],
    credential: Optional[str],
) -> NoAuth | HeaderAuth | Settings | ClientCredentials:
    if auth is not None:
        return auth
    if oauth is not None:
        return oauth
    if credential is not None:
        return HeaderAuth(credential=credential)
    return NoAuth()
