"""Non-secret MCP configuration and versioned discovery snapshots."""

import hashlib
import json
import re
import typing
from pathlib import Path
from typing import Annotated, Literal, Optional
from urllib.parse import urlsplit

import pydantic
from vectorops.runner.authentication.mcp_settings import Settings as OAuthSettings
from vectorops.runner.authentication import mcp_settings


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class Limits(Model):
    startup_seconds: float = pydantic.Field(
        default=15, gt=0, le=300, allow_inf_nan=False
    )
    request_seconds: float = pydantic.Field(
        default=30, gt=0, le=3600, allow_inf_nan=False
    )
    shutdown_seconds: float = pydantic.Field(
        default=10, gt=0, le=60, allow_inf_nan=False
    )
    max_bytes: int = pydantic.Field(default=1048576, ge=1024, le=16777216)
    max_tools: int = pydantic.Field(default=256, ge=1, le=4096)
    max_pages: int = pydantic.Field(default=32, ge=1, le=256)


class Root(Model):
    uri: str
    name: Optional[str] = None

    @pydantic.model_validator(mode="before")
    @classmethod
    def from_path(cls, value: object) -> object:
        if isinstance(value, dict) and "path" in value:
            copied = dict(value)
            path = copied.pop("path")
            if (
                "uri" in copied
                or not isinstance(path, str)
                or not Path(path).is_absolute()
            ):
                raise ValueError("root requires exactly one absolute path or URI")
            copied["uri"] = Path(path).as_uri()
            return copied
        return value

    @pydantic.field_validator("uri")
    @classmethod
    def file_uri(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "file" or not parsed.path.startswith("/"):
            raise ValueError("MCP roots must be absolute file URIs")
        if parsed.netloc not in ("", "localhost") or parsed.query or parsed.fragment:
            raise ValueError("MCP roots must reference local files")
        return value


class Server(Model):
    limits: Limits = pydantic.Field(default_factory=Limits)
    roots: list[Root] = pydantic.Field(default_factory=list)
    roots_mode: Literal["replace", "merge"] = pydantic.Field(
        default="replace", exclude_if=lambda value: value == "replace"
    )


class StdioServer(Server):
    kind: Literal["stdio"] = "stdio"
    command: str = pydantic.Field(min_length=1)
    args: list[str] = pydantic.Field(default_factory=list)
    cwd: Optional[str] = None
    env_credentials: dict[str, str] = pydantic.Field(default_factory=dict)
    env: dict[str, str] = pydantic.Field(
        default_factory=dict, exclude_if=lambda value: not value
    )

    @pydantic.field_validator("env")
    @classmethod
    def environment_values(cls, value: dict[str, str]) -> dict[str, str]:
        for name, text in value.items():
            if any(
                token in name.upper()
                for token in ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "PRIVATE_KEY")
            ):
                raise ValueError(
                    "secret environment variables require credential references"
                )
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or "\x00" in text:
                raise ValueError("invalid environment setting")
        return value

    @pydantic.field_validator("cwd")
    @classmethod
    def absolute_cwd(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.startswith("/"):
            raise ValueError("MCP cwd must be absolute")
        return value

    @pydantic.field_validator("env_credentials")
    @classmethod
    def environment_refs(cls, value: dict[str, str]) -> dict[str, str]:
        for name, reference in value.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                raise ValueError("invalid environment variable name")
            validate_reference(reference)
        return value


class HTTPServer(Server):
    kind: Literal["http"] = "http"
    url: str
    credential: Optional[str] = None
    oauth: Optional[OAuthSettings] = pydantic.Field(
        default=None, exclude_if=lambda value: value is None
    )
    auth: Optional[mcp_settings.Authentication] = pydantic.Field(
        default=None, exclude_if=lambda value: value is None
    )
    headers: dict[str, str] = pydantic.Field(
        default_factory=dict, exclude_if=lambda value: not value
    )
    header_credentials: dict[str, str] = pydantic.Field(
        default_factory=dict, exclude_if=lambda value: not value
    )

    @pydantic.model_validator(mode="after")
    def authentication_mode(self) -> "HTTPServer":
        if self.auth is not None and (
            self.oauth is not None or self.credential is not None
        ):
            raise ValueError("auth cannot be combined with legacy credential/oauth")
        if (
            isinstance(
                self.auth,
                (mcp_settings.AuthorizationCode, mcp_settings.ClientCredentials),
            )
            and self.auth.resource != self.url
        ):
            raise ValueError("authentication resource must equal the MCP URL")
        seen: set[str] = set()
        for values, secret in ((self.headers, False), (self.header_credentials, True)):
            for name, value in values.items():
                key = name.lower()
                if (
                    not re.fullmatch(r"[A-Za-z0-9-]+", name)
                    or key in seen
                    or key
                    in {
                        "host",
                        "content-length",
                        "transfer-encoding",
                        "connection",
                        "content-type",
                        "accept",
                        "accept-encoding",
                        "mcp-session-id",
                        "mcp-protocol-version",
                    }
                ):
                    raise ValueError("duplicate or reserved HTTP header")
                if not secret and key in {
                    "authorization",
                    "proxy-authorization",
                    "cookie",
                    "x-api-key",
                    "api-key",
                }:
                    raise ValueError("secret headers require credential references")
                if any(ord(char) < 32 or ord(char) > 126 for char in value):
                    raise ValueError("invalid HTTP header value")
                if secret:
                    validate_reference(value)
                seen.add(key)
        auth_header = (
            self.auth.header.lower()
            if isinstance(self.auth, mcp_settings.HeaderAuth)
            else "authorization"
        )
        if (
            self.credential
            or self.oauth
            or self.auth is not None
            and not isinstance(self.auth, mcp_settings.NoAuth)
        ) and auth_header in seen:
            raise ValueError("authentication header conflicts with configured headers")
        if self.oauth is not None:
            if self.credential is not None:
                raise ValueError(
                    "MCP HTTP accepts either OAuth or an API key reference"
                )
            if self.oauth.resource != self.url:
                raise ValueError(
                    "MCP OAuth resource must equal the configured endpoint"
                )
        return self

    @pydantic.field_validator("url")
    @classmethod
    def endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("MCP URL requires a hostname and no inline credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("MCP URL must not contain a query or fragment")
        if parsed.scheme != "https" and not (
            parsed.scheme == "http"
            and parsed.hostname in ("localhost", "127.0.0.1", "::1")
        ):
            raise ValueError("MCP HTTP requires HTTPS except on loopback")
        return value

    def authentication_settings(
        self,
    ) -> (
        mcp_settings.NoAuth
        | mcp_settings.HeaderAuth
        | mcp_settings.Settings
        | mcp_settings.ClientCredentials
    ):
        return mcp_settings.selected(self.auth, self.oauth, self.credential)

    @pydantic.field_validator("credential")
    @classmethod
    def credential_reference(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            validate_reference(value)
        return value


ServerConfig = Annotated[StdioServer | HTTPServer, pydantic.Field(discriminator="kind")]


def validate_reference(value: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValueError("invalid MCP credential profile reference")


class ServerTools(Model):
    enabled: bool = True
    tools: list[str] = pydantic.Field(default_factory=lambda: ["*"])
    exclude: list[str] = pydantic.Field(default_factory=list)

    def allows(self, name: str) -> bool:
        return self.enabled and Selection(
            server="", tools=self.tools, exclude=self.exclude
        ).allows(name)


class Settings(Model):
    configuration_contract: typing.ClassVar[str] = (
        "e27b17f60c6866b249b37b466f640e1123e34cfe37c0fc41eb7103c36cbd58e3"
    )
    servers: dict[str, ServerConfig] = pydantic.Field(default_factory=dict)
    roots: list[Root] = pydantic.Field(
        default_factory=list, exclude_if=lambda value: not value
    )
    mode: Literal["static", "dynamic"] = "static"
    max_active: int = pydantic.Field(
        default=32, ge=1, le=256, strict=True, exclude_if=lambda value: value == 32
    )
    server_tools: dict[str, ServerTools] = pydantic.Field(
        default_factory=dict, exclude_if=lambda value: not value
    )

    @pydantic.model_validator(mode="before")
    @classmethod
    def separate_tool_settings(cls, value: object) -> object:
        """Keep exposure policy out of transport identities and recorded catalogs."""
        if not isinstance(value, dict) or not isinstance(value.get("servers"), dict):
            return value
        result = dict(value)
        servers = dict(value["servers"])
        policies = dict(value.get("server_tools", {}))
        for name, server in servers.items():
            if not isinstance(server, dict):
                continue
            connection = dict(server)
            policy = {
                key: connection.pop(key)
                for key in ("enabled", "tools", "exclude")
                if key in connection
            }
            if policy:
                if name in policies:
                    raise ValueError("duplicate MCP server tool settings")
                policies[name] = policy
            servers[name] = connection
        return {**result, "servers": servers, "server_tools": policies}

    def selections(self) -> list["Selection"]:
        return [
            Selection(server=name, tools=policy.tools, exclude=policy.exclude)
            for name in sorted(self.servers)
            if (policy := self.tool_settings(name)).enabled
        ]

    def tool_settings(self, name: str) -> ServerTools:
        return self.server_tools.get(name, ServerTools())

    @pydantic.model_validator(mode="after")
    def resolve_roots(self) -> "Settings":
        if set(self.server_tools) - set(self.servers):
            raise ValueError("MCP tool settings reference an unknown server")
        for name in self.servers:
            self.servers[name] = self.server(name)
        return self

    def server(self, name: str) -> ServerConfig:
        selected = self.servers[name]
        if not self.roots:
            return selected.model_copy(deep=True)
        roots = selected.roots
        if "roots" not in selected.model_fields_set or selected.roots_mode == "merge":
            roots = list({root.uri: root for root in [*self.roots, *roots]}.values())
        return selected.model_copy(update={"roots": roots}, deep=True)

    @pydantic.field_validator("servers")
    @classmethod
    def server_names(cls, value: dict[str, ServerConfig]) -> dict[str, ServerConfig]:
        for name in value:
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name):
                raise ValueError("invalid MCP server name")
        return value


class Selection(Model):
    server: str
    tools: list[str] = pydantic.Field(default_factory=lambda: ["*"])
    exclude: list[str] = pydantic.Field(default_factory=list)

    def allows(self, name: str) -> bool:
        return (
            "*" not in self.exclude
            and name not in self.exclude
            and ("*" in self.tools or name in self.tools)
        )


class Descriptor(Model):
    remote_name: str = pydantic.Field(min_length=1)
    name: str
    description: str = ""
    input_schema: dict[str, pydantic.JsonValue]
    output_schema: Optional[dict[str, pydantic.JsonValue]] = None
    title: Optional[str] = pydantic.Field(
        default=None, exclude_if=lambda value: value is None
    )


class Catalog(Model):
    version: Literal[1] = 1
    server_name: str
    server: ServerConfig
    protocol_version: str
    tools: list[Descriptor]


def digest(value: pydantic.JsonValue) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def tool_name(server_name: str, server: ServerConfig, remote_name: str) -> str:
    identity: dict[str, pydantic.JsonValue] = {
        "server": server.model_dump(mode="json"),
        "alias": server_name,
        "tool": remote_name,
    }
    label = re.sub(r"[^A-Za-z0-9_]", "_", remote_name)[:20]
    return f"mcp_{server_name[:12]}_{label}_{digest(identity)[:20]}"
