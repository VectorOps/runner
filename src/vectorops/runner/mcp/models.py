"""Non-secret MCP configuration and versioned discovery snapshots."""

import hashlib
import json
import re
from typing import Annotated, Literal, Optional
from urllib.parse import urlsplit

import pydantic
from vectorops.runner.authentication.mcp_oauth import Settings as OAuthSettings


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


class StdioServer(Server):
    kind: Literal["stdio"] = "stdio"
    command: str = pydantic.Field(min_length=1)
    args: list[str] = pydantic.Field(default_factory=list)
    cwd: Optional[str] = None
    env_credentials: dict[str, str] = pydantic.Field(default_factory=dict)

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

    @pydantic.model_validator(mode="after")
    def authentication_mode(self) -> "HTTPServer":
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


class Settings(Model):
    servers: dict[str, ServerConfig] = pydantic.Field(default_factory=dict)

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
