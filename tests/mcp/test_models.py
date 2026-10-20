import base64

import connect
from mcp import types
import pydantic
import pytest

from vectorops.runner.mcp import models, schema
from vectorops.runner.mcp.binding import MCPTool, convert
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.api.llm.tools import ToolExecutionError


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/mcp",
        "https://user:secret@example.com/mcp",
        "https://example.com/mcp?token=secret",
        "https://example.com/mcp#secret",
    ],
)
def test_reject_secret_and_insecure_urls(url: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        models.HTTPServer(url=url)


def test_settings_forbid_inline_secrets_and_invalid_deadlines() -> None:
    with pytest.raises(pydantic.ValidationError):
        models.Settings.model_validate(
            {
                "servers": {
                    "s": {
                        "kind": "http",
                        "url": "https://example.com/mcp",
                        "headers": {"Authorization": "secret"},
                    }
                }
            }
        )
    for value in (0, -1, float("inf"), float("nan")):
        with pytest.raises(pydantic.ValidationError):
            models.Limits(request_seconds=value)


def test_names_bind_server_identity_and_avoid_normalization_collisions() -> None:
    server = models.HTTPServer(url="https://example.com/mcp")
    first = models.tool_name("a-b", server, "read")
    assert first != models.tool_name("a_b", server, "read")
    assert first != models.tool_name(
        "a-b", models.HTTPServer(url="https://other.test/mcp"), "read"
    )
    assert len(models.tool_name("a" * 64, server, "b" * 200)) <= 64


def test_schema_local_refs_work_and_remote_refs_fail_closed() -> None:
    local = schema.validator(
        {
            "type": "object",
            "$defs": {"value": {"type": "integer"}},
            "properties": {"value": {"$ref": "#/$defs/value"}},
        }
    )
    local.validate({"value": 1})
    remote = schema.validator(
        {
            "type": "object",
            "properties": {"value": {"$ref": "https://example.com/schema"}},
        }
    )
    with pytest.raises(Exception):
        remote.validate({"value": 1})


def test_conversion_preserves_images_structured_content_errors_and_fallbacks() -> None:
    response = convert(
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="hello"),
                types.ImageContent(
                    type="image",
                    mimeType="image/png",
                    data=base64.b64encode(b"image").decode(),
                ),
                types.AudioContent(type="audio", mimeType="audio/wav", data="YQ=="),
            ],
            structuredContent={"value": 1},
            isError=True,
        )
    )
    assert response.is_error
    assert isinstance(response.content[1], connect.ImageBlock)
    assert isinstance(response.content[2], connect.TextBlock)
    assert '"audio"' in response.content[2].text
    assert isinstance(response.content[3], connect.TextBlock)
    assert '"structuredContent"' in response.content[3].text
    with pytest.raises(MCPError, match="base64"):
        convert(
            types.CallToolResult(
                content=[
                    types.ImageContent(type="image", mimeType="image/png", data="?")
                ]
            )
        )


def test_binding_detaches_schema_and_validates_without_io() -> None:
    server = models.StdioServer(command="unused")
    descriptor = models.Descriptor(
        remote_name="test",
        name=models.tool_name("test", server, "test"),
        input_schema={"type": "object", "required": ["value"]},
    )
    catalog = models.Catalog(
        server_name="test",
        server=server,
        protocol_version="2025-03-26",
        tools=[descriptor],
    )
    binding = MCPTool(catalog, descriptor)
    descriptor.input_schema.clear()
    binding.definition.input_schema.clear()
    with pytest.raises(ToolExecutionError):
        binding.validate({})
    binding.validate({"value": 1})
    assert not models.Selection(server="test", tools=[]).allows("test")
    assert not models.Selection(server="test", exclude=["test"]).allows("test")


def test_schema_depth_limit() -> None:
    nested: dict[str, pydantic.JsonValue] = {"type": "object"}
    for _ in range(40):
        nested = {"type": "object", "properties": {"next": nested}}
    with pytest.raises(ValueError, match="complexity"):
        schema.validator(nested)


def test_exposure_policy_is_separate_from_transport_identity() -> None:
    connection = models.StdioServer(command="unused")
    settings = models.Settings.model_validate(
        {
            "mode": "static",
            "max_active": 3,
            "servers": {
                "test": {
                    **connection.model_dump(mode="json"),
                    "tools": ["echo", "delete"],
                    "exclude": ["delete"],
                },
                "disabled": {
                    "kind": "stdio",
                    "command": "unused",
                    "enabled": False,
                },
            },
        }
    )
    assert settings.server("test") == connection
    assert models.tool_name(
        "test", settings.server("test"), "echo"
    ) == models.tool_name("test", connection, "echo")
    assert settings.selections() == [
        models.Selection(server="test", tools=["echo", "delete"], exclude=["delete"])
    ]
    assert settings.tool_settings("test").allows("echo")
    assert not settings.tool_settings("test").allows("delete")
    assert not settings.tool_settings("test").allows("other")
    assert models.Settings.model_validate(settings.model_dump(mode="json")) == settings
    assert settings.mode == "static"
    assert settings.max_active == 3
    assert models.Settings().mode == "static"
    assert models.Settings().selections() == []
    assert models.Settings().model_dump(mode="json") == {
        "servers": {},
        "mode": "static",
    }


@pytest.mark.parametrize("mode", ["static", "dynamic"])
def test_exposure_mode_is_explicit_in_snapshots(mode: str) -> None:
    settings = models.Settings.model_validate({"mode": mode})
    snapshot = settings.model_dump(mode="json")
    assert snapshot["mode"] == mode
    assert models.Settings.model_validate(snapshot) == settings


@pytest.mark.parametrize(
    "value",
    [
        {"mode": "automatic"},
        {"max_active": 0},
        {"max_active": 257},
        {"max_active": True},
        {"servers": {"s": {"kind": "stdio", "command": "unused", "tools": "*"}}},
        {"servers": {"s": {"kind": "stdio", "command": "unused", "enabled": "bad"}}},
        {"server_tools": {"missing": {}}},
    ],
)
def test_invalid_exposure_settings(value: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        models.Settings.model_validate(value)
