"""Bounded HTTP requests with text normalization and binary-safe results."""

import base64
import binascii
from html.parser import HTMLParser
import json
import typing
from typing import Annotated, Literal, Optional
from urllib.parse import urlsplit

import aiohttp
import connect
import pydantic

from vectorops.runner.runner.activity_context import ActivityExecutionContext
from . import tools


class TextBody(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    kind: Literal["text"] = "text"
    content_type: str = "text/plain; charset=utf-8"
    text: str = pydantic.Field(max_length=1048576)


class BinaryBody(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    kind: Literal["binary"] = "binary"
    content_type: str = "application/octet-stream"
    data_base64: str = pydantic.Field(max_length=1398104)

    @pydantic.field_validator("data_base64")
    @classmethod
    def validate_base64(cls, value: str) -> str:
        try:
            base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("data_base64 must be valid base64") from exc
        return value


class WebFetchArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    url: str
    method: Literal["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"] = "GET"
    headers: dict[str, str] = pydantic.Field(default_factory=dict)
    timeout_s: float = pydantic.Field(default=30, gt=0, le=120, allow_inf_nan=False)
    max_bytes: int = pydantic.Field(default=65536, gt=0, le=1048576)
    body: Optional[
        Annotated[
            typing.Union[TextBody, BinaryBody], pydantic.Field(discriminator="kind")
        ]
    ] = None

    @pydantic.field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("URL must use HTTP or HTTPS and include a host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("URL credentials are not supported")
        return value

    @pydantic.model_validator(mode="after")
    def validate_body(self) -> "WebFetchArguments":
        if self.body is not None and self.method in ("GET", "HEAD"):
            raise ValueError("GET and HEAD cannot have a request body")
        return self


class _HTMLText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag in ("script", "style"):
            self.hidden += 1
        if not self.hidden and tag in ("p", "div", "br", "li", "h1", "h2", "h3", "tr"):
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden - 1)
        if not self.hidden and tag in ("p", "div", "li", "h1", "h2", "h3", "tr"):
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)

    def text(self) -> str:
        return "\n".join(
            line
            for part in "".join(self.parts).splitlines()
            if (line := " ".join(part.split()))
        )


def web_fetch_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[WebFetchArguments]:
    async def execute(
        context: ActivityExecutionContext, arguments: WebFetchArguments
    ) -> tools.ToolResponse:
        headers = dict(arguments.headers)
        data: Optional[bytes] = None
        if arguments.body is not None:
            if not any(key.lower() == "content-type" for key in headers):
                headers["Content-Type"] = arguments.body.content_type
            data = (
                arguments.body.text.encode("utf-8")
                if isinstance(arguments.body, TextBody)
                else base64.b64decode(arguments.body.data_base64)
            )
            if len(data) > 1048576:
                raise tools.ToolExecutionError("Request body exceeds 1 MiB")
        timeout = aiohttp.ClientTimeout(total=arguments.timeout_s)
        async with aiohttp.ClientSession(
            timeout=timeout, cookie_jar=aiohttp.DummyCookieJar(), trust_env=False
        ) as session:
            async with session.request(
                arguments.method,
                arguments.url,
                headers=headers,
                data=data,
                allow_redirects=False,
            ) as response:
                content = bytearray()
                async for chunk in response.content.iter_chunked(
                    min(65536, arguments.max_bytes + 1)
                ):
                    remaining = arguments.max_bytes + 1 - len(content)
                    content.extend(chunk[:remaining])
                    if len(content) > arguments.max_bytes:
                        break
                truncated = len(content) > arguments.max_bytes
                raw = bytes(content[: arguments.max_bytes])
                mime = response.content_type
                payload: dict[str, pydantic.JsonValue] = {
                    "url": str(response.url),
                    "status": response.status,
                    "content_type": mime,
                    "bytes_read": len(raw),
                    "truncated": truncated,
                }
                location = response.headers.get("Location")
                if location is not None:
                    payload["location"] = location
                text_type = (
                    mime.startswith("text/")
                    or mime
                    in ("application/json", "application/xml", "application/xhtml+xml")
                    or mime.endswith(("+json", "+xml"))
                )
                if text_type:
                    try:
                        text = raw.decode(response.charset or "utf-8", errors="replace")
                    except LookupError as exc:
                        raise tools.ToolExecutionError(
                            "Unsupported response charset"
                        ) from exc
                    if mime in ("text/html", "application/xhtml+xml"):
                        parser = _HTMLText()
                        parser.feed(text)
                        text = parser.text()
                    payload.update({"encoding": "utf-8", "content": text})
                    blocks: list[connect.ToolResultContentBlock] = [
                        connect.TextBlock(text=json.dumps(payload))
                    ]
                elif (
                    mime in ("image/png", "image/jpeg", "image/gif", "image/webp")
                    and raw
                    and not truncated
                ):
                    blocks = [
                        connect.TextBlock(text=json.dumps(payload)),
                        connect.ImageBlock(
                            mime_type=mime, data=base64.b64encode(raw).decode("ascii")
                        ),
                    ]
                else:
                    payload.update(
                        {
                            "encoding": "base64",
                            "content": base64.b64encode(raw).decode("ascii"),
                        }
                    )
                    blocks = [connect.TextBlock(text=json.dumps(payload))]
                return tools.ToolResponse(
                    content=blocks, is_error=response.status >= 400
                )

    return tools.Tool(
        "web_fetch",
        "Send an HTTP(S) request from the runner host. Returns status, bounded content (default 64 KiB, "
        "maximum 1 MiB), and truncation metadata. HTML becomes plain text; supported images become image "
        "blocks; other binary content is base64. Redirects are returned with location, not followed. "
        "Uses the host network, including private addresses; subject to tool approval, not a network sandbox. "
        "Do not pass secrets in arguments: tool calls are recorded in workflow history.",
        WebFetchArguments,
        execute,
        approval=approval,
    )
