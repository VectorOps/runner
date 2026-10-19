"""Bounded HTTP requests with redirects and ranges over converted content."""

import asyncio
import base64
import binascii
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import hashlib
import json
import typing
from typing import Annotated, Literal, Optional
from urllib.parse import urljoin, urlsplit

import aiohttp
import connect
import pydantic

from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.api import tool_contracts as tools

from .web_content import convert_html

MAX_DOWNLOAD_BYTES: typing.Final = 16 * 1024 * 1024
MAX_REDIRECTS: typing.Final = 10


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
    max_bytes: int = pydantic.Field(
        default=65536,
        gt=0,
        le=1048576,
        description="Maximum returned bytes after HTML conversion (before base64 for binary).",
    )
    offset: int = pydantic.Field(
        default=0,
        ge=0,
        description="Byte offset in converted UTF-8 text or binary content; use next_offset to continue.",
    )
    format: Literal["markdown", "text", "raw"] = pydantic.Field(
        default="markdown",
        description="HTML output format; other text is returned unchanged.",
    )
    main_content_only: bool = pydantic.Field(
        default=True,
        description="Prefer main/article HTML and remove navigation; ignored for raw output.",
    )
    follow_redirects: bool = True
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
        parsed.port
        return value

    @pydantic.model_validator(mode="after")
    def validate_body(self) -> "WebFetchArguments":
        if self.body is not None and self.method in ("GET", "HEAD"):
            raise ValueError("GET and HEAD cannot have a request body")
        return self


def _origin(url: str) -> tuple[str, Optional[str], int]:
    parsed = urlsplit(url)
    return (
        parsed.scheme,
        parsed.hostname,
        (
            parsed.port
            if parsed.port is not None
            else (443 if parsed.scheme == "https" else 80)
        ),
    )


@asynccontextmanager
async def _request(
    session: aiohttp.ClientSession,
    arguments: WebFetchArguments,
    headers: dict[str, str],
    data: Optional[bytes],
) -> AsyncIterator[tuple[aiohttp.ClientResponse, list[pydantic.JsonValue]]]:
    """Follow bounded redirects without forwarding cross-origin credentials or bodies."""
    url = arguments.url
    method = arguments.method
    redirects: list[pydantic.JsonValue] = []
    while True:
        async with session.request(
            method, url, headers=headers, data=data, allow_redirects=False
        ) as response:
            location = response.headers.get("Location")
            if (
                not arguments.follow_redirects
                or response.status not in (301, 302, 303, 307, 308)
                or location is None
            ):
                yield response, redirects
                return
            if len(redirects) >= MAX_REDIRECTS:
                raise tools.ToolExecutionError(
                    f"Too many redirects (maximum {MAX_REDIRECTS})"
                )
            try:
                target = WebFetchArguments.validate_url(
                    urljoin(str(response.url), location)
                )
            except ValueError as exc:
                raise tools.ToolExecutionError(f"Invalid redirect URL: {exc}") from exc
            if urlsplit(url).scheme == "https" and urlsplit(target).scheme == "http":
                raise tools.ToolExecutionError(
                    "HTTPS to HTTP redirects are not allowed"
                )
            if (response.status == 303 and method != "HEAD") or (
                response.status in (301, 302) and method == "POST"
            ):
                method = "GET"
                data = None
                headers = {
                    key: value
                    for key, value in headers.items()
                    if key.lower()
                    not in (
                        "content-type",
                        "content-length",
                        "content-encoding",
                        "transfer-encoding",
                    )
                }
            if _origin(str(response.url)) != _origin(target):
                if data is not None:
                    raise tools.ToolExecutionError(
                        "Cross-origin redirects with a request body are not allowed"
                    )
                headers = {
                    key: value
                    for key, value in headers.items()
                    if key.lower() in ("accept", "accept-language", "user-agent")
                }
            redirects.append(
                {
                    "url": str(response.url),
                    "status": response.status,
                    "location": target,
                }
            )
            url = target


async def _read_response(response: aiohttp.ClientResponse) -> bytes:
    content = bytearray()
    async for chunk in response.content.iter_chunked(65536):
        if len(content) + len(chunk) > MAX_DOWNLOAD_BYTES:
            raise tools.ToolExecutionError(
                f"Response exceeds the {MAX_DOWNLOAD_BYTES}-byte decompressed download limit; "
                "no partial document was converted"
            )
        content.extend(chunk)
    return bytes(content)


def _text_range(content: bytes, offset: int, limit: int) -> tuple[int, int]:
    """Return UTF-8-aligned byte boundaries without dropping continuation bytes."""
    start = min(offset, len(content))
    if start < len(content) and content[start] & 0xC0 == 0x80:
        raise tools.ToolExecutionError(
            "offset must be a UTF-8 character boundary; use next_offset"
        )
    end = min(start + limit, len(content))
    while end > start and end < len(content) and content[end] & 0xC0 == 0x80:
        end -= 1
    if end == start and start < len(content):
        raise tools.ToolExecutionError(
            "max_bytes is too small for the next UTF-8 character; use at least 4"
        )
    return start, end


async def _format_response(
    response: aiohttp.ClientResponse,
    arguments: WebFetchArguments,
    redirects: list[pydantic.JsonValue],
) -> tools.ToolResponse:
    """Convert a complete bounded response, then select a resumable output range."""
    raw = await _read_response(response)
    mime = response.content_type
    text_type = (
        mime.startswith("text/")
        or mime in ("application/json", "application/xml", "application/xhtml+xml")
        or mime.endswith(("+json", "+xml"))
    )
    content = raw
    content_format = "binary"
    if text_type:
        try:
            text = raw.decode(response.charset or "utf-8", errors="replace")
        except LookupError as exc:
            raise tools.ToolExecutionError("Unsupported response charset") from exc
        content_format = "text"
        if mime in ("text/html", "application/xhtml+xml"):
            content_format = arguments.format
            if arguments.format != "raw":
                text = await asyncio.to_thread(
                    convert_html,
                    text,
                    str(response.url),
                    arguments.format,
                    arguments.main_content_only,
                )
        content = text.encode("utf-8")
        start, end = _text_range(content, arguments.offset, arguments.max_bytes)
    else:
        start = min(arguments.offset, len(content))
        end = min(start + arguments.max_bytes, len(content))
    selected = content[start:end]
    truncated = end < len(content)
    payload: dict[str, pydantic.JsonValue] = {
        "url": str(response.url),
        "requested_url": arguments.url,
        "status": response.status,
        "content_type": mime,
        "content_format": content_format,
        "bytes_read": len(raw),
        "output_bytes": len(selected),
        "total_bytes": len(content),
        "offset": start,
        "next_offset": end,
        "eof": not truncated,
        "truncated": truncated,
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "redirects": redirects,
    }
    location = response.headers.get("Location")
    if location is not None:
        payload["location"] = location
    blocks: list[connect.ToolResultContentBlock]
    if text_type:
        payload.update({"encoding": "utf-8", "content": selected.decode("utf-8")})
        blocks = [connect.TextBlock(text=json.dumps(payload))]
    elif (
        mime in ("image/png", "image/jpeg", "image/gif", "image/webp")
        and selected
        and start == 0
        and not truncated
    ):
        blocks = [
            connect.TextBlock(text=json.dumps(payload)),
            connect.ImageBlock(
                mime_type=mime, data=base64.b64encode(selected).decode("ascii")
            ),
        ]
    else:
        payload.update(
            {
                "encoding": "base64",
                "content": base64.b64encode(selected).decode("ascii"),
            }
        )
        blocks = [connect.TextBlock(text=json.dumps(payload))]
    return tools.ToolResponse(content=blocks, is_error=response.status >= 400)


@register_tool("web_fetch")
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
        async with (
            asyncio.timeout(arguments.timeout_s),
            aiohttp.ClientSession(
                timeout=timeout, cookie_jar=aiohttp.DummyCookieJar(), trust_env=False
            ) as session,
            _request(session, arguments, headers, data) as (response, redirects),
        ):
            return await _format_response(response, arguments, redirects)

    return tools.Tool(
        "web_fetch",
        "Fetch HTTP(S) content from the runner host, following up to 10 redirects by default. "
        "HTML becomes main-content Markdown with links, code, and tables; format also supports text or raw HTML. "
        "max_bytes limits output AFTER conversion (default 64 KiB, maximum 1 MiB). "
        "Use offset=next_offset to continue; UTF-8 byte ranges include total_bytes, eof, and content_sha256. "
        "Each call re-fetches the document; compare hashes across ranges. Downloads exceeding 16 MiB decompressed fail. "
        "Complete supported images become image blocks; partial images and other binary content are base64. "
        "Redirects reject HTTPS downgrades and cross-origin bodies, and drop custom headers across origins "
        "except Accept, Accept-Language, and User-Agent. Approval covers the redirect chain. "
        "Uses the host network, including private addresses; subject to tool approval, not a network sandbox. "
        "Do not pass secrets in arguments: tool calls are recorded in workflow history.",
        WebFetchArguments,
        execute,
        version=2,
        approval=approval,
    )
