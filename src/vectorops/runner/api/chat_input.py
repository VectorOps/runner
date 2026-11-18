"""User-only chat input contracts, independent of provider metadata.

Messages contain at most four blocks: text up to 64 KiB of characters or inline
PNG/JPEG/GIF/WebP images up to 4 MiB of base64 each. Image data uses padded base64,
not URLs; MIME declarations do not imply decoded image or model capability checks.
"""

import typing

import connect
import pydantic

LEGACY_IMAGE_BYTES: typing.Final = 786432
MAX_IMAGE_BYTES: typing.Final = 3 * 1024 * 1024


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class TextInput(Model):
    type: typing.Literal["text"]
    text: str = pydantic.Field(min_length=1, max_length=65536)


class ImageInput(Model):
    type: typing.Literal["image"]
    mime_type: typing.Literal["image/png", "image/jpeg", "image/gif", "image/webp"]
    data: str = pydantic.Field(
        min_length=4,
        max_length=1048576,
        pattern=r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
        json_schema_extra={"not": {"pattern": "[^A-Za-z0-9+/=]"}},
    )
    detail: typing.Literal["auto", "low", "high", "original"] = "auto"


class LargeImageInput(ImageInput):
    data: str = pydantic.Field(
        min_length=4,
        max_length=4 * MAX_IMAGE_BYTES // 3,
        pattern=r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
        json_schema_extra={"not": {"pattern": "[^A-Za-z0-9+/=]"}},
    )


UserContent: typing.TypeAlias = typing.Annotated[
    typing.Union[TextInput, ImageInput], pydantic.Field(discriminator="type")
]


class UserMessageInput(Model):
    content: list[UserContent] = pydantic.Field(min_length=1, max_length=4)

    def to_message(self) -> connect.UserMessage:
        return _to_message(self.content)


class LargeUserMessageInput(Model):
    content: list[
        typing.Annotated[
            typing.Union[TextInput, LargeImageInput],
            pydantic.Field(discriminator="type"),
        ]
    ] = pydantic.Field(min_length=1, max_length=4)

    def to_message(self) -> connect.UserMessage:
        return _to_message(self.content)


def _to_message(
    content: typing.Sequence[typing.Union[TextInput, ImageInput]],
) -> connect.UserMessage:
    blocks: list[typing.Union[connect.TextBlock, connect.ImageBlock]] = []
    for block in content:
        if isinstance(block, TextInput):
            blocks.append(connect.TextBlock(text=block.text))
        else:
            blocks.append(
                connect.ImageBlock(
                    mime_type=block.mime_type,
                    data=block.data,
                    detail=block.detail,
                )
            )
    return connect.UserMessage(content=blocks)


class UserMessageRequest(Model):
    prompt: str = pydantic.Field(max_length=65536)


class ImageMessageRequest(UserMessageRequest):
    max_image_bytes: int = pydantic.Field(
        default=MAX_IMAGE_BYTES, ge=3, le=MAX_IMAGE_BYTES
    )


def response_schema(
    max_image_bytes: int = LEGACY_IMAGE_BYTES,
) -> dict[str, pydantic.JsonValue]:
    """Publish an opt-in image budget without changing existing mailbox contracts."""
    if type(max_image_bytes) is not int or not 3 <= max_image_bytes <= MAX_IMAGE_BYTES:
        raise ValueError("image budget must be between 3 bytes and 3 MiB")
    schema = UserMessageInput.model_json_schema()
    schema["$defs"]["ImageInput"]["properties"]["data"]["maxLength"] = 4 * (
        max_image_bytes // 3
    )
    return schema
