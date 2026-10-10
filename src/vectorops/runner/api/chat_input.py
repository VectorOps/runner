"""User-only chat input contracts, independent of provider metadata.

Messages contain at most four blocks: text up to 64 KiB of characters or inline
PNG/JPEG/GIF/WebP images up to 1 MiB of base64 each. Image data uses padded base64,
not URLs; MIME declarations do not imply decoded image or model capability checks.
"""

import typing

import connect
import pydantic


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


UserContent: typing.TypeAlias = typing.Annotated[
    typing.Union[TextInput, ImageInput], pydantic.Field(discriminator="type")
]


class UserMessageInput(Model):
    content: list[UserContent] = pydantic.Field(min_length=1, max_length=4)

    def to_message(self) -> connect.UserMessage:
        blocks: list[typing.Union[connect.TextBlock, connect.ImageBlock]] = []
        for block in self.content:
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
