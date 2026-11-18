"""Mailbox adapter for bounded user chat input contracts."""

import connect

from vectorops.runner.api import chat_input, mailbox
from vectorops.runner.runner import mailbox as runtime_mailbox


def input_spec(
    request: chat_input.UserMessageRequest,
    channel: str = "user.message",
    *,
    max_image_bytes: int = chat_input.LEGACY_IMAGE_BYTES,
) -> runtime_mailbox.InputSpec:
    if max_image_bytes != chat_input.LEGACY_IMAGE_BYTES:
        request = chat_input.ImageMessageRequest(
            prompt=request.prompt, max_image_bytes=(max_image_bytes // 3) * 3
        )
    return runtime_mailbox.InputSpec(
        channel=channel,
        payload=request.model_dump(mode="json"),
        response_type="json",
        request_type="chat.message.v1",
        request_schema=request.model_json_schema(),
        response_schema=chat_input.response_schema(max_image_bytes),
    )


async def request_user_message(
    prompt: str = "Your message?",
    *,
    channel: str = "user.message",
    max_image_bytes: int = chat_input.LEGACY_IMAGE_BYTES,
) -> connect.UserMessage:
    """Wait for validated mailbox input and convert only to a user-role message."""
    spec = input_spec(
        chat_input.UserMessageRequest(prompt=prompt),
        channel,
        max_image_bytes=max_image_bytes,
    )
    response = await mailbox.request_input(
        channel=spec.channel,
        payload=spec.payload,
        response_type=spec.response_type,
        request_type=spec.request_type,
        request_schema=spec.request_schema,
        response_schema=spec.response_schema,
    )
    return chat_input.LargeUserMessageInput.model_validate(response).to_message()
