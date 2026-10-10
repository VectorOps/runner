"""Compatibility exports for chat contracts and their mailbox adapter."""

from vectorops.runner.api import chat_input, user_input
from vectorops.runner.runner import mailbox as runtime_mailbox

Model = chat_input.Model
TextInput = chat_input.TextInput
ImageInput = chat_input.ImageInput
UserContent = chat_input.UserContent
UserMessageInput = chat_input.UserMessageInput
request_user_message = user_input.request_user_message


class UserMessageRequest(chat_input.UserMessageRequest):
    def input_spec(self, channel: str = "user.message") -> runtime_mailbox.InputSpec:
        return user_input.input_spec(self, channel)
