from pathlib import Path
from collections.abc import Iterator
from typing import Optional, Union
from unittest.mock import AsyncMock, patch

import connect
import pytest

from vectorops.runner.api.llm.models import LLMResponseError
from vectorops.runner.api.llm.configuration import ConfigurationError
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.llm import RunnerLLM
from vectorops.runner.runner.project import RunnerProject


@pytest.fixture(autouse=True)
def cleanup_patches() -> Iterator[None]:
    yield
    patch.stopall()


class RunnerClient(connect.AsyncLLMClient):
    def __init__(self, response: connect.AssistantMessage) -> None:
        super().__init__()
        self.response = response
        self.calls: list[
            tuple[str, connect.GenerateRequest, Optional[connect.RequestOptions]]
        ] = []

    async def generate(
        self,
        model: Union[str, connect.ModelSpec],
        request: connect.GenerateRequest,
        *,
        provider: Optional[str] = None,
        options: Optional[connect.RequestOptions] = None,
    ) -> connect.AssistantMessage:
        assert isinstance(model, str)
        self.calls.append((model, request, options))
        return self.response


async def test_runner_llm_defaults_to_weak_and_uses_live_configuration(
    tmp_path: Path,
) -> None:
    project = await RunnerProject.open(tmp_path)
    await project.configuration.set(
        "llm",
        {
            "model": "strong-model",
            "temperature": 0.8,
            "profiles": {"weak": {"model": "weak-model", "temperature": 0.2}},
        },
    )
    response = connect.AssistantMessage(
        content=[connect.TextBlock(text="Useful title")], finish_reason="stop"
    )
    client = RunnerClient(response)
    authentication = AuthenticationService()
    transport = patch.object(
        authentication, "transport", new=AsyncMock(return_value=None)
    ).start()
    helper = RunnerLLM(
        project.configuration, authentication, client_factory=lambda: client
    )
    message = connect.UserMessage(content=[connect.TextBlock(text="Help with tests")])
    assert (
        await helper.generate(message, system_prompt="Title", max_output_tokens=64)
        == response
    )
    model, request, options = client.calls[0]
    assert model == "weak-model"
    assert request.messages == [message]
    assert request.system_prompt == "Title"
    assert request.temperature == 0.2
    assert request.max_output_tokens == 64
    assert request.tools == []
    assert options is not None
    transport.assert_awaited_once_with("weak-model", None)
    await project.configuration.set("llm", {"model": "updated"})
    await helper.generate(message, profile="strong")
    assert client.calls[1][0] == "updated"


@pytest.mark.parametrize("finish_reason", ["length", "tool_call", "unknown"])
async def test_runner_llm_rejects_unsuccessful_responses(
    tmp_path: Path, finish_reason: str
) -> None:
    project = await RunnerProject.open(tmp_path)
    await project.configuration.set("llm", {"model": "model"})
    response = connect.AssistantMessage.model_validate(
        {
            "content": [{"type": "text", "text": "incomplete"}],
            "finish_reason": finish_reason,
        }
    )
    authentication = AuthenticationService()
    patch.object(authentication, "transport", new=AsyncMock(return_value=None)).start()
    helper = RunnerLLM(
        project.configuration,
        authentication,
        client_factory=lambda: RunnerClient(response),
    )
    with pytest.raises(LLMResponseError):
        await helper.generate(
            connect.UserMessage(content=[connect.TextBlock(text="Hello")])
        )


async def test_runner_llm_configuration_error_has_file_and_profile(
    tmp_path: Path,
) -> None:
    project = await RunnerProject.open(tmp_path)
    helper = RunnerLLM(project.configuration, AuthenticationService())
    with pytest.raises(ConfigurationError) as error:
        await helper.generate(
            connect.UserMessage(content=[connect.TextBlock(text="Hello")])
        )
    message = str(error.value)
    assert str(project.config_path) in message
    assert "profile 'weak'" in message
    assert "model: required setting is missing" in message
    assert "[components.llm.profiles.weak]" in message
