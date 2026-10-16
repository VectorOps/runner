import hashlib
import json
from pathlib import Path
from typing import Optional, Union

import connect
import pytest

from vectorops.runner import global_config
from vectorops.runner.api.llm import api, models, provider
from vectorops.runner.authentication import models as am
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@pytest.mark.parametrize("legacy", [False, True])
async def test_provider_resolves_injected_profile_before_io(
    tmp_path: Path, legacy: bool
) -> None:
    authentication = AuthenticationService(global_config.GlobalConfiguration(tmp_path))
    await authentication.provision(
        am.Credential(
            provider="openai", profile="work", kind="api_key", secret="private-api-key"
        )
    )
    calls = []

    class Client(connect.AsyncLLMClient):
        def stream(
            self,
            model: Union[str, connect.ModelSpec],
            request: connect.GenerateRequest,
            *,
            provider: Optional[str] = None,
            options: Optional[connect.RequestOptions] = None,
        ) -> connect.StreamHandle:
            assert options is not None
            assert model == ("model" if legacy else "openai/model")
            assert provider == ("openai" if legacy else None)
            calls.append(options)
            raise RuntimeError("test transport boundary")

    backend = provider.ConnectProvider(client_factory=Client)
    context = ActivityExecutionContext(
        "execution", "workflow", "step", authentication=authentication
    )
    request = connect.GenerateRequest(messages=[connect.UserMessage(content="hello")])
    settings = models.Settings(
        model="model" if legacy else "openai/model",
        provider="openai" if legacy else None,
        credential_profile="work",
    )
    with pytest.raises(RuntimeError, match="test transport"):
        async for _ in backend.stream(context, settings, request):
            pass
    assert len(calls) == 1
    assert calls[0].idempotency_key == "step"
    assert calls[0].auth is not None
    assert (await calls[0].auth.resolve()).headers[
        "Authorization"
    ] == "Bearer private-api-key"
    assert "private-api-key" not in repr(context)
    assert "private-api-key" not in settings.model_dump_json()


def test_default_profile_preserves_legacy_request_digest() -> None:
    llm = object.__new__(api.LLM)
    llm._identity = "identity"
    llm._settings = models.Settings(model="openai/model")
    request = connect.GenerateRequest(messages=[connect.UserMessage(content="hello")])
    old_settings = llm._settings.model_dump(mode="json", exclude={"credential_profile"})
    encoded = json.dumps(
        {"request": request.model_dump(mode="json"), "settings": old_settings},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    legacy = hashlib.sha256(encoded.encode()).hexdigest()
    assert llm._input(request)["request_digest"] == legacy
    llm._settings = llm._settings.model_copy(update={"credential_profile": "work"})
    assert llm._input(request)["request_digest"] != legacy


async def test_authentication_exceptions_do_not_record_provider_payloads() -> None:
    class Client(connect.AsyncLLMClient):
        def stream(
            self,
            model: Union[str, connect.ModelSpec],
            request: connect.GenerateRequest,
            *,
            provider: Optional[str] = None,
            options: Optional[connect.RequestOptions] = None,
        ) -> connect.StreamHandle:
            raise connect.AuthenticationError(
                connect.ErrorInfo(
                    code="auth", message="private-provider-secret", status_code=401
                )
            )

    backend = provider.ConnectProvider(
        auth=connect.BearerTokenAuth("private-key"), client_factory=Client
    )
    with pytest.raises(am.AuthenticationError) as error:
        async for _ in backend.stream(
            ActivityExecutionContext(None, "workflow", "step"),
            models.Settings(model="openai/model"),
            connect.GenerateRequest(messages=[connect.UserMessage(content="hello")]),
        ):
            pass
    assert "private" not in str(error.value)


async def test_authentication_rejects_conflicting_provider_before_credentials(
    tmp_path: Path,
) -> None:
    authentication = AuthenticationService(global_config.GlobalConfiguration(tmp_path))
    with pytest.raises(am.AuthenticationError, match="conflicts"):
        await authentication.transport("openai", "anthropic/claude")
