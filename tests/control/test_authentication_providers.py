from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from vectorops.runner.authentication import models, oauth
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, protocol, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService


def test_catalog_tracks_oauth_adapters_without_storage_or_network() -> None:
    service = AuthenticationService(adapters={})
    assert all(not item.oauth_login for item in service.providers().providers)
    service.register("custom", oauth.ChatGPT())
    service.register("mcp-oauth-internal", oauth.ChatGPT())
    providers = {item.provider: item for item in service.providers().providers}
    assert providers["custom"].credential_kinds == ["api_key", "oauth2"]
    assert providers["custom"].oauth_login
    assert "chatgpt" not in providers
    assert "mcp-oauth-internal" not in providers
    assert providers["mcp"].credential_kinds == ["api_key"]


@pytest.mark.parametrize("tcp", [False, True])
async def test_provider_discovery_protocol(
    tmp_path: Path, tcp: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with RunnerService(tmp_path) as service:
        token = (await service.configuration.load()).control_token
        server = RunnerServer(service, token)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(await server.listen(), token)
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, token)
            async with connection:
                forbidden = AsyncMock(
                    side_effect=AssertionError("credential store accessed")
                )
                monkeypatch.setattr(service.authentication.store, "load", forbidden)
                monkeypatch.setattr(
                    service.authentication.configuration, "load", forbidden
                )
                result = await connection.authentication_providers()
                assert [item.provider for item in result.providers] == [
                    "anthropic",
                    "chatgpt",
                    "gemini",
                    "mcp",
                    "openai",
                    "openrouter",
                ]
                assert result.custom_api_key_providers
                assert [
                    item.provider for item in result.providers if item.oauth_login
                ] == ["chatgpt"]
                assert (
                    models.AuthenticationProviders.model_validate_json(
                        result.model_dump_json()
                    )
                    == result
                )
                for item in result.providers:
                    assert set(item.model_dump()) == {
                        "provider",
                        "display_name",
                        "credential_kinds",
                        "oauth_login",
                    }
                assert not service.authentication.pending()
                forbidden.assert_not_awaited()
                with pytest.raises(protocol.RemoteError) as error:
                    await connection.request("auth.providers", {"profile": "work"})
                assert error.value.code == "invalid_argument"
        finally:
            await server.aclose()
