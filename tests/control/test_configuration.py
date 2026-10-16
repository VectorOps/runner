from pathlib import Path
import secrets
from unittest.mock import patch

import connect
import pydantic
import pytest

from vectorops.runner.control import client, protocol, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


@pytest.mark.parametrize("name", ["llm.model", "components.llm.model"])
async def test_canonical_names_share_one_configuration_target(
    tmp_path: Path, name: str
) -> None:
    async with RunnerService(tmp_path) as service:
        config = (await service.runner.project()).configuration
        await service.dispatch("configuration.set", {"name": name, "value": "first"})
        assert config.get("components.llm.model") == config.get("llm.model") == "first"
        selected = await service.dispatch("configuration.get", {"name": name})
        assert selected == {
            "name": "components.llm.model",
            "exists": True,
            "value": "first",
        }
        await config.set("components.llm.model", "second")
        assert config.inspect("llm.model").value == "second"
        await service.dispatch("configuration.reset", {"name": name})
        assert config.get("components.llm.model") is None


@pytest.mark.parametrize("tcp", [False, True])
async def test_configuration_over_protocol(tmp_path: Path, tcp: bool) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "base"\n')
    path = "components.llm.model"
    token = secrets.token_urlsafe(32)
    async with RunnerService(tmp_path) as service:
        server = RunnerServer(service, token)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(await server.listen(), token)
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, token)
            async with connection:
                assert (await connection.get_configuration(path)).value == "base"
                await connection.set_configuration(path, "remote", permanent=True)
                assert (await service.runner.project()).configuration.get(
                    "llm.model"
                ) == "remote"
                await connection.set_configuration(path, "temporary")
                assert (await connection.get_configuration(path)).value == "temporary"
                await connection.reset_configuration(path)
                assert (await connection.get_configuration(path)).value == "base"
                await connection.set_configuration(path, "restart", permanent=True)
                with pytest.raises(protocol.RemoteError) as error:
                    await connection.request(
                        "configuration.set", {"name": "llm..model", "value": "bad"}
                    )
                assert error.value.code == "invalid_argument"
        finally:
            await server.aclose()
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration(path)).value == "restart"


async def test_snapshot_arbitrary_values_and_literal_keys(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.llm]\nmodel = "base"\ntemperature = "invalid"\n'
    )
    async with client.embedded(tmp_path) as connection:
        snapshot = await connection.get_configuration()
        assert snapshot.name == "" and snapshot.exists
        assert isinstance(snapshot.value, dict)
        assert snapshot.value["data_directory"] == "data"
        assert snapshot.value["components"] == {
            "llm": {"model": "base", "temperature": "invalid"}
        }
        path = "components.arbitrary~1key.~01.value"
        await connection.set_configuration(
            path, [False, 0, None, {"x": 1}], permanent=True
        )
        selected = await connection.get_configuration(path)
        assert selected.value == [False, 0, None, {"x": 1}]
        await connection.set_configuration("components.llm.model", None)
        assert (await connection.get_configuration("components.llm.model")).exists
        await connection.set_configuration("components.llm", "invalid-object")
        assert (
            await connection.get_configuration("components.llm")
        ).value == "invalid-object"
        assert not (await connection.get_configuration("components.llm.model")).exists
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration(path)).value == selected.value
        await connection.reset_configuration(path)
        assert not (await connection.get_configuration(path)).exists


@pytest.mark.parametrize("method", ["configuration.set", "configuration.reset"])
@pytest.mark.parametrize(
    "path",
    [
        "",
        "data_directory",
        "workflows_directory",
        "environment",
        "logging.level",
        "retention",
    ],
)
async def test_startup_settings_are_read_only(
    tmp_path: Path, method: str, path: str
) -> None:
    async with client.embedded(tmp_path) as connection:
        before = await connection.get_configuration()
        params: dict[str, pydantic.JsonValue] = {
            "name": path,
            **({"value": {"components": {}}} if method == "configuration.set" else {}),
        }
        with pytest.raises(protocol.RemoteError) as error:
            await connection.request(method, params)
        assert error.value.code == "read_only"
        assert await connection.get_configuration() == before
        await connection.set_configuration(
            "components", {"data_directory": "component-value"}
        )
        assert (await connection.get_configuration("data_directory")).value == "data"


async def test_suggestions_are_advisory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = connect.ModelRegistry(
        [
            connect.ModelSpec(provider="test", model="sample-a", api_family="test"),
            connect.ModelSpec(provider="other", model="sample-b", api_family="test"),
            connect.ModelSpec(provider="other", model="sample-a", api_family="test"),
            connect.ModelSpec(
                provider="router", model="vendor/nested", api_family="test"
            ),
        ]
    )
    monkeypatch.setattr(connect, "default_model_registry", registry)
    async with client.embedded(tmp_path) as connection:
        path = "components.llm.profiles.strong.model"
        assert (await connection.suggest_configuration(path, "test/")).values == [
            "test/sample-a"
        ]
        assert (await connection.suggest_configuration(path)).values == [
            "other/sample-a",
            "other/sample-b",
            "router/vendor/nested",
            "test/sample-a",
        ]
        for name in (
            "components.llm.model",
            "components.llm.compaction.summary_settings.model",
            "components.workflows.defaults.strong_model",
        ):
            assert (await connection.suggest_configuration(name, "router/")).values == [
                "router/vendor/nested"
            ]
        assert (
            await connection.suggest_configuration("components.llm.provider")
        ).values == []
        assert (
            await connection.suggest_configuration(
                "components.llm.reasoning.effort", "hi"
            )
        ).values == ["high"]
        assert (
            await connection.suggest_configuration(
                "components.llm.compaction.summary_settings.reasoning.summary"
            )
        ).values == ["auto", "concise", "detailed"]
        assert (
            await connection.suggest_configuration(
                "components.workflows.defaults.thinking_level", "x"
            )
        ).values == ["xhigh"]
        assert (
            await connection.suggest_configuration("components.custom.model")
        ).values == []
        await connection.set_configuration(path, "not-in-catalog")
        assert (await connection.get_configuration(path)).value == "not-in-catalog"


async def test_persistence_failure_does_not_publish(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as connection:
        path = "components.worker.count"
        await connection.set_configuration(path, 1, permanent=True)
        with patch.object(
            WorkspaceDatabase, "save_document", side_effect=OSError("failed")
        ):
            with pytest.raises(protocol.RemoteError) as error:
                await connection.set_configuration(path, 2)
        assert error.value.code == "configuration_error"
        assert (await connection.get_configuration(path)).value == 1


async def test_lifetimes_reset_and_toml_baseline(tmp_path: Path) -> None:
    toml = tmp_path / "config.toml"
    toml.write_text("[components.worker]\ncount = 1\n")
    path = "components.worker.count"
    async with client.embedded(tmp_path) as connection:
        await connection.set_configuration(path, 2, permanent=True)
        await connection.set_configuration(path, 3)
        assert (await connection.get_configuration(path)).value == 3
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration(path)).value == 1
        await connection.set_configuration(path, 4, permanent=True)
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration(path)).value == 4
        await connection.reset_configuration(path)
        assert (await connection.get_configuration(path)).value == 1
        await connection.set_configuration(path, 5, permanent=True)
        toml.write_text("[components.worker]\ncount = 6\n")
        assert (await connection.get_configuration(path)).value == 5
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration(path)).value == 6


@pytest.mark.parametrize(
    "params",
    [
        {"path": "components.llm.model", "value": 1},
        {"name": ["components", "llm", "model"], "value": 1},
        {"name": "components..model", "value": 1},
        {"name": "components. ", "value": 1},
        {"name": 1, "value": 1},
        {"name": "components.x"},
        {"name": "components.x", "value": 1, "permanent": "yes"},
    ],
)
async def test_invalid_request_shapes(tmp_path: Path, params: dict) -> None:
    async with client.embedded(tmp_path) as connection:
        with pytest.raises(protocol.RemoteError) as error:
            await connection.request("configuration.set", params)
        assert error.value.code == "invalid_argument"
