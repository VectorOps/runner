from pathlib import Path
import secrets

import pytest

from vectorops.runner.control import client, protocol, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService


@pytest.mark.parametrize("tcp", [False, True])
async def test_configuration_over_protocol(tmp_path: Path, tcp: bool) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "base"\n')
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
                assert (await connection.get_configuration("llm.model")).value == "base"
                changed = await connection.set_configuration(
                    "llm.model", "remote", permanent=True
                )
                assert changed.source == "permanent"
                assert (await service.runner.project()).configuration.get(
                    "llm.model"
                ) == "remote"
                await connection.set_configuration("llm.model", "temporary")
                assert (
                    await connection.get_configuration("llm.model")
                ).source == "temporary"
                assert (
                    await connection.reset_configuration("llm.model")
                ).value == "base"
                await connection.set_configuration(
                    "llm.model", "restart", permanent=True
                )
                with pytest.raises(protocol.RemoteError) as error:
                    await connection.request(
                        "configuration.set", {"name": "llm..model", "value": "bad"}
                    )
                assert error.value.code == "invalid_argument"
        finally:
            await server.aclose()
    async with client.embedded(tmp_path) as connection:
        assert (await connection.get_configuration("llm.model")).value == "restart"
