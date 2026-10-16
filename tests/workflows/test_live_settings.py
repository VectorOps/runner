from pathlib import Path
from unittest.mock import patch

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import pending, settled
from tests.workflows.test_agent import configure
from tests.fixtures.user_input import reply
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_model_changes_between_turns_and_replay(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path, "[components.workflows.agent]\nagents = []\n")
    defaults = ["components", "llm", "profiles", "strong"]
    model_key = [*defaults, "model"]
    reasoning_key = [*defaults, "reasoning", "effort"]
    backend = FakeProvider([answer("first"), answer("second"), answer("third")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as connection:
            accepted = await connection.start(
                protocol.Start(reference="builtin:agent", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            await connection.set_configuration(model_key, "first-model")
            await connection.set_configuration(reasoning_key, "high")
            await reply(connection, request, "hello")
            request = await pending(connection, accepted.execution_id)
            assert backend.settings[0].model == "first-model"
            assert backend.settings[0].reasoning is not None
            assert backend.settings[0].reasoning.effort == "high"

            await connection.set_configuration(model_key, "second-model")
            await connection.set_configuration(reasoning_key, "low")
            await reply(connection, request, "continue")
            request = await pending(connection, accepted.execution_id)
            assert backend.settings[1].model == "second-model"
            assert backend.settings[1].reasoning is not None
            assert backend.settings[1].reasoning.effort == "low"

            await connection.set_configuration(defaults, {})
            await connection.set_configuration(
                ["components", "llm", "model"], "fallback-model"
            )
            await connection.set_configuration(
                ["components", "llm", "reasoning"], {"effort": "medium"}
            )
            await reply(connection, request, "again")
            request = await pending(connection, accepted.execution_id)
            assert backend.settings[2].model == "fallback-model"
            assert backend.settings[2].reasoning is not None
            assert backend.settings[2].reasoning.effort == "medium"
            assert [message.role for message in backend.requests[2].messages] == [
                "user",
                "assistant",
                "user",
                "assistant",
                "user",
            ]

        async with client.embedded(tmp_path) as connection:
            restored = await pending(connection, accepted.execution_id)
            assert restored.request_id == request.request_id
            assert len(backend.requests) == 3
            await connection.stop(accepted.execution_id)
            await settled(connection, accepted.execution_id)


async def test_new_helper_invocation_reads_current_overrides(tmp_path: Path) -> None:
    configure(tmp_path, tmp_path)
    runner = WorkflowRunner(tmp_path)
    config = (await runner.project()).configuration
    backend = FakeProvider([answer("first"), answer("second")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        await config.set("workflows.defaults.weak_model", "first-helper")
        await runner.run("builtin:helper", input={"task": "Inspect."})
        await config.set("workflows.defaults.weak_model", "second-helper")
        await config.set("workflows.helper.thinking_level", "low")
        await runner.run("builtin:helper", input={"task": "Inspect again."})
    assert [item.model for item in backend.settings] == [
        "first-helper",
        "second-helper",
    ]
    assert backend.settings[1].reasoning is not None
    assert backend.settings[1].reasoning.effort == "low"
