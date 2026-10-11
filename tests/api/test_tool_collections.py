from pathlib import Path
from unittest.mock import patch

import connect
import pydantic
import pytest

from tests.fixtures import replay, sources
from tests.api.test_llm import FakeProvider, answer
from tests.fixtures.history import MemoryHistory
from tests.mcp.test_replay import configure
from vectorops.runner.api import tools, tool_registry
from vectorops.runner.api.llm import plan
from vectorops.runner.api.llm import provider
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.workflows import tools as workflow_tools


def test_auto_registration_and_forwarding() -> None:
    assert set(tool_registry.registrations()) >= {
        "read_file",
        "list_files",
        "exec",
        "poll_process",
        "write_stdin",
        "kill_process",
        "list_processes",
        "apply_patch",
        "update_plan",
        "web_fetch",
        "mcp",
        "run_agent",
        "subworkflow",
    }
    assert workflow_tools.list_files_tool is tools.list_files_tool
    with pytest.raises(ValueError, match="already registered"):
        tools.register_tool("read_file")(tools.read_file_tool)
    with pytest.raises(ValueError, match="identifier"):
        tools.register_tool("bad name")


async def test_defaults_overrides_and_replay(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[components.tools.collections.edit.config.apply_patch]\nformat = "patch"\n'
    )
    defaults = {"edit": tools.ToolCollection(tools=["read_file", "apply_patch"])}

    async def workflow() -> pydantic.JsonValue:
        bindings = await tools.load_collection("edit", defaults=defaults)
        assert [binding.definition.name for binding in bindings] == [
            "read_file",
            "apply_patch",
        ]
        assert "SEARCH" in bindings[1].definition.description
        return [binding.definition.model_dump(mode="json") for binding in bindings]

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text("[components.tools.collections.edit]\ntools = []\n")
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome
    assert defaults["edit"].config == {}


@pytest.mark.parametrize("configured", [False, True])
async def test_empty_collection_and_missing_default(
    tmp_path: Path, history: MemoryHistory, configured: bool
) -> None:
    if configured:
        (tmp_path / "config.toml").write_text(
            "[components.tools.collections.empty]\ntools = []\n"
        )

    async def workflow() -> pydantic.JsonValue:
        defaults = (
            {"empty": tools.ToolCollection(tools=["read_file"])}
            if configured
            else {"empty": tools.ToolCollection()}
        )
        assert await tools.load_collection("empty", defaults=defaults) == []
        with pytest.raises(ValueError, match="unknown tool collection"):
            await tools.load_collection("unknown")
        return None

    assert (
        await replay.run(history, workflow, workspace=tmp_path)
    ).outcome.kind == "completed"


@pytest.mark.parametrize(
    "config",
    [
        'tools = ["missing"]',
        'tools = ["read_file", "read_file"]',
        'tools = ["read_file"]\nconfig.read_file.typo = true',
        "tools = []\nconfig.read_file.typo = true",
        'tools = ["a", "b"]\nconfig.a.factory = "read_file"\nconfig.b.factory = "read_file"',
        'tools = ["apply_patch"]\nconfig.apply_patch.format = "invalid"',
        'tools = ["run_agent"]\nconfig.run_agent.references = []',
        'tools = ["mcp"]\nconfig.mcp.selections = [{server = "s", typo = true}]',
        'tools = ["read_file"]\nconfig.read_file.factory = 1',
    ],
)
async def test_invalid_collections(
    tmp_path: Path, history: MemoryHistory, config: str
) -> None:
    (tmp_path / "config.toml").write_text(
        "[components.tools.collections.invalid]\n" + config
    )

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ValueError):
            await tools.load_collection("invalid")
        return None

    assert (
        await replay.run(history, workflow, workspace=tmp_path)
    ).outcome.kind == "completed"


async def test_fresh_stateful_bindings(history: MemoryHistory) -> None:
    async def workflow() -> pydantic.JsonValue:
        defaults = {"plan": tools.ToolCollection(tools=["update_plan"])}
        first = await tools.load_collection("plan", defaults=defaults)
        second = await tools.load_collection("plan", defaults=defaults)
        assert first[0] is not second[0]
        assert isinstance(first[0], plan.UpdatePlanTool)
        assert isinstance(second[0], plan.UpdatePlanTool)
        assert first[0].todos == second[0].todos == []
        return None

    assert (await replay.run(history, workflow)).outcome.kind == "completed"


async def test_custom_module_config_and_workspace_isolation(
    tmp_path: Path, history: MemoryHistory
) -> None:
    path = tmp_path / "config.toml"
    selection = '[components.tools.collections.custom]\ntools = ["test_echo"]\nconfig.test_echo.prefix = "configured"\n'
    path.write_text(
        '[components.tools]\nmodules = ["tests.fixtures.registered_tools"]\n'
        + selection
    )

    async def workflow() -> pydantic.JsonValue:
        bindings = await tools.load_collection("custom")
        assert len(bindings) == 1
        result = await tools.invoke_tool(
            bindings[0],
            connect.ToolCallBlock(
                id="echo", name="test_echo", arguments={"text": "hello"}
            ),
        )
        assert result.content == [connect.TextBlock(text="configured: hello")]
        return None

    first = await replay.run(history, workflow, workspace=tmp_path)
    assert first.outcome.kind == "completed"
    path.write_text(selection)

    async def forbidden() -> pydantic.JsonValue:
        with pytest.raises(ValueError, match="module is not enabled"):
            await tools.load_collection("custom")
        return None

    assert (
        await replay.run(MemoryHistory(), forbidden, workspace=tmp_path)
    ).outcome.kind == "completed"
    assert (
        await replay.run(history, workflow, first.id, workspace=tmp_path)
    ).outcome == first.outcome


async def test_all_options_validate_before_discovery(
    tmp_path: Path, history: MemoryHistory
) -> None:
    (tmp_path / "config.toml").write_text(
        '[components.tools.collections.test]\ntools = ["run_agent", "apply_patch"]\n'
        'config.run_agent.references = ["not_found:main"]\nconfig.apply_patch.format = "invalid"\n'
    )

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(pydantic.ValidationError):
            await tools.load_collection("test")
        return None

    assert (
        await replay.run(history, workflow, workspace=tmp_path)
    ).outcome.kind == "completed"


async def test_mcp_collection_and_persistent_replay(workspace: Path) -> None:
    configure(workspace)
    with (workspace / "config.toml").open("a") as stream:
        stream.write(
            '[components.tools.collections.remote]\ntools = ["repository"]\n'
            '[components.tools.collections.remote.config.repository]\nfactory = "mcp"\n'
            'selections = [{server = "test", tools = ["echo"]}]\n'
        )
    (workspace / "workflows/collection.py").write_text(
        sources.read("tool_collection.py")
    )
    runner = WorkflowRunner(workspace)
    first = await runner.run("collection:main")
    assert not connect.ToolResultMessage.model_validate(first).is_error
    (workspace / "config.toml").write_text("")
    with (
        patch.object(
            MCPRuntime, "discover", side_effect=AssertionError("replay discovery")
        ),
        patch.object(MCPRuntime, "call", side_effect=AssertionError("replay call")),
    ):
        assert await runner.resume() == first


async def test_builtin_helper_uses_selected_collection(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = "{tmp_path}"\n'
        '[components.workflows.defaults]\nweak_model = "test"\n'
        '[components.workflows.helper]\ntool_collection = "readonly"\n'
        '[components.tools.collections.readonly]\ntools = ["read_file", "list_files"]\n'
    )
    backend = FakeProvider([answer("done")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        assert (
            await WorkflowRunner(tmp_path).run(
                "builtin:helper", input={"task": "Inspect only."}
            )
            == "done"
        )
    assert [tool.name for tool in backend.requests[0].tools] == [
        "read_file",
        "list_files",
    ]
