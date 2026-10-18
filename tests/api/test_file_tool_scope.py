import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import connect
import pydantic
import pytest

from tests.api.test_tools import ApprovalStore, approve
from tests.fixtures import replay
from vectorops.runner import tool_authorization as auth, tool_policy
from vectorops.runner.api import tools
from vectorops.runner.environment import paths
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.patch import PatchService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext


def payload(response: tools.ToolResponse) -> dict[str, pydantic.JsonValue]:
    block = response.content[0]
    assert isinstance(block, connect.TextBlock)
    return pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(block.text)


@pytest.mark.parametrize("home_relative", [False, True])
async def test_absolute_and_home_relative_patch_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, home_relative: bool
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))
    service = PatchService(root)
    internal = f"*** Begin Patch\n*** Add File: {root / 'inside'}\n+ok\n*** End Patch"
    assert await service.scope(internal) == "internal"
    assert not (await service.apply(internal)).is_error
    filename = "~/outside/file" if home_relative else str(tmp_path / "outside/file")
    external = f"*** Begin Patch\n*** Add File: {filename}\n+ok\n*** End Patch"
    assert await service.scope(external) == "external"
    assert not (await service.apply(external)).is_error
    assert (tmp_path / "outside/file").read_text() == "ok\n"


async def test_external_lines_and_home_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "file").write_text("first\nsecond\n")
    target = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root))
    )
    try:
        context = ActivityExecutionContext(None, "workflow", "step", environment=target)
        result = payload(
            await tools.read_file_tool().execute(
                context,
                {"path": "~/file", "mode": "lines", "start_line": 2, "max_lines": 1},
            )
        )
        assert result["text"] == "second\n"
        result = payload(await tools.list_files_tool().execute(context, {"path": "~"}))
        assert "~/file" in str(result["entries"])
    finally:
        await target.processes.close()


@pytest.mark.parametrize("name", ["read_file", "list_files"])
@pytest.mark.parametrize(
    "location",
    ["relative", "absolute_internal", "parent", "absolute_external", "symlink"],
)
async def test_read_and_list_scope_and_external_access(
    tmp_path: Path, name: str, location: str
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "file").write_text("internal")
    (outside / "file").write_text("external")
    (root / "link").symlink_to(outside, target_is_directory=True)
    selected = {
        "relative": ".",
        "absolute_internal": str(root),
        "parent": "../outside",
        "absolute_external": str(outside),
        "symlink": "link",
    }[location]
    if name == "read_file":
        selected = str(Path(selected) / "file")
    target = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root))
    )
    try:
        context = ActivityExecutionContext(None, "workflow", "step", environment=target)
        binding = (
            tools.read_file_tool() if name == "read_file" else tools.list_files_tool()
        )
        request = connect.ToolCallBlock(
            id="call", name=name, arguments={"path": selected}
        )
        metadata = await auth.ToolAuthorizationService.resolve_auth_context(
            binding, request, context
        )
        expected = (
            "internal" if location in ("relative", "absolute_internal") else "external"
        )
        assert metadata.properties == {"scope": expected}
        result = payload(await binding.execute(context, request.arguments))
        if name == "read_file":
            assert result["content"] == expected
        else:
            assert "file" in str(result["entries"])
        with pytest.raises(Exception):
            paths.resolve_path(target.root, "../outside")
    finally:
        await target.processes.close()


@pytest.mark.parametrize("format", ["v4a", "patch"])
async def test_patch_external_create_update_delete(tmp_path: Path, format: str) -> None:
    root = tmp_path / "root"
    root.mkdir()
    service = PatchService(root)
    filename = "../outside/file"

    def patch(before: str, after: str) -> str:
        if format == "patch":
            return f"```text\n{filename}\n<<<<<<< SEARCH\n{before + chr(10) if before else ''}=======\n{after + chr(10) if after else ''}>>>>>>> REPLACE\n```"
        if not before:
            body = f"*** Add File: {filename}\n+{after}"
        elif not after:
            body = f"*** Delete File: {filename}"
        else:
            body = f"*** Update File: {filename}\n@@\n-{before}\n+{after}"
        return f"*** Begin Patch\n{body}\n*** End Patch"

    for before, after in [("", "old"), ("old", "new"), ("new", "")]:
        text = patch(before, after)
        assert await service.scope(text, format=format) == "external"
        assert not (await service.apply(text, format=format)).is_error
        path = tmp_path / "outside/file"
        assert path.read_text().strip() == after if after else not path.exists()


async def test_patch_mixed_move_reverse_and_symlink_scope(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "old").write_bytes(b"\x00\xff")
    service = PatchService(root)
    text = "*** Begin Patch\n*** Update File: old\n*** Move to: ../outside/new\n*** End Patch"
    assert await service.scope(text) == "external"
    assert not (await service.apply(text)).is_error
    assert (outside / "new").read_bytes() == b"\x00\xff"
    assert await service.scope(text, reverse=True) == "external"
    assert not (await service.apply(text, reverse=True)).is_error
    (root / "link").symlink_to(outside, target_is_directory=True)
    mixed = "*** Begin Patch\n*** Add File: internal\n+ok\n*** Add File: link/external\n+ok\n*** End Patch"
    assert await service.scope(mixed) == "external"
    assert not (await service.apply(mixed)).is_error
    (root / "file_link").symlink_to(outside / "external")
    rejected = await service.apply(
        "*** Begin Patch\n*** Update File: file_link\n@@\n-ok\n+bad\n*** End Patch"
    )
    assert rejected.is_error
    assert (outside / "external").read_text() == "ok\n"


@pytest.mark.parametrize("external_call", [False, True])
@pytest.mark.parametrize("name", ["read_file", "list_files", "apply_patch"])
async def test_external_call_prompts_under_internal_rule(
    tmp_path: Path, name: str, external_call: bool
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    external = tmp_path / "file"
    external.write_text("original")
    (root / "file").write_text("original")
    target = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root))
    )
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    data = Mock(spec=auth.AuthorizationData)
    data.settings.return_value = tool_policy.ApprovalSettings()
    data.rules = AsyncMock(return_value=[replay.allow(name, {"scope": "internal"})])
    data.approval_rule_id = AsyncMock(return_value=None)
    context = ActivityExecutionContext(
        None,
        "test",
        "step",
        environment=target,
        mailbox=box,
        persist_authorization=AsyncMock(),
    )
    binding: tools.ToolBinding
    arguments: dict[str, pydantic.JsonValue]
    filename = "../file" if external_call else "file"
    if name == "apply_patch":
        binding = tools.apply_patch_tool()
        arguments = {
            "text": f"*** Begin Patch\n*** Update File: {filename}\n@@\n-original\n+changed\n*** End Patch"
        }
    else:
        binding = (
            tools.read_file_tool() if name == "read_file" else tools.list_files_tool()
        )
        arguments = {
            "path": filename if name == "read_file" else ".." if external_call else "."
        }
    context = ActivityExecutionContext(
        None,
        "test",
        "step",
        environment=target,
        mailbox=box,
        persist_authorization=AsyncMock(),
        persist_authorization_decision=AsyncMock(),
    )
    try:
        task = asyncio.create_task(
            auth.ToolAuthorizationService(data).authorize(
                context,
                binding,
                connect.ToolCallBlock(id="call", name=name, arguments=arguments),
            )
        )
        if not external_call:
            result = await asyncio.wait_for(task, 2)
            assert result.decision.action == "approve"
            assert not store.requests
            return
        await asyncio.wait_for(store.registered.wait(), 2)
        request = store.requests["step"]
        assert tool_policy.ToolApprovalRequest.model_validate(
            request.payload
        ).auth_context.properties == {"scope": "external"}
        assert external.read_text() == "original"
        await approve(box, request, {"decision": "deny"})
        assert (await task).decision.action == "deny"
        assert external.read_text() == "original"
    finally:
        await target.processes.close()
