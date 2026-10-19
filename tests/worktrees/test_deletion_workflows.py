import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import connect
import pydantic
import pytest

from tests.fixtures import worktree_cases as cases, worktree_deletion as flows
from vectorops.runner.api import tools
from vectorops.runner.control import protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import branching, execution_state, mailbox
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager


def test_llm_delete_has_no_workflow_cancellation_argument() -> None:
    properties = tools.worktree_delete_tool().definition.input_schema["properties"]
    assert set(properties) == {"worktree_id", "force"}
    with pytest.raises(pydantic.ValidationError):
        m.Delete.model_validate(
            {
                "worktree_id": "00000000-0000-0000-0000-000000000001",
                "force": True,
                "cancel_workflows": True,
            }
        )


async def start(
    service: RunnerService, reference: str, key: str, **arguments: object
) -> str:
    request = p.Start.model_validate(
        {"reference": reference, "idempotency_key": key, **arguments}
    )
    return p.Accepted.model_validate(
        await service.dispatch("executions.start", request.model_dump(mode="json"))
    ).execution_id


async def status(service: RunnerService, identifier: str, expected: str) -> None:
    async with asyncio.timeout(10):
        while True:
            try:
                if (
                    await service.runner.execution_state(identifier)
                ).status == expected:
                    return
            except (FileNotFoundError, KeyError):
                pass
            await asyncio.sleep(0.01)


async def settled(service: RunnerService, identifier: str) -> None:
    async with asyncio.timeout(10):
        while (
            await service.dispatch("executions.current", {"execution_id": identifier})
            is not None
        ):
            await asyncio.sleep(0.01)


async def delete(
    service: RunnerService, record: m.Record, *, force: bool = False
) -> m.Record:
    return m.Record.model_validate(
        await service.dispatch(
            "worktrees.delete",
            m.Delete(worktree_id=record.id, force=force).model_dump(mode="json"),
        )
    )


@pytest.mark.parametrize(
    "binding,cold",
    [
        ("root", False),
        ("root", True),
        ("child", False),
        ("child", True),
        ("raw", False),
        ("raw", True),
        ("process", False),
        ("process", True),
        ("finished-process", False),
        ("finished-process", True),
        ("finished-child", False),
        ("finished-child", True),
    ],
)
async def test_paused_executions_are_cancelled_before_deletion(
    worktree_project: RunnerProject,
    worktree_probe: cases.Probe,
    binding: str,
    cold: bool,
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Paused"), key="paused")
        if binding in ("child", "finished-child"):
            identifier = await start(
                service,
                flows.REFERENCE
                + ("child_paused" if binding == "child" else "finished_child_paused"),
                "start",
                input={"worktree_id": str(record.id)},
            )
        elif binding in ("process", "finished-process"):
            identifier = await start(
                service,
                flows.REFERENCE + "process_paused",
                "start",
                input={
                    "worktree_id": str(record.id),
                    "completed": binding == "finished-process",
                },
            )
        elif binding == "raw":
            identifier = await start(
                service,
                flows.REFERENCE + "paused",
                "start",
                base_project_path=record.path,
            )
        else:
            identifier = await start(
                service, flows.REFERENCE + "paused", "start", worktree_id=str(record.id)
            )
        await status(service, identifier, "paused")
        requests = (
            await service.runner.pending_inputs(
                mailbox.RequestQuery(execution_id=identifier)
            )
        ).requests
        assert len(requests) == 1
        if not cold:
            assert (await delete(service, record)).status == "deleted"
            await settled(service, identifier)
            assert (
                await service.runner.execution_state(identifier)
            ).status == "cancelled"
            assert not (
                await service.runner.pending_inputs(
                    mailbox.RequestQuery(execution_id=identifier)
                )
            ).requests
    if cold:
        async with RunnerService(worktree_project.root) as service:
            assert (
                await service.dispatch(
                    "executions.current", {"execution_id": identifier}
                )
                is None
            )
            assert (await delete(service, record)).status == "deleted"
            assert (
                await service.runner.execution_state(identifier)
            ).status == "cancelled"
            assert not (
                await service.runner.pending_inputs(
                    mailbox.RequestQuery(execution_id=identifier)
                )
            ).requests
    assert not Path(record.path).exists()


async def test_running_user_blocks_normal_delete_without_cancelling_paused_users(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    worktree_probe.gates["running"] = asyncio.Event()
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Shared"), key="shared")
        paused = await start(
            service, flows.REFERENCE + "paused", "paused", worktree_id=str(record.id)
        )
        await status(service, paused, "paused")
        running = await start(
            service, flows.REFERENCE + "running", "running", worktree_id=str(record.id)
        )
        assert await asyncio.wait_for(worktree_probe.started.get(), 5) == "running"
        with pytest.raises(p.RemoteError, match="running executions"):
            await delete(service, record)
        assert (await service.runner.execution_state(paused)).status == "paused"
        assert (await service.runner.execution_state(running)).status == "running"
        assert Path(record.path).is_dir()
        assert (await delete(service, record, force=True)).status == "deleted"
        for identifier in (paused, running):
            await settled(service, identifier)
            assert (
                await service.runner.execution_state(identifier)
            ).status == "cancelled"
        assert len(worktree_probe.events) >= 2
        assert await manager.store.uses(record.id) == []


@pytest.mark.parametrize("mode", ["paused", "running"])
async def test_deletion_waits_for_cleanup_and_blocks_new_attachments(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, mode: str
) -> None:
    worktree_probe.gates["running"] = asyncio.Event()
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Cleanup"), key="cleanup")
        identifier = await start(
            service,
            flows.REFERENCE + mode,
            "start",
            input={"cleanup_wait": True},
            worktree_id=str(record.id),
        )
        if mode == "paused":
            await status(service, identifier, "paused")
        else:
            await asyncio.wait_for(worktree_probe.started.get(), 5)
        deleting = asyncio.create_task(delete(service, record, force=mode == "running"))
        try:
            await asyncio.wait_for(worktree_probe.cleanup_started.wait(), 5)
            assert not deleting.done() and Path(record.path).is_dir()
            with pytest.raises(m.WorktreeError, match="not ready: deleting"):
                async with manager.use(record.id, "new-execution"):
                    pytest.fail("new attachment entered during deletion")
            worktree_probe.cleanup_gate.set()
            assert (await asyncio.wait_for(deleting, 5)).status == "deleted"
            await settled(service, identifier)
            assert (
                await service.runner.execution_state(identifier)
            ).status == "cancelled"
        finally:
            worktree_probe.cleanup_gate.set()
            await asyncio.gather(deleting, return_exceptions=True)


async def test_answer_racing_normal_deletion_requires_force(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    worktree_probe.gates["after-input"] = asyncio.Event()
    async with RunnerService(worktree_project.root) as service:
        record = await (await service.runner.project()).worktrees.create(
            m.Create(name="Wake race"), key="race"
        )
        identifier = await start(
            service,
            flows.REFERENCE + "paused",
            "start",
            input={"after_input": True},
            worktree_id=str(record.id),
        )
        await status(service, identifier, "paused")
        request = (
            await service.runner.pending_inputs(
                mailbox.RequestQuery(execution_id=identifier)
            )
        ).requests[0]
        stopping, release = asyncio.Event(), asyncio.Event()
        original = SQLiteHistory.stop_execution

        async def stop(
            database: SQLiteHistory, *, require_paused: bool = False
        ) -> None:
            if require_paused:
                stopping.set()
                await release.wait()
            await original(database, require_paused=require_paused)

        with patch.object(SQLiteHistory, "stop_execution", stop):
            deleting = asyncio.create_task(delete(service, record))
            try:
                await asyncio.wait_for(stopping.wait(), 5)
                await service.dispatch(
                    "mailbox.respond",
                    mailbox.Answer(
                        execution_id=identifier,
                        request_id=request.request_id,
                        response="continue",
                        idempotency_key="answer",
                    ).model_dump(mode="json"),
                )
                assert (
                    await asyncio.wait_for(worktree_probe.started.get(), 5)
                    == "after-input"
                )
                release.set()
                with pytest.raises(p.RemoteError, match="execution is running"):
                    await asyncio.wait_for(deleting, 5)
                assert (
                    await service.runner.execution_state(identifier)
                ).status == "running"
                assert Path(record.path).is_dir()
            finally:
                release.set()
                await asyncio.gather(deleting, return_exceptions=True)
        worktree_probe.gates["after-input"].set()
        await settled(service, identifier)


@pytest.mark.parametrize("terminal", ["completed", "failed", "cancelled"])
async def test_terminal_executions_keep_their_history_status(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, terminal: str
) -> None:
    worktree_probe.gates["running"] = asyncio.Event()
    async with RunnerService(worktree_project.root) as service:
        record = await (await service.runner.project()).worktrees.create(
            m.Create(name="Terminal"), key="terminal"
        )
        reference = (
            cases.REFERENCE + "leaf"
            if terminal == "completed"
            else flows.REFERENCE + ("failed" if terminal == "failed" else "running")
        )
        identifier = await start(
            service, reference, "start", worktree_id=str(record.id)
        )
        if terminal == "cancelled":
            await asyncio.wait_for(worktree_probe.started.get(), 5)
            await service.dispatch("executions.stop", {"execution_id": identifier})
        await settled(service, identifier)
        assert (await service.runner.execution_state(identifier)).status == terminal
        assert (await delete(service, record)).status == "deleted"
        assert (await service.runner.execution_state(identifier)).status == terminal


@pytest.mark.parametrize("child_first", [False, True])
async def test_tool_refuses_paused_users_and_its_own_finished_child_target(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, child_first: bool
) -> None:
    async with RunnerService(worktree_project.root) as service:
        record = await (await service.runner.project()).worktrees.create(
            m.Create(name="Tool"), key="tool"
        )
        victim = None
        if not child_first:
            victim = await start(
                service,
                flows.REFERENCE + "paused",
                "paused",
                worktree_id=str(record.id),
            )
            await status(service, victim, "paused")
        caller = await start(
            service,
            flows.REFERENCE + "delete_tool",
            "tool",
            input={"worktree_id": str(record.id), "child_first": child_first},
        )
        await settled(service, caller)
        assert (await service.runner.execution_state(caller)).status == "completed"
        assert Path(record.path).is_dir()
        if victim is not None:
            assert (await service.runner.execution_state(victim)).status == "paused"


@pytest.mark.parametrize("force", [False, True])
async def test_abandoned_lease_does_not_make_a_terminal_execution_active(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, force: bool
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Abandoned"), key="abandoned")
        await service.runner.run(cases.REFERENCE + "leaf", worktree_id=record.id)
        identifier = service.runner.last_execution_id
        assert identifier is not None
        await manager.store.acquire(record.id, uuid4(), identifier)
        response = connect.ToolResultMessage.model_validate(
            await service.runner.run(
                flows.REFERENCE + "delete_tool",
                input={"worktree_id": str(record.id), "force": force},
            )
        )
        assert response.is_error is not force
        if not force:
            assert "abandoned usage lease" in str(response.content)
        assert Path(record.path).exists() is not force
        assert (await service.runner.execution_state(identifier)).status == "completed"


async def test_deletion_reads_only_indexed_owners(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Scoped"), key="scoped")
        unrelated = await start(service, flows.REFERENCE + "paused", "unrelated")
        owner = await start(
            service, flows.REFERENCE + "paused", "owner", worktree_id=str(record.id)
        )
        await status(service, unrelated, "paused")
        await status(service, owner, "paused")
        original = SQLiteHistory.get_execution

        async def read(database: SQLiteHistory) -> execution_state.ExecutionRecord:
            assert database.path.parent.name != unrelated, "unrelated history read"
            return await original(database)

        index = await service.runner.execution_index()
        with (
            patch.object(index, "discover", side_effect=AssertionError("archive scan")),
            patch.object(SQLiteHistory, "get_execution", read),
        ):
            assert (await delete(service, record)).status == "deleted"
        assert (await service.runner.execution_state(unrelated)).status == "paused"


async def test_branching_keeps_both_worktrees_owned_by_the_execution(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        first = await manager.create(m.Create(name="First"), key="first")
        second = await manager.create(m.Create(name="Second"), key="second")
        identifier = await start(service, flows.REFERENCE + "selected_child", "select")
        await status(service, identifier, "paused")
        query = mailbox.RequestQuery(execution_id=identifier)
        selection = (await service.runner.pending_inputs(query)).requests[0]
        await service.runner.answer_input(
            mailbox.Answer(
                execution_id=identifier,
                request_id=selection.request_id,
                response=str(first.id),
                idempotency_key="first",
            )
        )
        await status(service, identifier, "paused")
        previous = (await service.runner.pending_inputs(query)).requests[0]
        await service.dispatch(
            "executions.branch",
            branching.Branch(
                execution_id=identifier,
                step_id=selection.request_id,
                idempotency_key="second",
                action=branching.InsertInput(response=str(second.id)),
            ).model_dump(mode="json"),
        )
        async with asyncio.timeout(10):
            while True:
                pending = (await service.runner.pending_inputs(query)).requests
                if pending and pending[0].request_id != previous.request_id:
                    break
                await asyncio.sleep(0.01)
        await status(service, identifier, "paused")
        index = await service.runner.execution_index()
        for record in (first, second):
            assert await index.worktree_executions(record.id, record.path) == {
                identifier
            }
            assert await manager.store.uses(record.id) == []
        assert (await delete(service, first)).status == "deleted"
        assert (await service.runner.execution_state(identifier)).status == "cancelled"
        assert Path(second.path).is_dir()


async def test_deletion_prevents_reactivating_historical_ownership(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Admission"), key="admission")
        await service.runner.run(
            cases.REFERENCE + "matrix", input={"child": str(record.id)}
        )
        identifier = service.runner.last_execution_id
        assert identifier is not None
        database = SQLiteHistory(
            worktree_project.data_path / "executions" / identifier / "history.sqlite3"
        )
        execution = await database.get_execution()
        completion = await database.get_step(
            execution.checkpoint.heads[execution.root_workflow_id]
        )
        assert completion.previous_step_id is not None
        removing, release = asyncio.Event(), asyncio.Event()
        original = manager.backend.delete

        async def remove(worktree: m.Record, *, force: bool) -> None:
            removing.set()
            await release.wait()
            await original(worktree, force=force)

        with patch.object(manager.backend, "delete", remove):
            deleting = asyncio.create_task(delete(service, record))
            resuming = None
            try:
                await asyncio.wait_for(removing.wait(), 5)
                resuming = asyncio.create_task(
                    service.runner.run(
                        cases.REFERENCE + "matrix",
                        execution_id=identifier,
                        continue_from_step_id=completion.previous_step_id,
                    )
                )
                await asyncio.sleep(0.05)
                assert not resuming.done()
                release.set()
                assert (await asyncio.wait_for(deleting, 5)).status == "deleted"
                with pytest.raises(m.WorktreeError, match="not ready: deleted"):
                    await asyncio.wait_for(resuming, 5)
            finally:
                release.set()
                await asyncio.gather(
                    deleting,
                    *([resuming] if resuming is not None else []),
                    return_exceptions=True,
                )
        assert (await service.runner.execution_state(identifier)).status == "completed"


async def test_concurrent_deletions_share_cancellation_and_removal(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        manager = (await service.runner.project()).worktrees
        record = await manager.create(m.Create(name="Concurrent"), key="concurrent")
        identifier = await start(
            service, flows.REFERENCE + "paused", "start", worktree_id=str(record.id)
        )
        await status(service, identifier, "paused")
        with patch.object(
            manager.backend, "delete", wraps=manager.backend.delete
        ) as remove:
            first, second = await asyncio.gather(
                delete(service, record), delete(service, record)
            )
        assert first == second and first.status == "deleted"
        remove.assert_awaited_once()
        assert (await service.runner.execution_state(identifier)).status == "cancelled"


async def test_tool_self_deletion_fails_without_deadlock(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        runner = service.runner
        record = await (await runner.project()).worktrees.create(
            m.Create(name="Self"), key="self"
        )
        result = await asyncio.wait_for(
            runner.run(
                flows.REFERENCE + "delete_tool",
                input={"worktree_id": str(record.id), "force": True},
                worktree_id=record.id,
            ),
            5,
        )
        response = connect.ToolResultMessage.model_validate(result)
        assert response.is_error and "still in use" in str(response.content)
        assert Path(record.path).is_dir()


@pytest.mark.parametrize("binding", ["root", "child"])
@pytest.mark.parametrize("cold", [False, True])
@pytest.mark.parametrize("force", [False, True])
async def test_llm_deletion_never_cancels_waiting_workflows(
    worktree_project: RunnerProject,
    worktree_probe: cases.Probe,
    binding: str,
    cold: bool,
    force: bool,
) -> None:
    async def refused(service: RunnerService, record: m.Record, victim: str) -> None:
        manager = (await service.runner.project()).worktrees
        before_record = await manager.get(m.Get(worktree_id=record.id))
        before = (
            await service.runner.pending_inputs(
                mailbox.RequestQuery(execution_id=victim)
            )
        ).requests
        result = await asyncio.wait_for(
            service.runner.run(
                flows.REFERENCE + "delete_tool",
                input={"worktree_id": str(record.id), "force": force},
            ),
            5,
        )
        response = connect.ToolResultMessage.model_validate(result)
        assert response.is_error and "still in use" in str(response.content)
        assert (await service.runner.execution_state(victim)).status == "paused"
        assert (
            await service.runner.pending_inputs(
                mailbox.RequestQuery(execution_id=victim)
            )
        ).requests == before
        assert Path(record.path).is_dir()
        assert await manager.get(m.Get(worktree_id=record.id)) == before_record

    async with RunnerService(worktree_project.root) as service:
        record = await (await service.runner.project()).worktrees.create(
            m.Create(name="Waiting child"), key="waiting"
        )
        if binding == "root":
            victim = await start(
                service,
                flows.REFERENCE + "paused",
                "victim",
                worktree_id=str(record.id),
            )
        else:
            victim = await start(
                service,
                flows.REFERENCE + "child_paused",
                "victim",
                input={"worktree_id": str(record.id)},
            )
        await status(service, victim, "paused")
        if not cold:
            await refused(service, record, victim)
    if cold:
        async with RunnerService(worktree_project.root) as service:
            assert (
                await service.dispatch("executions.current", {"execution_id": victim})
                is None
            )
            await refused(service, record, victim)


async def test_force_deletion_refuses_unconfirmed_cleanup(manager: Manager) -> None:
    record = await manager.create(m.Create(name="Unconfirmed"), key="unconfirmed")
    binding = manager.use(record.id, "execution")
    await binding.__aenter__()

    class Control:
        async def prepare_worktree_deletion(
            self,
            worktree: m.Record,
            execution_ids: set[str],
            *,
            force: bool,
            cancel: bool,
        ) -> Callable[[], Awaitable[None]]:
            assert force and cancel and execution_ids == {"execution"}

            async def cleanup() -> None:
                error = RuntimeError("unconfirmed process cleanup")
                await binding.__aexit__(RuntimeError, error, None)

            return cleanup

    try:
        with pytest.raises(m.WorktreeError, match="cleanup did not release"):
            await manager.delete(
                m.Delete(worktree_id=record.id, force=True),
                workflows=Control(),
                cancel_workflows=True,
            )
        assert Path(record.path).is_dir()
        assert (await manager.get(m.Get(worktree_id=record.id))).status == "ready"
    finally:
        await binding.__aexit__(None, None, None)


async def test_cancellation_timeout_never_deletes_files(manager: Manager) -> None:
    record = await manager.create(m.Create(name="Timeout"), key="timeout")
    manager.settings = m.Settings(timeout_seconds=0.05)

    class Control:
        async def prepare_worktree_deletion(
            self,
            worktree: m.Record,
            execution_ids: set[str],
            *,
            force: bool,
            cancel: bool,
        ) -> Callable[[], Awaitable[None]]:
            async def cleanup() -> None:
                await asyncio.Event().wait()

            return cleanup

    with pytest.raises(m.WorktreeError, match="timed out stopping"):
        await manager.delete(
            m.Delete(worktree_id=record.id, force=True),
            workflows=Control(),
            cancel_workflows=True,
        )
    assert Path(record.path).is_dir()
    assert (await manager.get(m.Get(worktree_id=record.id))).status == "ready"
