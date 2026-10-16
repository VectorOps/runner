import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import patch

import connect
import pytest

from tests.api.test_llm import FakeProvider, answer
from tests.control.test_client_commands import pending, settled
from tests.fixtures import sources
from tests.fixtures.user_input import reply
from vectorops.runner.api import chat_input, llm
from vectorops.runner.api.llm import provider
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import inbox
from vectorops.runner import models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.streaming import StreamItem


def payload(text: str) -> chat_input.UserMessageInput:
    return chat_input.UserMessageInput(
        content=[chat_input.TextInput(type="text", text=text)]
    )


async def test_host_management_and_live_steering(tmp_path: Path) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "test"\n')
    started = asyncio.Event()
    release = asyncio.Event()

    class Blocked(FakeProvider):
        async def stream(
            self,
            context: ActivityExecutionContext,
            settings: llm.Settings,
            request: connect.GenerateRequest,
        ) -> AsyncGenerator[StreamItem, None]:
            if not self.requests:
                started.set()
                await release.wait()
            async for item in super().stream(context, settings, request):
                yield item

    backend = Blocked([answer("initial"), answer("steered"), answer("idle")])
    identifier: str
    with patch.object(provider, "ConnectProvider", return_value=backend):
        async with client.embedded(tmp_path) as host:
            accepted = await host.start(
                protocol.Start(reference="builtin:agent", idempotency_key="agent")
            )
            identifier = accepted.execution_id
            request = await pending(host, identifier)
            listing = await host.list_mailboxes(identifier)
            assert len(listing.mailboxes) == 1
            address = listing.mailboxes[0].address
            assert (
                address.workflow_id
                == (await host.execution(identifier)).root_workflow_id
            )
            await reply(host, request, "hello")
            await asyncio.wait_for(started.wait(), 2)
            first = await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("discard one"))
            )
            second = await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("discard two"))
            )
            third = await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("change direction"))
            )
            assert (await host.inspect_mailbox(address)).queued_messages == 3
            page = await host.list_messages(
                inbox.MessageQuery(**address.model_dump(), limit=1)
            )
            assert page.messages == (first,)
            assert page.next_after == first.id
            assert (
                await host.remove_message(
                    inbox.MessageRef(**address.model_dump(), message_id=second.id)
                )
                == second
            )
            assert await host.pop_message(address) == first
            assert (
                await host.list_messages(inbox.MessageQuery(**address.model_dump()))
            ).messages == (third,)
            release.set()
            next_request = await pending(host, identifier)
            assert next_request.request_id != request.request_id
            assert len(backend.requests) == 2
            assert backend.requests[1].messages[-1].content == [
                connect.TextBlock(text="change direction")
            ]
            assert (await host.inspect_mailbox(address)).queued_messages == 0
            assert (
                await host.remove_message(
                    inbox.MessageRef(**address.model_dump(), message_id=third.id)
                )
                is None
            )
            await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("idle input"))
            )
            await asyncio.sleep(0.02)
            assert len(backend.requests) == 2
            assert (await host.inspect_mailbox(address)).queued_messages == 1
            assert (
                await pending(host, identifier)
            ).request_id == next_request.request_id
            await reply(host, next_request, "next turn")
            newest = await pending(host, identifier)
            assert newest.request_id != next_request.request_id
            assert len(backend.requests) == 3
            assert backend.requests[2].messages[-1].content == [
                connect.TextBlock(text="idle input")
            ]
            await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("lost on restart"))
            )
            assert (await host.inspect_mailbox(address)).queued_messages == 1
            with pytest.raises(protocol.RemoteError, match="not registered"):
                await host.send_message(
                    inbox.Send(
                        **address.model_copy(
                            update={"conversation_id": "unknown"}
                        ).model_dump(),
                        payload=payload("wrong"),
                    )
                )

        async with client.embedded(tmp_path) as host:
            await host.resume(
                protocol.Resume(execution_id=identifier, idempotency_key="resume")
            )
            restored = await pending(host, identifier)
            assert restored.request_id == newest.request_id
            assert len(backend.requests) == 3
            async with asyncio.timeout(5):
                while True:
                    try:
                        restored_listing = await host.list_mailboxes(identifier)
                        if restored_listing.mailboxes:
                            break
                    except protocol.RemoteError as exc:
                        if exc.code != "mailbox_conflict":
                            raise
                    await asyncio.sleep(0.01)
            assert restored_listing.mailboxes[0].queued_messages == 0
            await host.stop(identifier)
            await settled(host, identifier)
            with pytest.raises(protocol.RemoteError, match="not active"):
                await host.send_message(
                    inbox.Send(**address.model_dump(), payload=payload("late"))
                )


async def test_subagent_has_no_root_inbox(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "config.toml").write_text(
        "[components.tools.approval]\napprove_all = true\n"
    )
    (tmp_path / "workflows/parent.py").write_text(sources.read("agent_subagent.py"))
    async with client.embedded(tmp_path) as host:
        accepted = await host.start(
            protocol.Start(reference="parent:main", idempotency_key="parent")
        )
        request = await pending(host, accepted.execution_id)
        assert (await host.list_mailboxes(accepted.execution_id)).mailboxes == ()
        address = inbox.Address(
            execution_id=accepted.execution_id,
            workflow_id=request.workflow_id,
            conversation_id="chat",
        )
        with pytest.raises(protocol.RemoteError, match="root conversation"):
            await host.send_message(
                inbox.Send(**address.model_dump(), payload=payload("wrong target"))
            )
        await host.stop(accepted.execution_id)
        await settled(host, accepted.execution_id)


async def test_inbox_events_support_query_free_steering(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as host:
        await host.subscribe(
            protocol.Subscribe(
                kinds=["mailbox.state_updated", "mailbox.enqueued", "mailbox.removed"]
            )
        )
        accepted = await host.start(
            protocol.Start(reference="builtin:agent", idempotency_key="inbox-events")
        )
        async with asyncio.timeout(5):
            opened = (await host.next_event()).event
        assert opened["kind"] == "mailbox.state_updated"
        assert opened["status"] == "open"
        assert opened["queued_messages"] == 0
        address = inbox.Address.model_validate(opened["address"])
        assert address.execution_id == accepted.execution_id
        await pending(host, accepted.execution_id)
        message = await host.send_message(
            inbox.Send(**address.model_dump(), payload=payload("steer"))
        )
        async with asyncio.timeout(5):
            enqueued = (await host.next_event()).event
        assert enqueued["kind"] == "mailbox.enqueued"
        assert enqueued["message_id"] == message.id
        assert inbox.Message.model_validate(enqueued["message"]) == message
        await host.remove_message(
            inbox.MessageRef(**address.model_dump(), message_id=message.id)
        )
        async with asyncio.timeout(5):
            removed = (await host.next_event()).event
        assert removed["kind"] == "mailbox.removed"
        assert removed["message_id"] == message.id
        assert "message" not in removed
        assert removed["queued_messages"] == 0
        await host.stop(accepted.execution_id)
        async with asyncio.timeout(5):
            closed = (await host.next_event()).event
        assert closed["kind"] == "mailbox.state_updated"
        assert closed["status"] == "closed"
        assert closed["address"] == opened["address"]


async def test_legacy_agent_history_retains_original_activity_order(
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text('[components.llm]\nmodel = "test"\n')
    configure = WorkflowExecutor.configure

    async def legacy(
        executor: WorkflowExecutor, configuration: models.Workflow
    ) -> models.Workflow:
        return (await configure(executor, configuration)).model_copy(
            update={"inbox_version": None}
        )

    backend = FakeProvider([answer("old response"), answer("new response")])
    with patch.object(provider, "ConnectProvider", return_value=backend):
        with patch.object(WorkflowExecutor, "configure", legacy):
            async with client.embedded(tmp_path) as host:
                accepted = await host.start(
                    protocol.Start(reference="builtin:agent", idempotency_key="legacy")
                )
                request = await pending(host, accepted.execution_id)
                await reply(host, request, "old input")
                next_request = await pending(host, accepted.execution_id)
                assert (
                    await host.list_mailboxes(accepted.execution_id)
                ).mailboxes == ()

        async with client.embedded(tmp_path) as host:
            await host.resume(
                protocol.Resume(
                    execution_id=accepted.execution_id, idempotency_key="legacy-resume"
                )
            )
            restored = await pending(host, accepted.execution_id)
            assert restored.request_id == next_request.request_id
            await reply(host, restored, "new input")
            newest = await pending(host, accepted.execution_id)
            assert newest.request_id != restored.request_id
            assert len(backend.requests) == 2
            assert (await host.list_mailboxes(accepted.execution_id)).mailboxes == ()
            await host.stop(accepted.execution_id)
            await settled(host, accepted.execution_id)
