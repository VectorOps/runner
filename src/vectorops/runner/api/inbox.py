"""Addressed steering inbox handles with replay-aware non-blocking reads."""

import typing
import pydantic

from vectorops.runner.api import composition as c, chat_input
from vectorops.runner.runner import inbox as runtime
from vectorops.runner.runner.activity_context import ActivityExecutionContext


class Inbox:
    def __init__(
        self, conversation_id: str, *, workflow_id: typing.Optional[str] = None
    ) -> None:
        context = c.workflow_context()
        self._context = context
        self._target = runtime.Target(
            conversation_id=conversation_id,
            workflow_id=context.workflow.id if workflow_id is None else workflow_id,
        )
        self._registry = context.inboxes
        if (
            self._registry is not None
            and self._target.workflow_id == self._registry.root_workflow_id
        ):
            self._registry.register(conversation_id)

    @property
    def address(self) -> runtime.Address:
        if self._registry is None:
            raise runtime.InboxConflict("inbox has no active execution address")
        return runtime.Address(
            execution_id=self._registry.execution_id, **self._target.model_dump()
        )

    def _check_context(self) -> None:
        if c.workflow_context() is not self._context:
            raise runtime.InboxConflict("inbox belongs to a different workflow context")

    async def drain(self) -> tuple[chat_input.UserMessageInput, ...]:
        self._check_context()
        if self._registry is None:
            return ()
        return await self._drain()

    @c.activity_wrapper(kind="mailbox.drain", name="drain")
    async def _drain(
        self, call: c.ActivityCall
    ) -> tuple[chat_input.UserMessageInput, ...]:
        """Record empty batches as well as messages, without polling during replay."""
        self._check_context()

        async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
            assert self._registry is not None
            return [
                message.payload.model_dump(mode="json")
                for message in self._registry.drain(self._target)
            ]

        result = await call.run(
            input=self._target.model_dump(mode="json"), invoke=invoke
        )
        return pydantic.TypeAdapter(
            tuple[chat_input.UserMessageInput, ...]
        ).validate_python(result)
