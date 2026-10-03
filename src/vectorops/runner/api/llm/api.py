"""Workflow-local conversations reconstructed by deterministic activity replay."""

from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import aclosing, contextmanager
import asyncio
import hashlib
import json
import typing
from typing import Optional

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.llm import models, provider, tools as tool_api
from vectorops.runner.api.llm.configuration import (
    explicit_options,
    resolve_configuration,
)
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem


def _encoded(value: pydantic.JsonValue) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: pydantic.JsonValue) -> str:
    return hashlib.sha256(_encoded(value).encode()).hexdigest()


class LLM:
    def __init__(
        self,
        identity: str,
        settings: Optional[
            typing.Union[models.Settings, models.SettingsOverrides]
        ] = None,
        *,
        system_prompt: Optional[str] = None,
        backend: Optional[provider.Provider] = None,
        compaction: typing.Union[
            models.Compaction, None, models.Unset
        ] = models.Unset.VALUE,
        **overrides: typing.Unpack[models.SettingsParameters],
    ) -> None:
        context = c.workflow_context()
        if not identity.strip():
            raise ValueError("LLM identity must not be blank")
        self._context = context
        self._identity = identity
        self._overrides = explicit_options(settings, compaction=compaction, **overrides)
        self._settings: Optional[models.Settings] = None
        self._system_prompt = system_prompt
        self._backend = backend if backend is not None else provider.ConnectProvider()
        self._compaction: Optional[models.Compaction] = None
        self._messages: list[connect.Message] = []
        self._usage = connect.Usage()
        self._busy = False

    def _check_context(self) -> None:
        if c.workflow_context() is not self._context:
            raise RuntimeError("LLM belongs to a different workflow context")

    @contextmanager
    def _mutation(self) -> Iterator[None]:
        self._check_context()
        if self._busy:
            raise RuntimeError(
                "overlapping mutations on the same LLM are not supported"
            )
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    @property
    def settings(self) -> models.Settings:
        self._check_context()
        if self._settings is None:
            raise RuntimeError(
                "LLM settings are resolved at the first generation; use await resolve_settings() to read them separately"
            )
        return self._settings.model_copy(deep=True)

    async def _configure(self) -> models.Configuration:
        resolved = await resolve_configuration(self._overrides)
        self._settings = models.Settings.model_validate(
            resolved.model_dump(exclude={"compaction", "max_rounds"})
        )
        self._compaction = resolved.compaction
        return resolved

    @property
    def history(self) -> tuple[connect.Message, ...]:
        self._check_context()
        return tuple(message.model_copy(deep=True) for message in self._messages)

    @property
    def usage(self) -> connect.Usage:
        self._check_context()
        return self._usage.model_copy(deep=True)

    def set_system_prompt(self, prompt: Optional[str]) -> None:
        with self._mutation():
            self._system_prompt = prompt

    def add_message(self, message: connect.Message) -> None:
        with self._mutation():
            self._messages.append(message.model_copy(deep=True))

    def add_user_message(self, text: str) -> None:
        self.add_message(connect.UserMessage(content=text))

    def _request(
        self, definitions: Sequence[connect.ToolSpec]
    ) -> connect.GenerateRequest:
        assert self._settings is not None
        return connect.GenerateRequest(
            messages=[message.model_copy(deep=True) for message in self._messages],
            system_prompt=self._system_prompt,
            tools=[definition.model_copy(deep=True) for definition in definitions],
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
            reasoning=self._settings.reasoning,
        )

    def _input(self, request: connect.GenerateRequest) -> dict[str, pydantic.JsonValue]:
        assert self._settings is not None
        return {
            "instance": self._identity,
            "request_digest": _digest(
                {
                    "request": request.model_dump(mode="json"),
                    "settings": self._settings.model_dump(mode="json"),
                }
            ),
        }

    async def _invoke(
        self, call: c.ActivityCall, request: connect.GenerateRequest
    ) -> connect.AssistantMessage:
        async def invoke(
            ctx: ActivityExecutionContext,
        ) -> AsyncGenerator[StreamItem, None]:
            assert self._settings is not None
            async with aclosing(
                self._backend.stream(ctx, self._settings, request)
            ) as stream:
                async for item in stream:
                    if isinstance(item, c.ActivityFinal):
                        response = connect.AssistantMessage.model_validate(item.value)
                        provider.validate_response(response)
                    yield item

        result = await call.stream(input=self._input(request), invoke=invoke)
        return connect.AssistantMessage.model_validate(result)

    def _account(self, response: connect.AssistantMessage) -> None:
        self._usage = connect.accumulate_usage(self._usage, response.usage)

    @c.activity_wrapper(kind="llm.generate", version=1)
    async def _generate(
        self, call: c.ActivityCall, definitions: Sequence[connect.ToolSpec]
    ) -> connect.AssistantMessage:
        response = await self._invoke(call, self._request(definitions))
        self._messages.append(response.model_copy(deep=True))
        self._account(response)
        return response

    async def generate(
        self, *, tools: Sequence[connect.ToolSpec] = ()
    ) -> connect.AssistantMessage:
        with self._mutation():
            await self._configure()
            await self._maybe_compact()
            return await self._generate(tools)

    async def run(
        self,
        *,
        tools: Sequence[tool_api.ToolBinding] = (),
        max_rounds: Optional[int] = None,
    ) -> connect.AssistantMessage:
        """Run parallel tool rounds, committing results in model-call order.

        Infrastructure failures propagate after all calls settle; no partial
        round is appended to the conversation. Recorded successes remain reusable.
        """
        if max_rounds is not None and (
            isinstance(max_rounds, bool)
            or not isinstance(max_rounds, int)
            or max_rounds < 1
        ):
            raise ValueError("max_rounds must be positive")
        bindings = {tool.definition.name: tool for tool in tools}
        if len(bindings) != len(tools):
            raise ValueError("duplicate tool names")
        definitions = [tool.definition for tool in tools]
        with self._mutation():
            resolved = await self._configure()
            limit = resolved.max_rounds if max_rounds is None else max_rounds
            for index in range(limit):
                if index:
                    await self._configure()
                await self._maybe_compact()
                response = await self._generate(definitions)
                calls = [
                    block for block in response.content if block.type == "tool_call"
                ]
                if not calls:
                    return response
                results = await asyncio.gather(
                    *(
                        tool_api.invoke_tool(
                            bindings.get(request.name), request, instance=self._identity
                        )
                        for request in calls
                    ),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
                self._messages.extend(
                    result
                    for result in results
                    if isinstance(result, connect.ToolResultMessage)
                )
        raise models.RoundLimitError(f"LLM exceeded max_rounds={limit}")

    async def _maybe_compact(self) -> None:
        """Compact an old prefix without splitting assistant/tool exchanges."""
        policy = self._compaction
        if policy is None:
            return
        transcript: list[pydantic.JsonValue] = [
            message.model_dump(mode="json") for message in self._messages
        ]
        if len(_encoded(transcript)) <= policy.max_context_characters:
            return
        limit = len(self._messages) - policy.keep_recent_messages
        pending: set[str] = set()
        boundaries: list[int] = []
        for index, message in enumerate(self._messages, start=1):
            if message.role == "assistant":
                pending.update(
                    block.id for block in message.content if block.type == "tool_call"
                )
            elif message.role == "tool":
                pending.discard(message.tool_call_id)
            if not pending and index <= limit:
                boundaries.append(index)
        if boundaries:
            await self._compact(max(boundaries), policy.instruction)

    @c.activity_wrapper(kind="llm.compact", version=1)
    async def _compact(self, call: c.ActivityCall, cut: int, instruction: str) -> None:
        assert self._settings is not None
        prefix: list[pydantic.JsonValue] = [
            message.model_dump(mode="json") for message in self._messages[:cut]
        ]
        request = connect.GenerateRequest(
            system_prompt=instruction,
            messages=[connect.UserMessage(content=_encoded(prefix))],
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
            reasoning=self._settings.reasoning,
        )
        response = await self._invoke(call, request)
        text = "".join(block.text for block in response.content if block.type == "text")
        if not text.strip() or any(
            block.type == "tool_call" for block in response.content
        ):
            raise models.LLMResponseError(
                "compaction requires a nonempty tool-free summary"
            )
        self._messages = [
            connect.UserMessage(content="Summary of earlier conversation:\n" + text),
            *self._messages[cut:],
        ]
        self._account(response)
