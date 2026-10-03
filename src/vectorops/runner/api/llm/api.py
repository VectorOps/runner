"""Workflow-local conversations reconstructed by deterministic activity replay."""

from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import aclosing, contextmanager
import hashlib
import json
from typing import Optional

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.llm import models, provider, tools
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
        settings: models.Settings,
        *,
        system_prompt: Optional[str] = None,
        backend: Optional[provider.Provider] = None,
        compaction: Optional[models.Compaction] = None,
    ) -> None:
        context = c.workflow_context()
        if not identity.strip():
            raise ValueError("LLM identity must not be blank")
        self._context = context
        self._identity = identity
        self._settings = settings.model_copy(deep=True)
        self._system_prompt = system_prompt
        self._backend = backend if backend is not None else provider.ConnectProvider()
        self._compaction = compaction.model_copy(deep=True) if compaction else None
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
        return connect.GenerateRequest(
            messages=[message.model_copy(deep=True) for message in self._messages],
            system_prompt=self._system_prompt,
            tools=[definition.model_copy(deep=True) for definition in definitions],
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
            reasoning=self._settings.reasoning,
        )

    def _input(self, request: connect.GenerateRequest) -> dict[str, pydantic.JsonValue]:
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
            await self._maybe_compact()
            return await self._generate(tools)

    async def run(
        self, *, tools: Sequence[tools.ToolBinding] = (), max_rounds: int = 100
    ) -> connect.AssistantMessage:
        """Generate and execute tools sequentially until a tool-free response arrives."""
        if max_rounds < 1:
            raise ValueError("max_rounds must be positive")
        bindings = {tool.definition.name: tool for tool in tools}
        if len(bindings) != len(tools):
            raise ValueError("duplicate tool names")
        definitions = [tool.definition for tool in tools]
        with self._mutation():
            for _ in range(max_rounds):
                await self._maybe_compact()
                response = await self._generate(definitions)
                calls = [
                    block for block in response.content if block.type == "tool_call"
                ]
                if not calls:
                    return response
                for request in calls:
                    await self._tool(request, bindings.get(request.name))
        raise models.RoundLimitError(f"LLM exceeded max_rounds={max_rounds}")

    @c.activity_wrapper(kind="llm.tool", version=1)
    async def _tool(
        self,
        call: c.ActivityCall,
        request: connect.ToolCallBlock,
        binding: Optional[tools.ToolBinding],
    ) -> None:
        async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
            try:
                if binding is None:
                    raise tools.ToolExecutionError(f"unknown tool: {request.name}")
                response = await binding.execute(ctx, request.arguments)
            except tools.ToolExecutionError as exc:
                response = tools.ToolResponse(
                    content=[connect.TextBlock(text=str(exc))], is_error=True
                )
            return connect.ToolResultMessage(
                tool_call_id=request.id,
                tool_name=request.name,
                content=response.content,
                is_error=response.is_error,
            ).model_dump(mode="json")

        result = await call.run(
            input={
                "instance": self._identity,
                "call_digest": _digest(request.model_dump(mode="json")),
                "tool_digest": (
                    _digest(binding.definition.model_dump(mode="json"))
                    if binding
                    else None
                ),
                "tool_version": binding.version if binding else None,
            },
            invoke=invoke,
        )
        self._messages.append(connect.ToolResultMessage.model_validate(result))

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
