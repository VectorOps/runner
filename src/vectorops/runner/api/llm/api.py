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
from vectorops.runner.api.llm import retry as retry_api
from vectorops.runner.api.llm import observability
from vectorops.runner.api.llm import compaction as compaction_api
from vectorops.runner.api.llm.configuration import (
    explicit_options,
    resolve_configuration,
)
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
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
        retry: Optional[models.RetryPolicy] = None,
        raise_tool_errors: bool = False,
        **overrides: typing.Unpack[models.SettingsParameters],
    ) -> None:
        context = c.workflow_context()
        if not identity.strip():
            raise ValueError("LLM identity must not be blank")
        self._context = context
        self._identity = identity
        self._overrides = explicit_options(settings, compaction=compaction, **overrides)
        if retry is not None:
            self._overrides["retry"] = retry.model_dump(mode="json", exclude_unset=True)
        self._retry = models.RetryPolicy()
        self._raise_tool_errors = raise_tool_errors
        self._settings: Optional[models.Settings] = None
        self._system_prompt = system_prompt
        self._backend = backend if backend is not None else provider.ConnectProvider()
        self._compaction: Optional[models.Compaction] = None
        self._messages: list[connect.Message] = []
        self._compaction_history = compaction_api.History()
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
            resolved.model_dump(exclude={"compaction", "max_rounds", "retry"})
        )
        self._compaction = resolved.compaction
        self._retry = resolved.retry
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

    def set_settings(self, settings: models.SettingsOverrides) -> None:
        """Replace generation overrides without resetting history or runtime policies.

        Omitted fields inherit workspace LLM settings on the next round instead
        of retaining a previous explicit value. Compaction and retry are unchanged.
        """
        with self._mutation():
            self._overrides = {
                **{
                    key: value
                    for key, value in self._overrides.items()
                    if key in ("compaction", "retry")
                },
                **explicit_options(settings),
            }

    def set_raise_tool_errors(self, enabled: bool) -> None:
        with self._mutation():
            self._raise_tool_errors = enabled

    def add_message(self, message: connect.Message) -> None:
        with self._mutation():
            self._messages.append(message.model_copy(deep=True))

    def add_user_message(self, text: str) -> None:
        self.add_message(connect.UserMessage(content=text))

    def _request(
        self, definitions: Sequence[connect.ToolSpec], *, snapshot: bool = False
    ) -> connect.GenerateRequest:
        assert self._settings is not None
        messages = [message.model_copy(deep=True) for message in self._messages]
        factory = (
            connect.GenerateRequest.model_construct
            if snapshot
            else connect.GenerateRequest
        )
        request = factory(
            messages=messages,
            system_prompt=self._system_prompt,
            tools=[definition.model_copy(deep=True) for definition in definitions],
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
            reasoning=self._settings.reasoning,
        )
        return (
            request.model_copy(update={"messages": messages}) if snapshot else request
        )

    def _input(
        self,
        request: connect.GenerateRequest,
        settings: Optional[models.Settings] = None,
    ) -> dict[str, pydantic.JsonValue]:
        assert self._settings is not None
        selected = self._settings if settings is None else settings
        payload = selected.model_dump(mode="json")
        if selected.credential_profile is None:
            payload.pop("credential_profile", None)
        return {
            "instance": self._identity,
            "request_digest": _digest(
                {
                    "request": request.model_dump(mode="json"),
                    "settings": payload,
                }
            ),
        }

    async def _invoke(
        self,
        call: c.ActivityCall,
        request: connect.GenerateRequest,
        *,
        compaction_request: Optional[connect.GenerateRequest] = None,
        cut: int = 0,
        settings: Optional[models.Settings] = None,
        measured: bool = False,
        summary: bool = False,
        prefix_tokens: Optional[int] = None,
    ) -> compaction_api.Generation:
        assert self._settings is not None
        selected = self._settings if settings is None else settings

        async def invoke(
            ctx: ActivityExecutionContext,
        ) -> AsyncGenerator[StreamItem, None]:
            observation = observability.Observation(selected, request)
            async with aclosing(
                retry_api.stream(
                    self._backend,
                    ctx,
                    selected,
                    request,
                    self._retry,
                    observation,
                )
            ) as stream:
                async for item in stream:
                    if summary and isinstance(item, c.ActivityFinal):
                        summary_response = connect.AssistantMessage.model_validate(
                            item.value
                        )
                        self._summary_text(summary_response)
                        assert self._settings is not None
                        size = compaction_api.summary_tokens(
                            summary_response, selected, self._settings
                        )
                        if (
                            size is not None
                            and prefix_tokens is not None
                            and size >= prefix_tokens
                        ):
                            raise models.LLMResponseError(
                                "compaction summary did not reduce the selected prefix"
                            )
                    if (
                        isinstance(item, c.ActivityFinal)
                        and compaction_request is not None
                    ):
                        response = connect.AssistantMessage.model_validate(item.value)
                        text = "".join(
                            block.text
                            for block in response.content
                            if block.type == "text"
                        )
                        if text.strip() and not any(
                            block.type == "tool_call" for block in response.content
                        ):
                            after = compaction_request.model_copy(
                                update={
                                    "messages": [
                                        connect.UserMessage(
                                            content="Summary of earlier conversation:\n"
                                            + text
                                        ),
                                        *compaction_request.messages[cut:],
                                    ]
                                }
                            )
                            assert item.metrics is not None
                            metric = observability.Metrics.model_validate(
                                item.metrics["llm"]
                            )
                            metric.compaction = observability.compaction_savings(
                                compaction_request, after
                            )
                            item = c.ActivityFinal(
                                item.value, {"llm": metric.model_dump(mode="json")}
                            )
                    if measured and isinstance(item, c.ActivityFinal):
                        assert self._compaction is not None
                        generation = compaction_api.Generation(
                            response=connect.AssistantMessage.model_validate(
                                item.value
                            ),
                            message_id=ctx.step_id,
                            context_window=compaction_api.context_window(
                                selected, self._compaction
                            ),
                        )
                        item = c.ActivityFinal(
                            generation.model_dump(mode="json"), item.metrics
                        )
                    yield item

        result = await call.stream(input=self._input(request, selected), invoke=invoke)
        if measured:
            return compaction_api.Generation.model_validate(result)
        return compaction_api.Generation(
            response=connect.AssistantMessage.model_validate(result), message_id=""
        )

    def _account(self, response: connect.AssistantMessage) -> None:
        self._usage = connect.accumulate_usage(self._usage, response.usage)

    @c.activity_wrapper(kind="llm.generate", version=1)
    async def _generate(
        self, call: c.ActivityCall, definitions: Sequence[connect.ToolSpec]
    ) -> connect.AssistantMessage:
        response = (await self._invoke(call, self._request(definitions))).response
        self._messages.append(response.model_copy(deep=True))
        self._account(response)
        return response

    def _signature(self, definitions: Sequence[connect.ToolSpec]) -> str:
        assert self._settings is not None
        return _digest(
            {
                "model": self._settings.model,
                "provider": self._settings.provider,
                "system_prompt": self._system_prompt,
                "tools": [
                    definition.model_dump(mode="json") for definition in definitions
                ],
                "reasoning": (
                    None
                    if self._settings.reasoning is None
                    else self._settings.reasoning.model_dump(mode="json")
                ),
            }
        )

    @c.activity_wrapper(kind="llm.generate", name="_generate", version=2)
    async def _generate_measured(
        self, call: c.ActivityCall, definitions: Sequence[connect.ToolSpec]
    ) -> connect.AssistantMessage:
        generation = await self._invoke(call, self._request(definitions), measured=True)
        self._compaction_history.record(
            generation, len(self._messages), self._signature(definitions)
        )
        self._messages.append(generation.response.model_copy(deep=True))
        self._account(generation.response)
        return generation.response

    async def _round(
        self, definitions: Sequence[connect.ToolSpec]
    ) -> connect.AssistantMessage:
        """Recover one recorded overflow without repeating tools or predicting fit."""
        policy = self._compaction
        if policy is None or policy.max_context_characters is not None:
            await self._maybe_compact(definitions)
            return await self._generate(definitions)
        try:
            response = await self._generate_measured(definitions)
        except ActivityError as exc:
            if (
                exc.error.type != "ContextLengthError"
                or not self._compaction_history.checkpoints
            ):
                raise
            anchor = self._compaction_history.checkpoints[-1]
            if not await self._compact_at(anchor.message_id, definitions, force=True):
                raise
            response = await self._generate_measured(definitions)
        if self._compaction_history.checkpoints:
            anchor = self._compaction_history.checkpoints[-1]
            if anchor.index == len(self._messages) - 1:
                await self._compact_at(anchor.message_id, definitions)
        return response

    async def generate(
        self, *, tools: Sequence[connect.ToolSpec] = ()
    ) -> connect.AssistantMessage:
        with self._mutation():
            await self._configure()
            return await self._round(tools)

    async def run(
        self,
        *,
        tools: Sequence[tool_api.ToolBinding] = (),
        tool_set: Optional[tool_api.ToolSet] = None,
        max_rounds: Optional[int] = None,
    ) -> connect.AssistantMessage:
        """Run parallel tool rounds, committing results in model-call order.

        Tool exceptions become model-visible responses by default. Strict mode
        and runner infrastructure failures propagate after all calls settle,
        without appending a partial round.
        Optional immutable tool sets advance after complete rounds, including
        replayed results, and expose new bindings only to the following round.
        """
        if max_rounds is not None and (
            isinstance(max_rounds, bool)
            or not isinstance(max_rounds, int)
            or max_rounds < 1
        ):
            raise ValueError("max_rounds must be positive")
        static = tuple(tools)
        selected = tool_set

        def resolve(
            current: Optional[tool_api.ToolSet],
        ) -> tuple[dict[str, tool_api.ToolBinding], list[connect.ToolSpec]]:
            available = [*static, *(current.bindings if current is not None else ())]
            bindings = {tool.definition.name: tool for tool in available}
            if len(bindings) != len(available):
                raise ValueError("duplicate tool names")
            return bindings, [tool.definition for tool in available]

        bindings, definitions = resolve(selected)
        with self._mutation():
            resolved = await self._configure()
            limit = resolved.max_rounds if max_rounds is None else max_rounds
            for index in range(limit):
                if index:
                    await self._configure()
                response = await self._round(definitions)
                calls = [
                    block for block in response.content if block.type == "tool_call"
                ]
                if not calls:
                    return response
                results = await asyncio.gather(
                    *(
                        tool_api.invoke_tool(
                            bindings.get(request.name),
                            request,
                            instance=self._identity,
                            raise_errors=self._raise_tool_errors,
                        )
                        for request in calls
                    ),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
                completed = [
                    result
                    for result in results
                    if isinstance(result, connect.ToolResultMessage)
                ]
                if selected is not None:
                    next_set = selected.advance(
                        tuple(result.model_copy(deep=True) for result in completed)
                    )
                    bindings, definitions = resolve(next_set)
                    selected = next_set
                self._messages.extend(completed)
        raise models.RoundLimitError(f"LLM exceeded max_rounds={limit}")

    async def _maybe_compact(self, definitions: Sequence[connect.ToolSpec]) -> None:
        """Compact an old prefix without splitting assistant/tool exchanges."""
        policy = self._compaction
        if policy is None or policy.max_context_characters is None:
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
            await self._compact(max(boundaries), policy.instruction, definitions)

    @c.activity_wrapper(kind="llm.compact", version=1)
    async def _compact(
        self,
        call: c.ActivityCall,
        cut: int,
        instruction: str,
        definitions: Sequence[connect.ToolSpec],
    ) -> None:
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
        response = (
            await self._invoke(
                call, request, compaction_request=self._request(definitions), cut=cut
            )
        ).response
        text = self._summary_text(response)
        self._messages = [
            connect.UserMessage(content="Summary of earlier conversation:\n" + text),
            *self._messages[cut:],
        ]
        self._account(response)

    @staticmethod
    def _summary_text(response: connect.AssistantMessage) -> str:
        text = "".join(block.text for block in response.content if block.type == "text")
        if not text.strip() or any(
            block.type == "tool_call" for block in response.content
        ):
            raise models.LLMResponseError(
                "compaction requires a nonempty tool-free summary"
            )
        return text

    async def _compact_at(
        self,
        message_id: str,
        definitions: Sequence[connect.ToolSpec],
        *,
        force: bool = False,
    ) -> bool:
        assert self._compaction is not None
        selection = self._compaction_history.select(
            self._messages,
            message_id,
            self._compaction,
            self._signature(definitions),
            force=force,
        )
        if selection is None:
            return False
        await self._compact_measured(selection, definitions)
        return True

    @c.activity_wrapper(kind="llm.compact", name="_compact", version=2)
    async def _compact_measured(
        self,
        call: c.ActivityCall,
        selection: compaction_api.Selection,
        definitions: Sequence[connect.ToolSpec],
    ) -> None:
        """Record one summary update, then atomically replace only its selected prefix."""
        assert self._settings is not None and self._compaction is not None
        policy = self._compaction
        values = self._settings.model_dump()
        values["max_output_tokens"] = policy.summary_max_output_tokens
        values.update(policy.summary_settings.model_dump(exclude_unset=True))
        if values["max_output_tokens"] is None:
            values["max_output_tokens"] = policy.summary_max_output_tokens
        settings = models.Settings.model_validate(values)
        start = int(self._compaction_history.has_summary)
        previous: pydantic.JsonValue = None
        if start:
            previous = self._messages[0].model_dump(mode="json")
        transcript: list[pydantic.JsonValue] = [
            message.model_dump(mode="json")
            for message in self._messages[start : selection.cut]
        ]
        request = connect.GenerateRequest(
            system_prompt=policy.instruction
            + (
                "\nUpdate the previous summary with the transcript. Preserve task intent, "
                "constraints, decisions, and unfinished work; remove superseded details. "
                "Use sections: Goal, Constraints, Progress, Decisions, Next steps, Critical context."
            ),
            messages=[
                connect.UserMessage(
                    content=_encoded(
                        {"previous_summary": previous, "transcript": transcript}
                    )
                )
            ],
            temperature=settings.temperature,
            max_output_tokens=settings.max_output_tokens,
            reasoning=settings.reasoning,
        )
        response = (
            await self._invoke(
                call,
                request,
                settings=settings,
                summary=True,
                prefix_tokens=selection.prefix_tokens,
                compaction_request=self._request(definitions, snapshot=True),
                cut=selection.cut,
            )
        ).response
        text = self._summary_text(response)
        summary_tokens = compaction_api.summary_tokens(
            response, settings, self._settings
        )
        self._compaction_history.commit(selection, summary_tokens)
        self._messages = [
            connect.UserMessage(content="Summary of earlier conversation:\n" + text),
            *self._messages[selection.cut :],
        ]
        self._account(response)
