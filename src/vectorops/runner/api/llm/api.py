"""Workflow-local conversations reconstructed by deterministic activity replay."""

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
import asyncio
import hashlib
import json
import typing
from typing import Optional
from uuid import uuid4

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api import inbox as inbox_api
from vectorops.runner.api.llm import models, provider, tools as tool_api
from vectorops.runner.api.llm import invocation
from vectorops.runner.api.llm import compaction as compaction_api
from vectorops.runner.api.llm.configuration import (
    explicit_options,
    resolve_configuration,
)
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner import streaming


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
        profile: Optional[str] = None,
        system_prompt: Optional[str] = None,
        backend: Optional[provider.Provider] = None,
        compaction: typing.Union[
            models.Compaction, None, models.Unset
        ] = models.Unset.VALUE,
        retry: Optional[models.RetryPolicy] = None,
        raise_tool_errors: bool = False,
        inbox: Optional[inbox_api.Inbox] = None,
        **overrides: typing.Unpack[models.SettingsParameters],
    ) -> None:
        context = c.workflow_context()
        if not identity.strip():
            raise ValueError("LLM identity must not be blank")
        self._context = context
        self._identity = identity
        self._session_id = uuid4().hex
        self._profile = profile
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
        self._inbox = inbox

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
        resolved = await resolve_configuration(self._overrides, profile=self._profile)
        self._settings = resolved.generation_settings()
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

        Omitted fields inherit the selected profile and workspace LLM settings
        on the next round instead of retaining a previous explicit value.
        Profile selection, compaction, and retry overrides are unchanged.
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
        """Snapshot list membership; retry isolates messages at the provider boundary."""
        assert self._settings is not None
        messages = list(self._messages)
        factory = (
            connect.GenerateRequest.model_construct
            if snapshot
            else connect.GenerateRequest
        )
        request = factory(
            messages=messages,
            system_prompt=self._system_prompt,
            tools=list(definitions),
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
        request: typing.Union[
            connect.GenerateRequest, Callable[[], connect.GenerateRequest]
        ],
        *,
        settings: Optional[models.Settings] = None,
        finalize: Optional[invocation.Finalizer] = None,
    ) -> pydantic.JsonValue:
        """Prepare requests once for live work, never for terminal generation replay."""
        assert self._settings is not None
        selected = self._settings if settings is None else settings
        prepared: Optional[connect.GenerateRequest] = None

        def prepare() -> connect.GenerateRequest:
            nonlocal prepared
            if prepared is None:
                prepared = request() if callable(request) else request
            return prepared

        def provider_request() -> connect.GenerateRequest:
            return prepare().model_copy(
                update={"session": connect.SessionHints(session_id=self._session_id)}
            )

        return await invocation.invoke(
            call,
            lambda: self._input(prepare(), selected),
            self._backend,
            selected,
            provider_request,
            self._retry,
            finalize,
        )

    def _account(self, response: connect.AssistantMessage) -> None:
        self._usage = connect.accumulate_usage(self._usage, response.usage)

    @c.activity_wrapper(kind="llm.generate", version=1)
    async def _generate(
        self, call: c.ActivityCall, definitions: Sequence[connect.ToolSpec]
    ) -> connect.AssistantMessage:
        response = connect.AssistantMessage.model_validate(
            await self._invoke(call, lambda: self._request(definitions))
        )
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
        def measured(
            item: streaming.ActivityFinal[pydantic.JsonValue],
            ctx: ActivityExecutionContext,
        ) -> streaming.ActivityFinal[pydantic.JsonValue]:
            assert self._settings is not None and self._compaction is not None
            result = models.Generation(
                response=connect.AssistantMessage.model_validate(item.value),
                message_id=ctx.step_id,
                context_window=compaction_api.context_window(
                    self._settings, self._compaction
                ),
            )
            return streaming.ActivityFinal(result.model_dump(mode="json"), item.metrics)

        generation = models.Generation.model_validate(
            await self._invoke(
                call, lambda: self._request(definitions), finalize=measured
            )
        )
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
            anchor = self._compaction_history.latest_anchor()
            if exc.error.type != "ContextLengthError" or anchor is None:
                raise
            if not await self._compact_at(anchor, definitions, force=True):
                raise
            response = await self._generate_measured(definitions)
        anchor = self._compaction_history.latest_anchor(len(self._messages) - 1)
        if anchor is not None:
            await self._compact_at(anchor, definitions)
        return response

    async def generate(
        self, *, tools: Sequence[connect.ToolSpec] = ()
    ) -> connect.AssistantMessage:
        with self._mutation():
            await self._configure()
            await self._steer()
            return await self._round(tools)

    async def _steer(self) -> bool:
        if self._inbox is None:
            return False
        messages = await self._inbox.drain()
        self._messages.extend(message.to_message() for message in messages)
        return bool(messages)

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
                await self._steer()
                response = await self._round(definitions)
                calls = [
                    block for block in response.content if block.type == "tool_call"
                ]
                if not calls:
                    if index + 1 < limit and await self._steer():
                        continue
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
        cut = compaction_api.legacy_cut(self._messages, policy)
        if cut is not None:
            await self._compact(cut, policy.instruction, definitions)

    @c.activity_wrapper(kind="llm.compact", version=1)
    async def _compact(
        self,
        call: c.ActivityCall,
        cut: int,
        instruction: str,
        definitions: Sequence[connect.ToolSpec],
    ) -> None:
        assert self._settings is not None
        summary = compaction_api.legacy_summary(
            self._messages,
            cut,
            instruction,
            self._settings,
            self._request(definitions),
        )
        response = connect.AssistantMessage.model_validate(
            await self._invoke(
                call,
                summary.request,
                finalize=summary.finalize,
            )
        )
        text = compaction_api.summary_text(response)
        self._messages = compaction_api.replacement(self._messages, cut, text)
        self._account(response)

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
        summary = self._compaction_history.prepare(
            self._messages,
            selection,
            self._compaction,
            self._settings,
            self._request(definitions, snapshot=True),
        )
        response = connect.AssistantMessage.model_validate(
            await self._invoke(
                call,
                summary.request,
                settings=summary.settings,
                finalize=summary.finalize,
            )
        )
        text = compaction_api.summary_text(response)
        summary_tokens = compaction_api.summary_tokens(
            response, summary.settings, self._settings
        )
        self._compaction_history.commit(selection, summary_tokens)
        self._messages = compaction_api.replacement(self._messages, selection.cut, text)
        self._account(response)
