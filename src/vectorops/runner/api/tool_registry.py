"""Import-time factory definitions, never shared tool instances or live resources."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from functools import wraps
import inspect
from threading import RLock
import typing

import pydantic

from vectorops.runner import tool_authorization
from . import tool_contracts

if typing.TYPE_CHECKING:
    from .tool_contracts import ToolBinding


class ToolConfig(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


FactoryValue: typing.TypeAlias = typing.Union["ToolBinding", Sequence["ToolBinding"]]
FactoryResult: typing.TypeAlias = typing.Union[FactoryValue, Awaitable[FactoryValue]]
F = typing.TypeVar("F", bound=Callable[..., FactoryResult])


@dataclass(frozen=True)
class Registration:
    name: str
    module: str
    config: type[pydantic.BaseModel]
    build: Callable[[pydantic.BaseModel], Awaitable[list[ToolBinding]]]


_REGISTRY: typing.Final[dict[str, Registration]] = {}
_LOCK: typing.Final = RLock()


def register_tool(
    name: str,
    *,
    config: typing.Optional[type[pydantic.BaseModel]] = None,
    auth_context: typing.Optional[tool_authorization.AuthContextCallback] = None,
) -> Callable[[F], F]:
    """Register a no-argument factory, or a factory accepting one validated config.

    Without an auth callback the original callable is preserved. Factories run inside the workflow, so any
    external discovery must use recorded activities. Registration itself does no I/O.
    """
    if not name.isidentifier():
        raise ValueError("tool registration name must be an identifier")

    def decorate(factory: F) -> F:
        selected = factory
        if auth_context is not None:
            callback = auth_context

            def attach(value: FactoryValue) -> FactoryValue:
                if isinstance(value, Sequence):
                    return [
                        tool_contracts.with_auth_context(item, callback)
                        for item in value
                    ]
                return tool_contracts.with_auth_context(
                    typing.cast("ToolBinding", value), callback
                )

            @wraps(factory)
            def wrapped(*args: object, **kwargs: object) -> FactoryResult:
                value = factory(*args, **kwargs)
                if inspect.isawaitable(value):

                    async def resolve() -> FactoryValue:
                        return attach(await value)

                    return resolve()
                return attach(value)

            selected = typing.cast(F, wrapped)

        async def build(options: pydantic.BaseModel) -> list[ToolBinding]:
            value = selected() if config is None else selected(options)
            result = await value if inspect.isawaitable(value) else value
            if isinstance(result, Sequence):
                return list(result)
            return [typing.cast("ToolBinding", result)]

        with _LOCK:
            if name in _REGISTRY:
                raise ValueError(f"tool factory already registered: {name}")
            _REGISTRY[name] = Registration(
                name, factory.__module__, config or ToolConfig, build
            )
        return selected

    return decorate


def registrations() -> dict[str, Registration]:
    with _LOCK:
        return dict(_REGISTRY)
