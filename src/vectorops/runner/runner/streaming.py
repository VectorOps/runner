"""Typed full-state streaming updates and final activity results."""

from dataclasses import dataclass
import typing

import pydantic

T = typing.TypeVar("T", bound=pydantic.JsonValue)


@dataclass(frozen=True)
class ActivityDelta:
    """A complete replacement snapshot, not a patch to the preceding snapshot."""

    kind: typing.ClassVar[typing.Literal["delta"]] = "delta"
    value: pydantic.JsonValue


@dataclass(frozen=True)
class ActivityFinal(typing.Generic[T]):
    kind: typing.ClassVar[typing.Literal["final"]] = "final"
    value: T


StreamItem: typing.TypeAlias = typing.Union[
    ActivityDelta, ActivityFinal[pydantic.JsonValue]
]
