"""Immutable static bindings and composition of independently advancing tool sets."""

from collections.abc import Sequence
from dataclasses import dataclass

import connect

from .tool_contracts import ToolBinding, ToolSet


@dataclass(frozen=True)
class Static:
    bindings: tuple[ToolBinding, ...] = ()

    def advance(self, results: Sequence[connect.ToolResultMessage]) -> "Static":
        return self


@dataclass(frozen=True)
class Combined:
    sets: tuple[ToolSet, ...]

    @property
    def bindings(self) -> tuple[ToolBinding, ...]:
        bindings = tuple(
            binding for selected in self.sets for binding in selected.bindings
        )
        names = [binding.definition.name for binding in bindings]
        if len(set(names)) != len(names):
            raise ValueError("duplicate exposed tool name")
        return bindings

    def advance(self, results: Sequence[connect.ToolResultMessage]) -> "Combined":
        return Combined(tuple(selected.advance(results) for selected in self.sets))
