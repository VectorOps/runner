"""Bounded catalog search and immutable activation from recorded tool results."""

from collections.abc import Sequence
import json
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api.llm import tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from .models import digest
from .binding import MCPTool


class Search(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)
    query: str = pydantic.Field(default="", max_length=512)
    limit: int = pydantic.Field(default=5, ge=1, le=20)


class Match(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")
    name: str
    description: str


class RankedSearch(Search):
    source: Optional[str] = None


class Ranking(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    name_weight: float = pydantic.Field(default=4, ge=0, allow_inf_nan=False)
    title_weight: float = pydantic.Field(default=2, ge=0, allow_inf_nan=False)
    description_weight: float = pydantic.Field(default=1, ge=0, allow_inf_nan=False)
    schema_weight: float = pydantic.Field(default=0.5, ge=0, allow_inf_nan=False)
    min_score: float = pydantic.Field(default=0, ge=0, allow_inf_nan=False)


class SearchResult(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")
    version: Literal[1] = 1
    catalog_digest: str
    tools: list[Match] = pydantic.Field(max_length=20)


class SearchTool:
    def __init__(
        self, definitions: Sequence[connect.ToolSpec], *, max_active: int
    ) -> None:
        self._max_active = max_active
        self._definitions = tuple(item.model_copy(deep=True) for item in definitions)
        self.catalog_digest = digest(
            {
                "definitions": [
                    item.model_dump(mode="json") for item in self._definitions
                ],
                "max_active": max_active,
            }
        )
        self._definition = connect.ToolSpec.external(
            name=f"mcp_discover_{self.catalog_digest[:24]}",
            description=(
                "Search the selected MCP tool catalog by keywords. Matches become callable "
                "on the next round, not this round, and still require independent approval. "
                f"At most {max_active} matching tools remain active; later searches replace older activations."
            ),
            input_schema=Search.model_json_schema(),
        )

    @property
    def definition(self) -> connect.ToolSpec:
        return self._definition.model_copy(deep=True)

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> Optional[tools.ToolApproval]:
        return tools.ToolApproval()

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        Search.model_validate(arguments)

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        query = Search.model_validate(arguments)
        terms = query.query.casefold().split()
        ranked: list[tuple[int, str, connect.ToolSpec]] = []
        for definition in self._definitions:
            text = f"{definition.name} {definition.description}".casefold()
            score = sum(term in text for term in terms)
            if terms and not score:
                continue
            ranked.append((-score, definition.name, definition))
        ranked.sort(key=lambda item: (item[0], item[1]))
        result = SearchResult(
            catalog_digest=self.catalog_digest,
            tools=[
                Match(name=item.name, description=item.description[:1024])
                for _, _, item in ranked[: min(query.limit, self._max_active)]
            ],
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.model_dump_json())]
        )


class Discovery:
    """Immutable, per-run activation over a workflow-recorded, selected MCP catalog.

    Reuse starts with search-only visibility. Explicitly call api.mcp.dynamic again
    to refresh the catalog; remote notifications never change this snapshot.
    """

    def __init__(
        self,
        bindings: Sequence[tools.ToolBinding],
        *,
        max_active: int = 32,
        ranking: Optional[Ranking] = None,
    ) -> None:
        if (
            isinstance(max_active, bool)
            or not isinstance(max_active, int)
            or not 1 <= max_active <= 256
        ):
            raise ValueError("max_active must be between 1 and 256")
        if len(bindings) > 4096:
            raise ValueError("dynamic catalog exceeds 4096 tools")
        self._available = {binding.definition.name: binding for binding in bindings}
        if len(self._available) != len(bindings):
            raise ValueError("duplicate dynamic tool names")
        self._max_active = max_active
        self._search = SearchTool(
            [self._available[name].definition for name in sorted(self._available)],
            max_active=max_active,
        )
        if ranking is not None:
            self._search = RankedSearchTool(
                bindings, max_active=max_active, ranking=ranking
            )
        self._active: tuple[str, ...] = ()

    @property
    def bindings(self) -> Sequence[tools.ToolBinding]:
        return (self._search, *(self._available[name] for name in self._active))

    def advance(self, results: Sequence[connect.ToolResultMessage]) -> "Discovery":
        active = list(self._active)
        for result in results:
            if result.tool_name != self._search.definition.name or result.is_error:
                continue
            if len(result.content) != 1 or not isinstance(
                result.content[0], connect.TextBlock
            ):
                raise ValueError("invalid recorded MCP discovery result")
            payload = SearchResult.model_validate_json(result.content[0].text)
            if payload.catalog_digest != self._search.catalog_digest:
                raise ValueError("MCP discovery catalog mismatch")
            for match in payload.tools:
                if match.name not in self._available:
                    raise ValueError("MCP discovery selected an unavailable tool")
                if match.name in active:
                    active.remove(match.name)
                active.append(match.name)
            active = active[-self._max_active :]
        updated = object.__new__(Discovery)
        updated._available = self._available
        updated._max_active = self._max_active
        updated._search = self._search
        updated._active = tuple(active)
        return updated


class RankedSearchTool(SearchTool):
    def __init__(
        self,
        bindings: Sequence[tools.ToolBinding],
        *,
        max_active: int,
        ranking: Ranking,
    ) -> None:
        self._ranking = ranking.model_copy(deep=True)
        self._documents: dict[str, tuple[str, str, str, str, str]] = {}
        definitions = [binding.definition for binding in bindings]
        super().__init__(definitions, max_active=max_active)
        for binding in bindings:
            definition = binding.definition
            if isinstance(binding, MCPTool):
                descriptor = binding.descriptor
                self._documents[definition.name] = (
                    binding.source,
                    descriptor.remote_name,
                    descriptor.title or "",
                    definition.description,
                    json.dumps(descriptor.input_schema, sort_keys=True),
                )
            else:
                self._documents[definition.name] = (
                    "",
                    definition.name,
                    "",
                    definition.description,
                    json.dumps(definition.input_schema, sort_keys=True),
                )
        self.catalog_digest = digest(
            {
                "base": self.catalog_digest,
                "ranking": ranking.model_dump(mode="json"),
                "documents": {
                    name: list(parts) for name, parts in self._documents.items()
                },
            }
        )
        self._definition = self._definition.model_copy(
            update={
                "name": f"mcp_discover_{self.catalog_digest[:24]}",
                "input_schema": RankedSearch.model_json_schema(),
            }
        )

    @property
    def version(self) -> int:
        return 2

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        RankedSearch.model_validate(arguments)

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        query = RankedSearch.model_validate(arguments)
        terms = query.query.casefold().split()
        weights = (
            self._ranking.name_weight,
            self._ranking.title_weight,
            self._ranking.description_weight,
            self._ranking.schema_weight,
        )
        matches: list[tuple[float, str, connect.ToolSpec]] = []
        for definition in self._definitions:
            source, *fields = self._documents[definition.name]
            if query.source is not None and query.source != source:
                continue
            score = sum(
                weight * sum(term in text.casefold() for term in terms)
                for weight, text in zip(weights, fields)
            )
            if terms and (score <= 0 or score < self._ranking.min_score):
                continue
            matches.append((-score, definition.name, definition))
        matches.sort(key=lambda item: (item[0], item[1]))
        result = SearchResult(
            catalog_digest=self.catalog_digest,
            tools=[
                Match(name=item.name, description=item.description[:1024])
                for _, _, item in matches[: min(query.limit, self._max_active)]
            ],
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=result.model_dump_json())]
        )
