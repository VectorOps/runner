"""Worker-owned ranking and bounded complete-candidate prefix caching."""

from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
import heapq
import threading
from typing import Optional

from . import index
from .models import SearchResult


def subsequence(text: str, query: str) -> Optional[tuple[int, ...]]:
    positions: list[int] = []
    cursor = 0
    for character in query:
        found = text.find(character, cursor)
        if found < 0:
            return None
        positions.append(found)
        cursor = found + 1
    if positions:
        cursor = positions[-1]
        for i in range(len(query) - 2, -1, -1):
            cursor = text.rfind(query[i], 0, cursor)
            positions[i] = cursor
    return tuple(positions)


@dataclass(frozen=True, slots=True)
class Match:
    entry: index.Entry
    tier: int
    start: int
    positions: tuple[int, ...] = ()
    basename: bool = False

    def rank(self) -> tuple[int, int, int, int, int, int, str, str]:
        boundaries = sum(p in self.entry.boundaries for p in self.positions)
        consecutive = sum(
            b == a + 1 for a, b in zip(self.positions, self.positions[1:])
        )
        span = self.positions[-1] - self.positions[0] if self.positions else 0
        return (
            self.tier,
            -int(self.basename),
            -boundaries,
            -consecutive,
            span,
            len(self.entry.path),
            self.entry.normalized,
            self.entry.path,
        )


def _match(entry: index.Entry, query: str, literal: bool) -> Optional[Match]:
    if not query:
        return Match(entry, 0, 0)
    if literal:
        start = entry.basename.find(query)
        if start >= 0:
            tier = 0 if entry.basename == query else (1 if start == 0 else 2)
            return Match(entry, tier, entry.basename_start + start)
        start = entry.normalized.find(query)
        if start >= 0:
            return Match(entry, 3, start)
    positions = subsequence(entry.basename, query)
    if positions is not None:
        return Match(
            entry,
            4,
            0,
            tuple(p + entry.basename_start for p in positions),
            True,
        )
    positions = subsequence(entry.normalized, query)
    if positions is not None:
        return Match(entry, 4, 0, positions)
    return None


def _result(match: Match, query: str) -> SearchResult:
    positions = match.positions or tuple(range(match.start, match.start + len(query)))
    original_positions: list[int] = []
    for i, character in enumerate(match.entry.path):
        original_positions.extend([i] * len(character.casefold()))
    selected = sorted({original_positions[p] for p in positions})
    ranges: list[tuple[int, int]] = []
    for position in selected:
        if ranges and ranges[-1][1] == position:
            ranges[-1] = (ranges[-1][0], position + 1)
        else:
            ranges.append((position, position + 1))
    return SearchResult(path=match.entry.path, highlights=tuple(ranges))


class Matcher:
    def __init__(self, max_cache_candidates: int) -> None:
        self._budget = max_cache_candidates
        self._view = index.IndexView()
        self._cache: OrderedDict[str, tuple[tuple[int, int], ...]] = OrderedDict()

    def evaluate(
        self,
        view: index.IndexView,
        query: str,
        limit: int,
        cancel: threading.Event,
    ) -> tuple[tuple[SearchResult, ...], bool]:
        """Rank all verified candidates, retaining only top-K match objects."""
        if view is not self._view:
            self._view = view
            self._cache.clear()
        query = query.casefold()
        mask = index.character_mask(query)
        prefix = max(
            (key for key in self._cache if query.startswith(key)),
            key=len,
            default=None,
        )
        refined: Optional[set[tuple[int, int]]] = None
        if prefix is not None:
            refined = set(self._cache[prefix])
            self._cache.move_to_end(prefix)
        candidates: list[tuple[int, int]] = []
        cacheable = True
        count = 0

        def matches() -> Iterator[Match]:
            nonlocal cacheable, count
            for segment_id, segment in enumerate(view.segments):
                literal_ids: Optional[set[int]] = None
                for trigram in index.trigrams(query):
                    posting = segment.postings.get(trigram, b"")
                    ids = set(memoryview(posting).cast("I"))
                    literal_ids = ids if literal_ids is None else literal_ids & ids
                    if not literal_ids:
                        break
                for entry_id, entry in enumerate(segment.entries):
                    if cancel.is_set():
                        return
                    key = (segment_id, entry_id)
                    if refined is not None and key not in refined:
                        continue
                    if entry.characters & mask != mask:
                        continue
                    match = _match(
                        entry, query, literal_ids is None or entry_id in literal_ids
                    )
                    if match is not None:
                        count += 1
                        if cacheable:
                            candidates.append(key)
                            if len(candidates) > self._budget:
                                candidates.clear()
                                cacheable = False
                        yield match

        best = heapq.nsmallest(limit, matches(), key=Match.rank)
        if cacheable and not cancel.is_set():
            self._cache[query] = tuple(candidates)
            while (
                len(self._cache) > 8
                or sum(map(len, self._cache.values())) > self._budget
            ):
                self._cache.popitem(last=False)
        return tuple(_result(match, query) for match in best), count > limit
