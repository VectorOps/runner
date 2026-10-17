"""Immutable index segments and transactional generation construction."""

from array import array
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
import threading
import time
from types import MappingProxyType
from typing import Optional

from . import discovery
from .models import SearchConfig


def character_mask(text: str) -> int:
    mask = 0
    for character in text:
        mask |= 1 << (ord(character) % 128)
    return mask


def trigrams(text: str) -> set[str]:
    return {text[i : i + 3] for i in range(len(text) - 2)}


@dataclass(frozen=True, slots=True)
class Entry:
    path: str
    normalized: str
    basename: str
    basename_start: int
    boundaries: frozenset[int]
    characters: int
    cost: int

    @classmethod
    def create(cls, path: str) -> "Entry":
        normalized = path.casefold()
        boundaries: set[int] = set()
        offset = 0
        for i, character in enumerate(path):
            if (
                i == 0
                or not path[i - 1].isalnum()
                or (path[i - 1].islower() and character.isupper())
            ):
                boundaries.add(offset)
            offset += len(character.casefold())
        start = normalized.rfind("/") + 1
        return cls(
            path,
            normalized,
            normalized[start:],
            start,
            frozenset(boundaries),
            character_mask(normalized),
            512 + len(path.encode("utf-8", errors="surrogatepass")) * 128,
        )


@dataclass(frozen=True, slots=True, eq=False)
class Segment:
    entries: tuple[Entry, ...]
    postings: Mapping[str, bytes]

    @classmethod
    def create(cls, entries: Iterable[Entry]) -> "Segment":
        values = tuple(entries)
        postings: dict[str, array[int]] = {}
        for i, entry in enumerate(values):
            for trigram in trigrams(entry.normalized):
                postings.setdefault(trigram, array("I")).append(i)
        return cls(
            values,
            MappingProxyType({key: value.tobytes() for key, value in postings.items()}),
        )


@dataclass(frozen=True, slots=True, eq=False)
class IndexView:
    segments: tuple[Segment, ...] = ()


@dataclass(frozen=True, slots=True)
class RefreshOutcome:
    view: IndexView
    error: Optional[str] = None
    limited: bool = False


def build_refresh(
    root: Path,
    config: SearchConfig,
    baseline: IndexView,
    cancel: threading.Event,
    publish: Callable[[IndexView], None],
) -> RefreshOutcome:
    """Publish additive views, then commit a complete generation or roll back."""
    deadline = time.monotonic() + config.scan_timeout
    existing = {
        entry.path: entry for segment in baseline.segments for entry in segment.entries
    }
    known = set(existing)
    seen: set[str] = set()
    cost = sum(entry.cost for entry in existing.values())
    segments = list(baseline.segments)
    pending: list[Entry] = []
    last_publication = time.monotonic()
    paths = discovery.discover(root, config, cancel, deadline)
    try:
        for path in paths:
            discovery.check_scan(cancel, deadline)
            if path in seen:
                continue
            seen.add(path)
            if path not in known:
                if len(known) >= config.max_candidates:
                    raise discovery.ScanError("Candidate limit exceeded", limited=True)
                entry = Entry.create(path)
                if cost + entry.cost > config.max_memory_bytes:
                    raise discovery.ScanError(
                        "Index memory budget exceeded", limited=True
                    )
                known.add(path)
                cost += entry.cost
                pending.append(entry)
            if len(pending) >= config.batch_size:
                segments.append(Segment.create(pending))
                pending.clear()
            if time.monotonic() - last_publication >= config.publication_interval:
                if pending:
                    segments.append(Segment.create(pending))
                    pending.clear()
                publish(IndexView(tuple(segments)))
                last_publication = time.monotonic()
        if pending:
            segments.append(Segment.create(pending))
        committed: list[Segment] = []
        for segment in segments:
            discovery.check_scan(cancel, deadline)
            retained = tuple(entry for entry in segment.entries if entry.path in seen)
            if retained:
                committed.append(
                    segment
                    if len(retained) == len(segment.entries)
                    else Segment.create(retained)
                )
        discovery.check_scan(cancel, deadline)
        return RefreshOutcome(IndexView(tuple(committed)))
    except discovery.ScanError as error:
        return RefreshOutcome(baseline, str(error), error.limited)
    except Exception as error:
        return RefreshOutcome(baseline, f"{type(error).__name__}: {error}")
    finally:
        paths.close()
