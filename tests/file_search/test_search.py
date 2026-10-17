import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time

import pydantic
import pytest

from vectorops.runner import file_search as fs
from vectorops.runner.file_search import discovery, index, matcher


@pytest.mark.parametrize("cancelled", [True, False])
def test_ripgrep_stalled_child_is_killed_and_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    original = subprocess.Popen
    children: list[subprocess.Popen[bytes]] = []
    stop = threading.Event()

    def stalled(
        command: list[str], *, cwd: Path, stdout: int, stderr: int
    ) -> subprocess.Popen[bytes]:
        assert command == ["rg", "--files", "--hidden", "-0", "-g", "!.git", "."]
        assert cwd == tmp_path
        child = original(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
        )
        children.append(child)
        if cancelled:
            stop.set()
        return child

    monkeypatch.setattr(discovery.subprocess, "Popen", stalled)
    before = time.monotonic()
    with pytest.raises(
        discovery.ScanError, match="cancelled" if cancelled else "deadline"
    ):
        list(
            discovery.discover(
                tmp_path,
                fs.SearchConfig(),
                stop,
                time.monotonic() + (5 if cancelled else 0.1),
            )
        )
    assert time.monotonic() - before < 3
    assert children and children[0].poll() is not None


def make_view(*paths: str) -> index.IndexView:
    return index.IndexView((index.Segment.create(map(index.Entry.create, paths)),))


async def completed(stream: AsyncIterator[fs.SearchUpdate]) -> fs.SearchUpdate:
    async with asyncio.timeout(5):
        async for update in stream:
            if not update.refreshing and update.refresh_id is not None:
                return update
    raise AssertionError("Subscription closed without a completed refresh")


async def snapshot(service: fs.FileSearchService, query: str = "") -> fs.SearchUpdate:
    stream = service.search(query)
    try:
        return await completed(stream)
    finally:
        await stream.aclose()


def paths(update: fs.SearchUpdate) -> set[str]:
    return {result.path for result in update.results}


def test_ranking_and_no_trigram_requirement() -> None:
    worker = matcher.Matcher(100)
    view = make_view(
        "src/foo", "src/foobar", "src/afoo", "foo/bar", "src/f_o_o", "f/oo/z"
    )
    results, truncated = worker.evaluate(view, "foo", 20, threading.Event())
    assert [result.path for result in results] == [
        "src/foo",
        "src/foobar",
        "src/afoo",
        "foo/bar",
        "src/f_o_o",
        "f/oo/z",
    ]
    assert not truncated
    assert results[-2].highlights == ((4, 5), (6, 7), (8, 9))


def test_prefix_refines_complete_candidates_not_top_k() -> None:
    worker = matcher.Matcher(100)
    view = make_view("a", "ab", "abc", "axbxc", "cab")
    results, truncated = worker.evaluate(view, "a", 1, threading.Event())
    assert results[0].path == "a"
    assert truncated
    results, _ = worker.evaluate(view, "abc", 20, threading.Event())
    assert [result.path for result in results] == ["abc", "axbxc"]
    results, _ = worker.evaluate(make_view("new/abc"), "abc", 20, threading.Event())
    assert results[0].path == "new/abc"


def test_unicode_highlights_and_trigram_verification() -> None:
    worker = matcher.Matcher(0)
    view = make_view("Straße.py", "abcd", "abc_x_bcd")
    results, _ = worker.evaluate(view, "STRASSE", 20, threading.Event())
    assert results[0].highlights == ((0, 6),)
    results, _ = worker.evaluate(view, "abcd", 20, threading.Event())
    assert [result.path for result in results] == ["abcd", "abc_x_bcd"]
    assert results[1].highlights != ((0, 4),)


@pytest.mark.parametrize("query", ["a", "aa", "abc", "ac", "bc", "ca", "xyz"])
def test_accelerators_preserve_subsequence_correctness(query: str) -> None:
    values = ["abc", "abac", "a_b_c", "ca/b", "AAA", "xyz", "cab", "bca"]
    worker = matcher.Matcher(100)
    results, _ = worker.evaluate(make_view(*values), query, 100, threading.Event())
    expected = {
        value
        for value in values
        if matcher.subsequence(value.casefold(), query.casefold()) is not None
    }
    assert {result.path for result in results} == expected


@pytest.mark.parametrize("backend", [None, "missing-file-search-rg", "rg"])
def test_discovery_ignore_hidden_unicode_and_symlinks(
    tmp_path: Path, backend: str
) -> None:
    if backend == "rg" and not shutil.which("rg"):
        pytest.skip("ripgrep is not installed")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("secret")
    (tmp_path / ".gitignore").write_text("*.log\nignored/\n*.tmp\n")
    (tmp_path / "ignored").mkdir()
    (tmp_path / "ignored" / "file").touch()
    (tmp_path / "root.log").touch()
    (tmp_path / ".hidden").touch()
    (tmp_path / "space ünicode.txt").touch()
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / ".gitignore").write_text("!keep.tmp\nlocal.txt\n")
    (sub / "keep.tmp").touch()
    (sub / "drop.tmp").touch()
    (sub / "local.txt").touch()
    (sub / "ok").touch()
    (tmp_path / "linked-directory").symlink_to(sub, target_is_directory=True)
    (tmp_path / "linked-file").symlink_to(sub / "ok")
    config = fs.SearchConfig(rg_executable=backend)
    found = set(
        discovery.discover(tmp_path, config, threading.Event(), time.monotonic() + 5)
    )
    assert found == {
        ".gitignore",
        ".hidden",
        "space ünicode.txt",
        "sub/.gitignore",
        "sub/keep.tmp",
        "sub/ok",
    }


async def test_refresh_reuse_deletion_and_immutable_snapshots(tmp_path: Path) -> None:
    (tmp_path / "old").touch()
    service = fs.FileSearchService(tmp_path, fs.SearchConfig(rg_executable=None))
    try:
        refresh_id = await service.refresh()
        assert await service.refresh() == refresh_id
        old = await snapshot(service)
        assert paths(old) == {"old"}
        (tmp_path / "old").unlink()
        (tmp_path / "new").touch()
        assert await service.refresh() != refresh_id
        new = await snapshot(service)
        assert paths(new) == {"new"}
        assert paths(old) == {"old"}
        assert new.index_revision > old.index_revision
        assert not new.incomplete
    finally:
        await service.close()


async def test_live_batches_and_query_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = threading.Event()
    started = threading.Event()

    def scan(
        root: Path, config: fs.SearchConfig, cancel: threading.Event, deadline: float
    ) -> Iterator[str]:
        yield "existing"
        time.sleep(0.01)
        yield "new-match"
        started.set()
        while not release.wait(0.005):
            discovery.check_scan(cancel, deadline)

    (tmp_path / "existing").touch()
    service = fs.FileSearchService(
        tmp_path,
        fs.SearchConfig(rg_executable=None, publication_interval=0.001, batch_size=1),
    )
    stream = service.search("match")
    try:
        await service.refresh()
        await snapshot(service)
        monkeypatch.setattr(discovery, "discover", scan)
        refresh_id = await service.refresh()
        async with asyncio.timeout(5):
            async for update in stream:
                if "new-match" in paths(update):
                    assert update.refreshing
                    break
        assert started.is_set()
        other = service.search("existing")
        available = await anext(other)
        assert paths(available) == {"existing"}
        await other.aclose()
        await stream.aclose()
        assert await service.refresh() == refresh_id
        release.set()
        assert paths(await snapshot(service)) == {"existing", "new-match"}
    finally:
        release.set()
        await stream.aclose()
        await service.close()


@pytest.mark.parametrize(
    "failure", ["error", "candidate", "memory", "deadline", "entries"]
)
async def test_incomplete_refresh_preserves_completed_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    (tmp_path / "old").touch()
    config = fs.SearchConfig(
        rg_executable=None,
        max_candidates=1 if failure == "candidate" else 100,
        max_memory_bytes=1000 if failure == "memory" else 100_000,
        max_scan_entries=1 if failure == "entries" else 100,
        scan_timeout=0.2 if failure == "deadline" else 5,
        publication_interval=0.001,
    )
    service = fs.FileSearchService(tmp_path, config)
    try:
        await service.refresh()
        assert not (await snapshot(service)).incomplete
        (tmp_path / "old").unlink()
        (tmp_path / "new").touch()
        if failure == "entries":
            (tmp_path / "extra").touch()

        if failure in ("error", "deadline"):

            def broken(
                root: Path,
                config: fs.SearchConfig,
                cancel: threading.Event,
                deadline: float,
            ) -> Iterator[str]:
                yield "new"
                if failure == "deadline":
                    while True:
                        discovery.check_scan(cancel, deadline)
                        time.sleep(0.005)
                raise PermissionError("denied")

            monkeypatch.setattr(discovery, "discover", broken)
        await service.refresh()
        update = await snapshot(service)
        assert paths(update) == {"old"}
        assert update.incomplete
        assert update.refresh_error
        assert update.truncated == (failure != "error")
    finally:
        await service.close()


async def test_slow_consumer_and_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = threading.Event()

    def scan(
        root: Path, config: fs.SearchConfig, cancel: threading.Event, deadline: float
    ) -> Iterator[str]:
        started.set()
        for i in range(10000):
            discovery.check_scan(cancel, deadline)
            yield f"file-{i}"
            time.sleep(0.002)

    monkeypatch.setattr(discovery, "discover", scan)
    service = fs.FileSearchService(
        tmp_path, fs.SearchConfig(publication_interval=0.001)
    )
    stream = service.search("")
    await anext(stream)
    await service.refresh()
    await asyncio.to_thread(started.wait, 2)
    await asyncio.sleep(0.05)
    assert all(
        subscription.queue.qsize() <= 1 for subscription in service._subscriptions
    )
    async with asyncio.timeout(3):
        await asyncio.gather(service.close(), service.close())
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    with pytest.raises(RuntimeError, match="closed"):
        await service.refresh()
    with pytest.raises(RuntimeError, match="closed"):
        await anext(service.search("x"))


def test_configuration_and_search_validation(tmp_path: Path) -> None:
    with pytest.raises(pydantic.ValidationError):
        fs.SearchConfig(max_candidates=0)
    service = fs.FileSearchService(tmp_path, fs.SearchConfig(max_query_length=2))
    with pytest.raises(ValueError):
        service.search("abc")
    with pytest.raises(ValueError):
        service.search("a", limit=101)


def test_cancellation_and_ignore_size_limits(tmp_path: Path) -> None:
    stop = threading.Event()
    stop.set()
    with pytest.raises(discovery.ScanError, match="cancelled"):
        list(
            discovery.discover(tmp_path, fs.SearchConfig(), stop, time.monotonic() + 5)
        )
    (tmp_path / ".gitignore").write_text("too large")
    with pytest.raises(discovery.ScanError, match="size limit"):
        list(
            discovery.discover(
                tmp_path,
                fs.SearchConfig(rg_executable=None, max_ignore_bytes=1),
                threading.Event(),
                time.monotonic() + 5,
            )
        )
