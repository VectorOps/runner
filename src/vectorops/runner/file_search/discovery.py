"""Blocking, cancellable discovery backends used only by the index worker."""

from collections.abc import Generator, Iterator
import os
from pathlib import Path
import subprocess
import threading
import time

import pathspec

from .models import SearchConfig


class ScanError(Exception):
    def __init__(self, message: str, *, limited: bool = False) -> None:
        super().__init__(message)
        self.limited = limited


def check_scan(cancel: threading.Event, deadline: float) -> None:
    if cancel.is_set():
        raise ScanError("Refresh cancelled")
    if time.monotonic() >= deadline:
        raise ScanError("Scan deadline exceeded", limited=True)


def _ripgrep(
    root: Path, executable: str, cancel: threading.Event, deadline: float
) -> Iterator[str]:
    with subprocess.Popen(
        [executable, "--files", "--hidden", "-0", "-g", "!.git", "."],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ) as process:
        done = threading.Event()

        def watch() -> None:
            while not done.wait(0.02):
                if cancel.is_set() or time.monotonic() >= deadline:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    return

        watcher = threading.Thread(target=watch, name="file-search-rg-watch")
        watcher.start()
        try:
            assert process.stdout is not None
            pending = b""
            while chunk := os.read(process.stdout.fileno(), 65536):
                check_scan(cancel, deadline)
                paths = (pending + chunk).split(b"\0")
                pending = paths.pop()
                for raw in paths:
                    path = os.fsdecode(raw)
                    yield path[2:] if path.startswith("./") else path
            check_scan(cancel, deadline)
            code = process.wait()
            if code not in (0, 1) or pending:
                raise ScanError(f"ripgrep discovery failed (exit {code})")
        finally:
            done.set()
            if process.poll() is None:
                process.kill()
            process.wait()
            watcher.join()


def _scandir(
    root: Path, config: SearchConfig, cancel: threading.Event, deadline: float
) -> Iterator[str]:
    rules: tuple[tuple[str, pathspec.GitIgnoreSpec], ...] = ()
    stack = [(root, "", rules)]
    visited = 0
    while stack:
        check_scan(cancel, deadline)
        directory, prefix, inherited = stack.pop()
        ignore_path = directory / ".gitignore"
        try:
            with ignore_path.open("rb") as source:
                content = source.read(config.max_ignore_bytes + 1)
            if len(content) > config.max_ignore_bytes:
                raise ScanError("Gitignore size limit exceeded", limited=True)
            spec = pathspec.GitIgnoreSpec.from_lines(
                content.decode("utf-8", errors="surrogateescape").splitlines()
            )
            inherited = (*inherited, (prefix, spec))
        except FileNotFoundError:
            pass
        with os.scandir(directory) as entries:
            for entry in entries:
                check_scan(cancel, deadline)
                visited += 1
                if visited > config.max_scan_entries:
                    raise ScanError("Traversal entry limit exceeded", limited=True)
                if entry.name == ".git" or entry.is_symlink():
                    continue
                relative = prefix + entry.name
                is_directory = entry.is_dir(follow_symlinks=False)
                target = relative + ("/" if is_directory else "")
                ignored = False
                for base, spec in inherited:
                    decision = spec.check_file(target[len(base) :]).include
                    if decision is not None:
                        ignored = decision
                if ignored:
                    continue
                if is_directory:
                    stack.append((Path(entry.path), relative + "/", inherited))
                elif entry.is_file(follow_symlinks=False):
                    yield relative


def discover(
    root: Path, config: SearchConfig, cancel: threading.Event, deadline: float
) -> Generator[str, None, None]:
    """Prefer ripgrep; fall back only when the executable cannot be found."""
    check_scan(cancel, deadline)
    if not root.is_dir():
        raise ScanError("Workspace root is not a directory")
    if config.rg_executable is not None:
        try:
            yield from _ripgrep(root, config.rg_executable, cancel, deadline)
            return
        except FileNotFoundError:
            pass
    yield from _scandir(root, config, cancel, deadline)
