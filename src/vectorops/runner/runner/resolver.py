"""Resolve trusted workflow definitions with change-aware source caching."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import hashlib
import importlib
import importlib.util
import inspect
from pathlib import Path
import sys
from threading import RLock
from types import ModuleType
import typing
from uuid import uuid4

import pydantic

from vectorops.runner.api import composition, definitions
from vectorops.runner.references import WorkflowReference
from vectorops.runner.runner import catalog
from vectorops.runner.workflows import registry

WorkflowCallable: typing.TypeAlias = Callable[..., Awaitable[pydantic.JsonValue]]
WorkflowEntryPoint: typing.TypeAlias = typing.Union[
    WorkflowCallable, composition.WorkflowDefinition[..., pydantic.JsonValue]
]


@dataclass
class _Source:
    fingerprint: tuple[int, int, int, int, int]
    digest: bytes
    module: ModuleType
    descriptors: dict[str, definitions.WorkflowDescriptor] = field(default_factory=dict)


class WorkflowResolver:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._sources: dict[Path, _Source] = {}
        self._lock = RLock()

    async def resolve(self, reference: str) -> WorkflowEntryPoint:
        return await asyncio.to_thread(
            self._resolve, WorkflowReference.parse(reference)
        )

    async def describe(self, reference: str) -> definitions.WorkflowDescriptor:
        return await asyncio.to_thread(
            self._describe, WorkflowReference.parse(reference)
        )

    async def discover(
        self, *, after: typing.Optional[str] = None, limit: int = 100
    ) -> catalog.WorkflowCatalog:
        """Enumerate built-ins and local definitions, isolating invalid sources."""
        if not 1 <= limit <= 500:
            raise ValueError("catalog limit must be between 1 and 500")

        def scan() -> catalog.WorkflowCatalog:
            result = catalog.WorkflowCatalog()
            with self._lock:
                for name in registry.WORKFLOWS:
                    target = f"builtin:{name}"
                    if after is None or target > after:
                        result.workflows.append(
                            self._describe(WorkflowReference.parse(target))
                        )
                for path in sorted(self.root.rglob("*.py")):
                    relative = path.relative_to(self.root)
                    if any(
                        part.startswith(".") or part == "__pycache__"
                        for part in relative.parts
                    ):
                        continue
                    source = relative.with_suffix("").as_posix()
                    try:
                        reference = WorkflowReference.parse(f"{source}:workflow")
                        module = self._source(reference).module
                    except Exception as exc:
                        result.errors.append(
                            catalog.DiscoveryError(source=source, error=str(exc))
                        )
                        continue
                    for name, entry in sorted(vars(module).items()):
                        if (
                            not isinstance(entry, composition.WorkflowDefinition)
                            or entry.func.__module__ != module.__name__
                        ):
                            continue
                        target = f"{source}:{name}"
                        if after is not None and target <= after:
                            continue
                        try:
                            result.workflows.append(
                                self._describe(WorkflowReference.parse(target))
                            )
                        except Exception as exc:
                            result.errors.append(
                                catalog.DiscoveryError(source=target, error=str(exc))
                            )
                result.workflows.sort(key=lambda descriptor: descriptor.reference)
                if len(result.workflows) > limit:
                    result.next_after = result.workflows[limit - 1].reference
                    result.workflows = result.workflows[:limit]
            return result

        return await asyncio.to_thread(scan)

    async def invalidate(self, reference: typing.Optional[str] = None) -> None:
        def clear() -> None:
            with self._lock:
                if reference is None:
                    self._sources.clear()
                else:
                    parsed = WorkflowReference.parse(reference)
                    if not parsed.library and not parsed.builtin:
                        self._sources.pop(self._path(parsed), None)

        await asyncio.to_thread(clear)

    def _path(self, reference: WorkflowReference) -> Path:
        root = self.root.resolve()
        path = (root / (reference.source + ".py")).resolve()
        if not path.is_relative_to(root):
            raise ValueError("workflow must remain inside its source directory")
        return path

    def _source(self, reference: WorkflowReference) -> _Source:
        """Reuse unchanged modules, hashing only after filesystem metadata changes."""
        path = self._path(reference)
        stat = path.stat()
        fingerprint = (
            stat.st_mtime_ns,
            stat.st_ctime_ns,
            stat.st_size,
            stat.st_dev,
            stat.st_ino,
        )
        cached = self._sources.get(path)
        if cached is not None and cached.fingerprint == fingerprint:
            return cached
        source = path.read_bytes()
        digest = hashlib.sha256(source).digest()
        if cached is not None and cached.digest == digest:
            cached.fingerprint = fingerprint
            return cached
        self._sources.pop(path, None)
        module_name = f"_runner_workflow_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None:
            raise ImportError(f"cannot load workflow file: {path}")
        module = importlib.util.module_from_spec(spec)
        module.__dict__["__runner_contract_module__"] = (
            path.relative_to(self.root.resolve())
            .with_suffix("")
            .as_posix()
            .replace("/", ".")
        )
        sys.modules[module_name] = module
        try:
            exec(compile(source, str(path), "exec"), vars(module))
        finally:
            sys.modules.pop(module_name, None)
        cached = _Source(fingerprint, digest, module)
        self._sources[path] = cached
        return cached

    def _resolve(self, reference: WorkflowReference) -> WorkflowEntryPoint:
        with self._lock:
            if reference.builtin:
                return registry.resolve(reference.source)
            module = (
                importlib.import_module(reference.source)
                if reference.library
                else self._source(reference).module
            )
            return self._entry_point(module, reference.callable)

    def _describe(self, reference: WorkflowReference) -> definitions.WorkflowDescriptor:
        with self._lock:
            source = (
                None
                if reference.library or reference.builtin
                else self._source(reference)
            )
            if source is not None and str(reference) in source.descriptors:
                return source.descriptors[str(reference)].model_copy(deep=True)
            entry = self._resolve(reference)
            if isinstance(entry, composition.WorkflowDefinition):
                descriptor = entry.describe(str(reference))
            else:
                descriptor = definitions.WorkflowDescriptor(
                    reference=str(reference),
                    name=entry.__name__,
                    input_schema=definitions.input_model(entry).model_json_schema(),
                )
            if source is not None:
                source.descriptors[str(reference)] = descriptor
            return descriptor.model_copy(deep=True)

    @staticmethod
    def _entry_point(module: ModuleType, callable_name: str) -> WorkflowEntryPoint:
        entry = vars(module).get(callable_name)
        if isinstance(entry, composition.WorkflowDefinition):
            return typing.cast(
                composition.WorkflowDefinition[..., pydantic.JsonValue], entry
            )
        if not inspect.iscoroutinefunction(entry):
            raise TypeError(f"{callable_name} must be an async workflow function")
        return typing.cast(WorkflowCallable, entry)
