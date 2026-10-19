"""Concrete tool adapters, loaded explicitly by the tool facade or collection loader."""

import importlib


def register_builtins() -> None:
    """Load factory declarations without creating bindings or live resources."""
    for name in (
        "environment_tools",
        "list_files",
        "patch",
        "plan",
        "read_file",
        "subworkflows",
        "web_fetch",
        "worktrees",
    ):
        importlib.import_module(f"{__name__}.{name}")
    importlib.import_module("vectorops.runner.api.mcp")
