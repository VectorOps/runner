import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def patcher() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/patch_code_usage.py"
    spec = importlib.util.spec_from_file_location("patch_code_usage", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replace_rejects_missing_or_ambiguous_source(patcher: ModuleType) -> None:
    assert patcher.replace("old", "old", "new") == "new"
    assert patcher.replace("new", "old", "new") == "new"
    with pytest.raises(ValueError, match="not found"):
        patcher.replace("different", "old", "new")
    with pytest.raises(ValueError, match="Ambiguous"):
        patcher.replace("old old", "old", "new")


def test_fixture_upgrade_is_idempotent(patcher: ModuleType) -> None:
    original = "  input_tokens: 100,\n  estimated_calls: 1,\n"
    updated = patcher.fixture(original)
    assert "all_input_tokens: 100" in updated
    assert "confirmed_calls: 1" in updated
    assert "priced_calls: 1" in updated
    assert patcher.fixture(updated) == updated
