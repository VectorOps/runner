from pathlib import Path
import shutil

import pytest

from tests.fixtures import probe
from tests.fixtures.history import MemoryHistory
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture(autouse=True)
def global_configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOPS_RUNNER_CONFIG_DIR", str(tmp_path / "runner-global"))


@pytest.fixture
def history() -> MemoryHistory:
    return MemoryHistory()


@pytest.fixture
def execution_probe() -> probe.ExecutionProbe:
    probe.current = probe.ExecutionProbe()
    return probe.current


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    source = Path(__file__).parent / "fixtures" / "workflows"
    shutil.copytree(
        source, tmp_path / "workflows", ignore=shutil.ignore_patterns("__pycache__")
    )
    return tmp_path.resolve()


@pytest.fixture
def runner(
    workspace: Path, history: MemoryHistory, execution_probe: probe.ExecutionProbe
) -> WorkflowRunner:
    return WorkflowRunner(workspace, history)
