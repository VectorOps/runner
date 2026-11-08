import json
from pathlib import Path
import re
import shlex
from typing import Final

import pytest

from vectorops.runner import cli, skills

ROOT: Final = Path(__file__).resolve().parents[1]


def test_readme_quick_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    section = (
        (ROOT / "README.md")
        .read_text()
        .split("## Run your first workflow\n", 1)[1]
        .split("\n## ", 1)[0]
    )
    source = re.findall(r"```python\n(.*?)```", section, re.DOTALL)[0]
    expected = json.loads(re.findall(r"```json\n(.*?)```", section, re.DOTALL)[0])
    path = tmp_path / ".voc/workflows/greeting.py"
    path.parent.mkdir(parents=True)
    path.write_text(source)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("VOPS_RUNNER_WORKSPACE_DIRECTORY")
    outputs = []
    for block in re.findall(r"```bash\n(.*?)```", section, re.DOTALL):
        for line in block.splitlines():
            words = shlex.split(line)
            if words[:3] == ["uv", "run", "vops-runner"]:
                assert cli.main(words[3:]) == 0
                outputs.append(json.loads(capsys.readouterr().out))
    assert len(outputs) == 3
    assert outputs[0] == expected
    assert outputs[1]["execution"]["status"] == "completed"
    assert outputs[2] == expected


async def test_repository_skills_are_discoverable() -> None:
    catalog = await skills.discover_skills(ROOT, include_builtin=False)
    assert [skill.name for skill in catalog.skills] == [
        "documentation-standards",
        "tui-command-standards",
    ]
    assert all(skill.path.startswith(".voc/skills/") for skill in catalog.skills)
