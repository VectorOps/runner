from pathlib import Path


def path(name: str) -> Path:
    return Path(__file__).parent / "scripts" / name


def read(name: str) -> str:
    return path(name).read_text(encoding="utf-8")
