from __future__ import annotations  # Maakt moderne type hints mogelijk.

import os  # Environment variables lezen en zetten.
from pathlib import Path  # Pad naar repo-root en .env bepalen.


def load_repo_env() -> None:
    """Laad key=value regels uit .env in os.environ zonder bestaande waarden te overschrijven."""
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ[key] = value
