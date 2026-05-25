from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

DATA_DIR = REPO_ROOT / "data"
CURRENT_PATCH = "2026-05-22"
MATCHES_PATH = DATA_DIR / f"matches_{CURRENT_PATCH}.jsonl"
ACCOUNTS_PATH = DATA_DIR / f"accounts_{CURRENT_PATCH}.txt"
PP_SCORES_PATH = DATA_DIR / "pp_scores.json"
DATASET_PATH = DATA_DIR / f"team_comp_dataset_{CURRENT_PATCH}.csv"
FETCH_STATE_PATH = DATA_DIR / f"fetch_state_{CURRENT_PATCH}.json"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_ids(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_ids(path: Path, ids: list[str]) -> None:
    ensure_parent(path)
    path.write_text("\n".join(ids) + ("\n" if ids else ""), encoding="utf-8")


def chunked[T](values: list[T], size: int) -> list[list[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
