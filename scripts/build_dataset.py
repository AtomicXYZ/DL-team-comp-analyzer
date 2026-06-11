from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
import csv  # CSV-bestand schrijven.
from pathlib import Path  # Bestandspaden platform-onafhankelijk maken.
from typing import Any  # Waarden kunnen uit JSON/CSV verschillende types hebben.

from common import DATASET_PATH, MATCHES_PATH, PP_SCORES_PATH, ensure_parent, load_json, read_jsonl  # Gedeelde paden en file helpers.


BASE_COLUMNS = [
    "match_id",
    "start_time_s",
    "start_time_utc",
    "patch",
    "winner",
    "winner_team_index",
    "team_1_average_badge",
    "team_2_average_badge",
    "missing_pp_scores",
]

FIELDNAMES = [
    *BASE_COLUMNS,
    *[f"team_1_hero_{index}" for index in range(1, 7)],
    *[f"team_2_hero_{index}" for index in range(1, 7)],
    *[f"team_1_account_{index}" for index in range(1, 7)],
    *[f"team_2_account_{index}" for index in range(1, 7)],
    *[f"team_1_pp_score_{index}" for index in range(1, 7)],
    *[f"team_2_pp_score_{index}" for index in range(1, 7)],
]


def parse_args() -> argparse.Namespace:
    """Lees instellingen mee die je via de terminal kunt overschrijven."""
    parser = argparse.ArgumentParser(description="Build the final flat training CSV from JSONL matches and the ppScore cache.")
    parser.add_argument("--matches", type=Path, default=MATCHES_PATH)  # JSONL met genormaliseerde matches.
    parser.add_argument("--pp-scores", type=Path, default=PP_SCORES_PATH)  # JSON-cache met account_id -> ppScore.
    parser.add_argument("--output", type=Path, default=DATASET_PATH)  # CSV-bestand dat geschreven wordt.
    parser.add_argument("--require-complete-pp", action="store_true")  # Bewaar alleen rijen waar alle ppScores bekend zijn.
    return parser.parse_args()


def main() -> int:
    """Bouw de CSV-dataset uit match JSONL plus ppScore-cache."""
    args = parse_args()
    matches = read_jsonl(args.matches)
    pp_scores = {str(key): str(value) for key, value in load_json(args.pp_scores, {}).items()}
    rows = [build_row(match, pp_scores) for match in matches]
    if args.require_complete_pp:
        rows = [row for row in rows if row["missing_pp_scores"] == "0"]

    ensure_parent(args.output)
    with args.output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.output}")
    return 0


def build_row(match: dict[str, Any], pp_scores: dict[str, str]) -> dict[str, str]:
    """Maak van een geneste match-dict een platte CSV-rij."""
    row = {
        "match_id": str(match.get("match_id", "")),
        "start_time_s": value_or_empty(match.get("start_time_s")),
        "start_time_utc": value_or_empty(match.get("start_time_utc")),
        "patch": value_or_empty(match.get("patch")),
        "winner": value_or_empty(match.get("winner")),
        "winner_team_index": value_or_empty(match.get("winner_team_index")),
        "team_1_average_badge": value_or_empty(match.get("team_1_average_badge")),
        "team_2_average_badge": value_or_empty(match.get("team_2_average_badge")),
    }

    missing_pp_scores = 0
    for team_index in (1, 2):
        players = list(match.get(f"team_{team_index}_players", []) or [])[:6]
        for slot in range(1, 7):
            player = players[slot - 1] if slot <= len(players) else {}
            account_id = value_or_empty(player.get("account_id"))
            row[f"team_{team_index}_hero_{slot}"] = value_or_empty(player.get("hero_id"))
            row[f"team_{team_index}_account_{slot}"] = account_id
            row[f"team_{team_index}_pp_score_{slot}"] = pp_scores.get(account_id, "")
            if account_id and account_id not in pp_scores:
                missing_pp_scores += 1

    row["missing_pp_scores"] = str(missing_pp_scores)
    return row


def value_or_empty(value: Any) -> str:
    """Zet None om naar lege tekst, zodat CSV-cellen netjes leeg blijven."""
    return "" if value is None else str(value)


if __name__ == "__main__":
    raise SystemExit(main())
