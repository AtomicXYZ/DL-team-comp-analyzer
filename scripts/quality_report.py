from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a small quality report for the Deadlock team comp dataset."
    )
    parser.add_argument("--csv-path", default="data/processed/match_dataset.csv")
    parser.add_argument("--jsonl-path", default="data/processed/match_summaries.jsonl")
    parser.add_argument("--sample-limit", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    jsonl_path = Path(args.jsonl_path)

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    rows = read_csv_rows(csv_path)
    print_csv_report(rows, sample_limit=args.sample_limit)

    if jsonl_path.exists():
        summaries = read_jsonl_rows(jsonl_path)
        print()
        print_jsonl_report(summaries, sample_limit=args.sample_limit)
    else:
        print()
        print(f"JSONL not found: {jsonl_path}")

    return 0


def print_csv_report(rows: list[dict[str, str]], sample_limit: int) -> None:
    print("CSV Report")
    print(f"rows: {len(rows)}")

    match_id_counter = Counter(row.get("match_id", "") for row in rows if row.get("match_id"))
    duplicate_match_ids = [match_id for match_id, count in match_id_counter.items() if count > 1]
    print(f"duplicate_match_ids: {len(duplicate_match_ids)}")
    if duplicate_match_ids:
        print(f"duplicate_examples: {duplicate_match_ids[:sample_limit]}")

    unknown_counter: Counter[str] = Counter()
    for row in rows:
        for key, value in row.items():
            if is_unknown(value):
                unknown_counter[key] += 1

    print("unknown_counts:")
    for key, count in sorted(unknown_counter.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    not_six_vs_six: list[str] = []
    missing_hero_slots: list[str] = []
    winner_inconsistencies: list[str] = []
    badge_anomalies: list[str] = []

    for row in rows:
        match_id = row.get("match_id", "unknown")
        team_1_heroes = [row.get(f"team_1_hero_{index}", "") for index in range(1, 7)]
        team_2_heroes = [row.get(f"team_2_hero_{index}", "") for index in range(1, 7)]

        team_1_count = sum(1 for hero in team_1_heroes if hero)
        team_2_count = sum(1 for hero in team_2_heroes if hero)
        if team_1_count != 6 or team_2_count != 6:
            not_six_vs_six.append(match_id)

        if any(is_unknown(hero) or hero == "" for hero in team_1_heroes + team_2_heroes):
            missing_hero_slots.append(match_id)

        winner = row.get("winner", "")
        winner_team_index = row.get("winner_team_index", "")
        if not winner_matches_index(winner, winner_team_index):
            winner_inconsistencies.append(match_id)

        for badge_key in ("team_1_average_badge", "team_2_average_badge"):
            badge = row.get(badge_key, "")
            if badge and badge != "Unknown":
                try:
                    badge_value = int(badge)
                except ValueError:
                    badge_anomalies.append(f"{match_id}:{badge_key}={badge}")
                    continue
                if badge_value < 0 or badge_value > 200:
                    badge_anomalies.append(f"{match_id}:{badge_key}={badge}")

    print(f"rows_not_6v6: {len(not_six_vs_six)}")
    if not_six_vs_six:
        print(f"rows_not_6v6_examples: {not_six_vs_six[:sample_limit]}")

    print(f"rows_with_missing_hero_slots: {len(missing_hero_slots)}")
    if missing_hero_slots:
        print(f"rows_with_missing_hero_slots_examples: {missing_hero_slots[:sample_limit]}")

    print(f"winner_index_inconsistencies: {len(winner_inconsistencies)}")
    if winner_inconsistencies:
        print(f"winner_index_inconsistency_examples: {winner_inconsistencies[:sample_limit]}")

    print(f"badge_anomalies: {len(badge_anomalies)}")
    if badge_anomalies:
        print(f"badge_anomaly_examples: {badge_anomalies[:sample_limit]}")


def print_jsonl_report(rows: list[dict[str, Any]], sample_limit: int) -> None:
    print("JSONL Report")
    print(f"rows: {len(rows)}")

    duplicate_match_ids = count_duplicate_jsonl_match_ids(rows)
    print(f"duplicate_match_ids: {len(duplicate_match_ids)}")
    if duplicate_match_ids:
        print(f"duplicate_examples: {duplicate_match_ids[:sample_limit]}")

    unknown_counter: Counter[str] = Counter()
    team_size_issues: list[str] = []
    winner_inconsistencies: list[str] = []

    for row in rows:
        match_id = str(row.get("match_id", "unknown"))

        for key in (
            "start_time_s",
            "start_time_utc",
            "patch",
            "team_1_average_badge",
            "team_2_average_badge",
            "winner",
            "winner_team_index",
        ):
            if is_unknown(row.get(key)):
                unknown_counter[key] += 1

        for team_key in ("team_1_players", "team_2_players", "team_1_hero_names", "team_2_hero_names"):
            value = row.get(team_key)
            if not isinstance(value, list) or len(value) != 6:
                team_size_issues.append(match_id)
                break

        if not winner_matches_index(
            str(row.get("winner", "")),
            "" if row.get("winner_team_index") is None else str(row.get("winner_team_index")),
        ):
            winner_inconsistencies.append(match_id)

    print("unknown_counts:")
    for key, count in sorted(unknown_counter.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {key}: {count}")

    print(f"team_size_issues: {len(team_size_issues)}")
    if team_size_issues:
        print(f"team_size_issue_examples: {team_size_issues[:sample_limit]}")

    print(f"winner_index_inconsistencies: {len(winner_inconsistencies)}")
    if winner_inconsistencies:
        print(f"winner_index_inconsistency_examples: {winner_inconsistencies[:sample_limit]}")

    unresolved_hero_names = [
        str(row.get("match_id", "unknown"))
        for row in rows
        if any(str(name).startswith("Hero ") for name in row.get("team_1_hero_names", []))
        or any(str(name).startswith("Hero ") for name in row.get("team_2_hero_names", []))
    ]
    print(f"rows_with_unresolved_hero_names: {len(unresolved_hero_names)}")
    if unresolved_hero_names:
        print(f"rows_with_unresolved_hero_names_examples: {unresolved_hero_names[:sample_limit]}")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def count_duplicate_jsonl_match_ids(rows: list[dict[str, Any]]) -> list[str]:
    counter = Counter(str(row.get("match_id", "")) for row in rows if row.get("match_id") is not None)
    return [match_id for match_id, count in counter.items() if count > 1]


def is_unknown(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in {"", "Unknown"}
    return False


def winner_matches_index(winner: str, winner_team_index: str) -> bool:
    if winner_team_index not in {"0", "1"}:
        return False

    winner_normalized = winner.strip().lower()
    if winner_team_index == "0":
        return winner_normalized in {"team 1", "team1", "team_1", "hidden king", "archon"}
    return winner_normalized in {"team 2", "team2", "team_2", "archmother", "eternus"}


if __name__ == "__main__":
    raise SystemExit(main())
