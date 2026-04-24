from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dl_team_comp_analyzer.deadlock_api import DeadlockApiClient, DeadlockApiError
from dl_team_comp_analyzer.match_parser import (
    build_match_view,
    dataset_fieldnames,
    match_view_to_dataset_row,
    match_view_to_dict,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich an existing compact dataset by refetching full metadata for rows with missing fields."
    )
    parser.add_argument("--csv-path", default="data/processed/match_dataset.csv")
    parser.add_argument("--jsonl-path", default="data/processed/match_summaries.jsonl")
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--all-rows", action="store_true")
    parser.add_argument("--game-api-base", default="https://api.deadlock-api.com/v1")
    parser.add_argument("--assets-api-base", default="https://assets.deadlock-api.com/v2")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    jsonl_path = Path(args.jsonl_path)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    rows = read_csv_rows(csv_path)
    client = DeadlockApiClient(
        game_api_base=args.game_api_base,
        assets_api_base=args.assets_api_base,
    )

    updated_match_ids: set[str] = set()
    updated_count = 0
    attempted = 0

    for row in rows:
        if not args.all_rows and not needs_enrichment(row):
            continue

        if args.limit is not None and attempted >= args.limit:
            break

        attempted += 1
        match_id = row["match_id"]
        try:
            payload = client.fetch_match_metadata(match_id)
            match_view = build_match_view(payload, hero_resolver=client.get_hero_name)
        except (DeadlockApiError, ValueError) as exc:
            print(f"[{attempted}] match={match_id} failed: {exc}", file=sys.stderr)
            time.sleep(args.sleep_seconds)
            continue

        enriched_row = match_view_to_dataset_row(match_view)
        row.update(enriched_row)
        updated_match_ids.add(match_id)
        updated_count += 1
        print(f"[{attempted}] match={match_id} enriched")
        time.sleep(args.sleep_seconds)

    write_csv_rows(csv_path, rows)
    if jsonl_path.exists() and updated_match_ids:
        rewrite_jsonl(jsonl_path, client, updated_match_ids)

    print(f"Enriched {updated_count} matches")
    return 0


def needs_enrichment(row: dict[str, str]) -> bool:
    return any(
        (
            not row.get("start_time_s"),
            row.get("patch") in {None, "", "Unknown"},
            row.get("team_1_average_badge") in {None, "", "Unknown"},
            row.get("team_2_average_badge") in {None, "", "Unknown"},
        )
    )


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = dataset_fieldnames()
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def rewrite_jsonl(path: Path, client: DeadlockApiClient, updated_match_ids: set[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    rewritten: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue

        match_id = str(payload.get("match_id", ""))
        if match_id in updated_match_ids:
            try:
                full_payload = client.fetch_match_metadata(match_id)
                match_view = build_match_view(full_payload, hero_resolver=client.get_hero_name)
                payload = match_view_to_dict(match_view)
            except (DeadlockApiError, ValueError):
                pass
        rewritten.append(json.dumps(payload))

    path.write_text("\n".join(rewritten) + ("\n" if rewritten else ""), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
