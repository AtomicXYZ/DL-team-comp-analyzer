from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dl_team_comp_analyzer.bulk_extract import extract_match_payloads
from dl_team_comp_analyzer.deadlock_api import (
    DeadlockApiClient,
    DeadlockApiError,
    DeadlockRateLimitError,
)
from dl_team_comp_analyzer.match_parser import (
    build_match_view,
    dataset_fieldnames,
    match_view_to_dataset_row,
    match_view_to_dict,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch many compact Deadlock match summaries and append them to JSONL + CSV."
    )
    parser.add_argument("--target-count", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=0.35)
    parser.add_argument("--rate-limit-sleep-seconds", type=float, default=7.0)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--jsonl-output", default="data/processed/match_summaries.jsonl")
    parser.add_argument("--csv-output", default="data/processed/match_dataset.csv")
    parser.add_argument("--state-file", default="data/processed/fetch_state.json")
    parser.add_argument("--match-ids-file", help="Optional file with one match id per line.")
    parser.add_argument("--min-match-id", type=int)
    parser.add_argument("--max-match-id", type=int)
    parser.add_argument("--min-unix-timestamp", type=int)
    parser.add_argument("--max-unix-timestamp", type=int)
    parser.add_argument("--order-by", default="match_id")
    parser.add_argument("--order-direction", choices=("asc", "desc"), default="desc")
    parser.add_argument("--game-api-base", default="https://api.deadlock-api.com/v1")
    parser.add_argument("--assets-api-base", default="https://assets.deadlock-api.com/v2")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = DeadlockApiClient(
        game_api_base=args.game_api_base,
        assets_api_base=args.assets_api_base,
    )

    jsonl_path = Path(args.jsonl_output)
    csv_path = Path(args.csv_output)
    state_path = Path(args.state_file)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    existing_match_ids = load_existing_match_ids(csv_path if csv_path.exists() else jsonl_path)
    state = load_state(state_path) if args.resume else {}

    if args.match_ids_file:
        new_rows = run_exact_match_id_mode(
            args=args,
            client=client,
            existing_match_ids=existing_match_ids,
            jsonl_path=jsonl_path,
            csv_path=csv_path,
        )
        print(f"Added {new_rows} new matches from {args.match_ids_file}")
        return 0

    new_rows = run_paged_mode(
        args=args,
        client=client,
        existing_match_ids=existing_match_ids,
        jsonl_path=jsonl_path,
        csv_path=csv_path,
        state_path=state_path,
        state=state,
    )
    print(f"Added {new_rows} new matches in paged mode")
    return 0


def run_exact_match_id_mode(
    args: argparse.Namespace,
    client: DeadlockApiClient,
    existing_match_ids: set[str],
    jsonl_path: Path,
    csv_path: Path,
) -> int:
    match_ids = load_match_ids_file(Path(args.match_ids_file))
    remaining_ids = [match_id for match_id in match_ids if str(match_id) not in existing_match_ids]
    if not remaining_ids:
        return 0

    added = 0
    batches = chunked(remaining_ids, args.batch_size)
    for batch_index, batch_ids in enumerate(batches, start=1):
        try:
            payload = client.fetch_bulk_match_metadata(
                match_ids=batch_ids,
                include_info=True,
                include_player_info=True,
                include_player_items=False,
                include_player_stats=False,
                include_player_death_details=False,
                include_objectives=False,
                include_mid_boss=False,
            )
        except DeadlockRateLimitError as exc:
            wait_seconds = exc.retry_after_seconds or args.rate_limit_sleep_seconds
            print(
                f"[batch {batch_index}] rate limited, sleeping {wait_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
            continue
        except DeadlockApiError as exc:
            print(f"[batch {batch_index}] failed: {exc}", file=sys.stderr)
            time.sleep(args.sleep_seconds)
            continue
        added_in_batch = persist_payload_matches(
            payload=payload,
            existing_match_ids=existing_match_ids,
            jsonl_path=jsonl_path,
            csv_path=csv_path,
            hero_resolver=client.get_hero_name,
        )
        added += added_in_batch
        print(f"[batch {batch_index}] requested={len(batch_ids)} added={added_in_batch} total_added={added}")
        time.sleep(args.sleep_seconds)
    return added


def run_paged_mode(
    args: argparse.Namespace,
    client: DeadlockApiClient,
    existing_match_ids: set[str],
    jsonl_path: Path,
    csv_path: Path,
    state_path: Path,
    state: dict[str, Any],
) -> int:
    added = 0
    batch_index = 0

    current_min_match_id = state.get("next_min_match_id", args.min_match_id)
    current_max_match_id = state.get("next_max_match_id", args.max_match_id)

    while added < args.target_count:
        if args.max_batches is not None and batch_index >= args.max_batches:
            break

        query_params = {
            "limit": args.batch_size,
            "order_by": args.order_by,
            "order_direction": args.order_direction,
            "min_match_id": current_min_match_id,
            "max_match_id": current_max_match_id,
            "min_unix_timestamp": args.min_unix_timestamp,
            "max_unix_timestamp": args.max_unix_timestamp,
            "include_info": True,
            "include_player_info": True,
            "include_player_items": False,
            "include_player_stats": False,
            "include_player_death_details": False,
            "include_objectives": False,
            "include_mid_boss": False,
        }

        try:
            payload = client.fetch_bulk_match_metadata(**query_params)
        except DeadlockRateLimitError as exc:
            wait_seconds = exc.retry_after_seconds or args.rate_limit_sleep_seconds
            print(f"Rate limited, sleeping {wait_seconds:.1f}s", file=sys.stderr)
            time.sleep(wait_seconds)
            continue
        except DeadlockApiError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return added

        match_payloads = extract_match_payloads(payload)
        if not match_payloads:
            print("No more matches returned by the bulk endpoint.")
            break

        added_in_batch = persist_payload_matches(
            payload=match_payloads,
            existing_match_ids=existing_match_ids,
            jsonl_path=jsonl_path,
            csv_path=csv_path,
            hero_resolver=client.get_hero_name,
        )
        added += added_in_batch
        batch_index += 1

        match_ids = [extract_match_id(candidate) for candidate in match_payloads]
        numeric_match_ids = [match_id for match_id in match_ids if match_id is not None]
        if not numeric_match_ids:
            print("Could not find match IDs in the returned batch, stopping.")
            break

        if args.order_direction == "desc":
            current_max_match_id = min(numeric_match_ids) - 1
        else:
            current_min_match_id = max(numeric_match_ids) + 1

        state_payload = {
            "next_min_match_id": current_min_match_id,
            "next_max_match_id": current_max_match_id,
            "last_batch_size": len(match_payloads),
            "total_added_this_run": added,
            "last_seen_match_ids": numeric_match_ids[:10],
        }
        state_path.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

        print(
            f"[batch {batch_index}] fetched={len(match_payloads)} added={added_in_batch} "
            f"total_added={added} next_min={current_min_match_id} next_max={current_max_match_id}"
        )
        time.sleep(args.sleep_seconds)

    return added


def persist_payload_matches(
    payload: Any,
    existing_match_ids: set[str],
    jsonl_path: Path,
    csv_path: Path,
    hero_resolver,
) -> int:
    match_payloads = extract_match_payloads(payload)
    if not match_payloads:
        return 0

    fieldnames = dataset_fieldnames()
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv_header = not csv_path.exists()

    added = 0
    with jsonl_path.open("a", encoding="utf-8") as jsonl_file, csv_path.open(
        "a", newline="", encoding="utf-8"
    ) as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if write_csv_header:
            writer.writeheader()

        for raw_match in match_payloads:
            try:
                match_view = build_match_view(raw_match, hero_resolver=hero_resolver)
            except ValueError:
                continue

            match_id = match_view.match_id
            if match_id in existing_match_ids:
                continue

            summary = match_view_to_dict(match_view)
            dataset_row = match_view_to_dataset_row(match_view)
            jsonl_file.write(json.dumps(summary) + "\n")
            writer.writerow(dataset_row)
            existing_match_ids.add(match_id)
            added += 1

    return added


def extract_match_id(raw_match: dict[str, Any]) -> int | None:
    candidates = [raw_match]
    if isinstance(raw_match.get("match_info"), dict):
        candidates.append(raw_match["match_info"])

    for candidate in candidates:
        for key in ("match_id", "matchId", "id"):
            value = candidate.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def load_existing_match_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()

    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            return {row["match_id"] for row in reader if row.get("match_id")}

    match_ids: set[str] = set()
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            match_id = payload.get("match_id")
            if match_id:
                match_ids.add(str(match_id))
    return match_ids


def load_match_ids_file(path: Path) -> list[int]:
    match_ids: list[int] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            match_ids.append(int(line))
        except ValueError:
            continue
    return match_ids


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


if __name__ == "__main__":
    raise SystemExit(main())
