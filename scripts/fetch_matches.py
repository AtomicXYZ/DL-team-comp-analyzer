from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

from common import FETCH_STATE_PATH, MATCHES_PATH, append_jsonl, load_json, read_jsonl, write_json

from bulk_extract import extract_match_payloads
from deadlock_api import (
    DeadlockApiClient,
    DeadlockApiError,
    DeadlockRateLimitError,
)
from match_parser import build_match_view, match_view_to_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Deadlock matches into data/new_patch_matches.jsonl.")
    parser.add_argument("--output", type=Path, default=MATCHES_PATH)
    parser.add_argument("--state-file", type=Path, default=FETCH_STATE_PATH)
    parser.add_argument("--target-count", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=0.35)
    parser.add_argument("--rate-limit-sleep-seconds", type=float, default=10.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--min-match-id", type=int)
    parser.add_argument("--max-match-id", type=int)
    parser.add_argument("--order-direction", choices=("asc", "desc"), default="desc")
    parser.add_argument(
        "--game-mode",
        choices=("normal", "street_brawl", "explore_n_y_c", "internal"),
        default="normal",
        help="Deadlock API game_mode filter. Defaults to normal to exclude Street Brawl.",
    )
    parser.add_argument(
        "--allow-missing-start-time",
        action="store_true",
        help="Keep matches without start_time_s. By default these are skipped.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = DeadlockApiClient()
    existing_match_ids = {str(match.get("match_id")) for match in read_jsonl(args.output)}
    state = load_json(args.state_file, {}) if args.resume else {}

    current_min_match_id = state.get("next_min_match_id", args.min_match_id)
    current_max_match_id = state.get("next_max_match_id", args.max_match_id)
    added = 0
    batch_index = 0

    while added < args.target_count:
        if args.max_batches is not None and batch_index >= args.max_batches:
            break

        query = {
            "limit": args.batch_size,
            "order_by": "match_id",
            "order_direction": args.order_direction,
            "min_match_id": current_min_match_id,
            "max_match_id": current_max_match_id,
            "include_info": True,
            "include_player_info": True,
            "include_player_items": False,
            "include_player_stats": False,
            "include_player_death_details": False,
            "include_objectives": False,
            "include_mid_boss": False,
            "game_mode": args.game_mode,
        }

        try:
            payload = client.fetch_bulk_match_metadata(**query)
        except DeadlockRateLimitError as exc:
            wait_seconds = exc.retry_after_seconds or args.rate_limit_sleep_seconds
            print(f"Rate limited, sleeping {wait_seconds:.1f}s")
            time.sleep(wait_seconds)
            continue
        except DeadlockApiError as exc:
            print(f"Deadlock API failed: {exc}")
            return 1

        raw_matches = extract_match_payloads(payload)
        if not raw_matches:
            print("No matches returned, stopping.")
            break

        summaries = normalize_matches(
            raw_matches,
            existing_match_ids,
            allow_missing_start_time=args.allow_missing_start_time,
        )
        append_jsonl(args.output, summaries)
        added += len(summaries)
        batch_index += 1

        match_ids = [match_id for match_id in (extract_match_id(match) for match in raw_matches) if match_id]
        if not match_ids:
            print("No match IDs found in response, stopping.")
            break

        if args.order_direction == "desc":
            current_max_match_id = min(match_ids) - 1
        else:
            current_min_match_id = max(match_ids) + 1

        write_json(
            args.state_file,
            {
                "next_min_match_id": current_min_match_id,
                "next_max_match_id": current_max_match_id,
                "last_batch_size": len(raw_matches),
                "total_added_this_run": added,
            },
        )
        print(f"[batch {batch_index}] fetched={len(raw_matches)} added={len(summaries)} total_added={added}")
        time.sleep(args.sleep_seconds)

    return 0


def normalize_matches(
    raw_matches: list[dict[str, Any]],
    existing_match_ids: set[str],
    *,
    allow_missing_start_time: bool,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for raw_match in raw_matches:
        try:
            match_view = build_match_view(raw_match)
        except ValueError:
            continue
        if match_view.match_id in existing_match_ids:
            continue
        if not allow_missing_start_time and match_view.start_time_s is None:
            continue
        summaries.append(match_view_to_dict(match_view))
        existing_match_ids.add(match_view.match_id)
    return summaries


def extract_match_id(raw_match: dict[str, Any]) -> int | None:
    candidates = [raw_match]
    if isinstance(raw_match.get("match_info"), dict):
        candidates.append(raw_match["match_info"])

    for candidate in candidates:
        for key in ("match_id", "matchId", "id"):
            try:
                return int(candidate[key])
            except (KeyError, TypeError, ValueError):
                continue
    return None


if __name__ == "__main__":
    raise SystemExit(main())
