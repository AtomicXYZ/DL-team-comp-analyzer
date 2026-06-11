from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
import time  # Wachten tussen requests en bij fouten.
from pathlib import Path  # Bestandspaden voor output/state.
from typing import Any  # API payloads kunnen verschillende JSON-vormen hebben.

from common import CURRENT_PATCH, FETCH_STATE_PATH, MATCHES_PATH, append_jsonl, load_json, read_jsonl, write_json  # Gedeelde paden en file helpers.

from bulk_extract import extract_match_payloads  # Haalt losse match-dicts uit API-response.
from deadlock_api import (
    DeadlockApiClient,  # Client die Deadlock API requests uitvoert.
    DeadlockApiError,  # Algemene Deadlock API-fout.
    DeadlockRateLimitError,  # Specifieke fout voor HTTP 429/rate limits.
)
from match_parser import build_match_view, match_view_to_dict  # Raw match -> nette dict.


def parse_args() -> argparse.Namespace:
    """Lees fetch-instellingen uit de command line."""
    parser = argparse.ArgumentParser(description="Fetch normal Deadlock matches for one gameplay patch.")
    parser.add_argument("--output", type=Path, default=MATCHES_PATH)  # JSONL-bestand voor opgeslagen matches.
    parser.add_argument("--state-file", type=Path, default=FETCH_STATE_PATH)  # JSON-state om met --resume door te gaan.
    parser.add_argument("--target-count", type=int, default=10000)  # Stop als dit aantal unieke matches is bereikt.
    parser.add_argument("--batch-size", type=int, default=100)  # Aantal matches per API-request.
    parser.add_argument("--timeout-seconds", type=int, default=45)  # Maximale wachttijd per request.
    parser.add_argument("--sleep-seconds", type=float, default=0.35)  # Pauze tussen succesvolle batches.
    parser.add_argument("--rate-limit-sleep-seconds", type=float, default=10.0)  # Fallback-wachttijd bij rate limit.
    parser.add_argument("--request-error-sleep-seconds", type=float, default=10.0)  # Wachttijd na gewone API-fout.
    parser.add_argument("--max-consecutive-errors", type=int, default=5)  # Stop na zoveel opeenvolgende fouten.
    parser.add_argument("--resume", action="store_true")  # Lees state-file en ga verder waar vorige run stopte.
    parser.add_argument("--max-batches", type=int)  # Optioneel maximaal aantal batches voor test runs.
    parser.add_argument("--min-match-id", type=int)  # Ondergrens voor match_id filter.
    parser.add_argument("--max-match-id", type=int)  # Bovengrens voor match_id filter.
    parser.add_argument("--order-direction", choices=("asc", "desc"), default="desc")  # Nieuw naar oud of oud naar nieuw.
    parser.add_argument(
        "--required-patch",
        default=CURRENT_PATCH,
        help=f"Only store matches classified as this patch. Defaults to {CURRENT_PATCH}.",
    )
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
    """Haal batches matches op, filter ze en schrijf ze naar JSONL."""
    args = parse_args()
    client = DeadlockApiClient(timeout_seconds=args.timeout_seconds)
    existing_match_ids = {str(match.get("match_id")) for match in read_jsonl(args.output)}
    state = load_json(args.state_file, {}) if args.resume else {}

    current_min_match_id = state.get("next_min_match_id", args.min_match_id)
    current_max_match_id = state.get("next_max_match_id", args.max_match_id)
    added = 0
    batch_index = 0
    consecutive_errors = 0

    while len(existing_match_ids) < args.target_count:
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
            consecutive_errors += 1
            if consecutive_errors >= args.max_consecutive_errors:
                print(f"Deadlock API failed {consecutive_errors} times in a row: {exc}")
                return 1
            print(
                f"Deadlock API request failed ({consecutive_errors}/{args.max_consecutive_errors}); "
                f"retrying in {args.request_error_sleep_seconds:.1f}s: {exc}"
            )
            time.sleep(args.request_error_sleep_seconds)
            continue

        consecutive_errors = 0

        raw_matches = extract_match_payloads(payload)
        if not raw_matches:
            print("No matches returned, stopping.")
            break

        summaries, skipped_other_patch = normalize_matches(
            raw_matches,
            existing_match_ids,
            allow_missing_start_time=args.allow_missing_start_time,
            required_patch=args.required_patch,
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
                "total_matches": len(existing_match_ids),
            },
        )
        print(
            f"[batch {batch_index}] fetched={len(raw_matches)} added={len(summaries)} "
            f"skipped_other_patch={skipped_other_patch} total_matches={len(existing_match_ids)}"
        )
        time.sleep(args.sleep_seconds)

    return 0


def normalize_matches(
    raw_matches: list[dict[str, Any]],
    existing_match_ids: set[str],
    *,
    allow_missing_start_time: bool,
    required_patch: str | None,
) -> tuple[list[dict[str, Any]], int]:
    """Parse raw matches, verwijder duplicates en filter op patch/starttijd."""
    summaries: list[dict[str, Any]] = []
    skipped_other_patch = 0
    for raw_match in raw_matches:
        try:
            match_view = build_match_view(raw_match)
        except ValueError:
            continue
        if match_view.match_id in existing_match_ids:
            continue
        if not allow_missing_start_time and match_view.start_time_s is None:
            continue
        if required_patch and match_view.patch != required_patch:
            skipped_other_patch += 1
            continue
        summaries.append(match_view_to_dict(match_view))
        existing_match_ids.add(match_view.match_id)
    return summaries, skipped_other_patch


def extract_match_id(raw_match: dict[str, Any]) -> int | None:
    """Haal match_id uit een raw match of geneste match_info."""
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
