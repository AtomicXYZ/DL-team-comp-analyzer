from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dl_team_comp_analyzer.match_parser import dataset_fieldnames, summary_payload_to_dataset_row
from dl_team_comp_analyzer.statlocker_api import (
    StatlockerApiClient,
    StatlockerApiError,
    StatlockerRateLimitError,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge Statlocker profile ranks into an existing match summary JSONL + CSV."
    )
    parser.add_argument("--jsonl-path", default="data/processed/match_summaries.jsonl")
    parser.add_argument("--csv-path", default="data/processed/match_dataset.csv")
    parser.add_argument(
        "--rank-cache-path",
        default="data/processed/statlocker_profile_ranks.json",
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=0.1)
    parser.add_argument("--rate-limit-sleep-seconds", type=float, default=30.0)
    parser.add_argument("--limit-accounts", type=int)
    parser.add_argument("--jsonl-only", action="store_true")
    parser.add_argument("--statlocker-api-base", default="https://statlocker.gg/api")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    jsonl_path = Path(args.jsonl_path)
    csv_path = Path(args.csv_path)
    rank_cache_path = Path(args.rank_cache_path)

    if not jsonl_path.exists():
        print(f"JSONL not found: {jsonl_path}", file=sys.stderr)
        return 1

    summaries = load_jsonl_summaries(jsonl_path)
    if not summaries:
        print(f"No summaries found in {jsonl_path}", file=sys.stderr)
        return 1

    client = StatlockerApiClient(api_base=args.statlocker_api_base)
    account_ids = collect_account_ids(summaries)
    if args.limit_accounts is not None:
        account_ids = account_ids[: args.limit_accounts]

    known_ranks = collect_existing_ranks(summaries)
    cached_ranks = load_rank_cache(rank_cache_path)
    known_ranks.update(cached_ranks)
    summaries, restored_players = apply_ranks_to_summaries(summaries, known_ranks)
    if restored_players:
        write_jsonl_summaries(jsonl_path, summaries)
        if not args.jsonl_only:
            write_csv_dataset(csv_path, summaries)

    pending_account_ids = [account_id for account_id in account_ids if account_id not in known_ranks]

    print(
        f"Loaded {len(summaries)} matches with {len(account_ids)} unique accounts. "
        f"{len(pending_account_ids)} accounts still need a Statlocker lookup. "
        f"Restored {restored_players} player slots from cache."
    )

    fetched_ranks = fetch_ranks(
        client=client,
        account_ids=pending_account_ids,
        batch_size=max(1, min(args.batch_size, 100)),
        sleep_seconds=args.sleep_seconds,
        rate_limit_sleep_seconds=args.rate_limit_sleep_seconds,
        on_batch_ranks=lambda batch_ranks: persist_batch_ranks(
            batch_ranks=batch_ranks,
            known_ranks=known_ranks,
            rank_cache_path=rank_cache_path,
        ),
    )
    known_ranks.update(fetched_ranks)
    if fetched_ranks:
        write_rank_cache(rank_cache_path, known_ranks)

    updated_summaries, updated_players = apply_ranks_to_summaries(summaries, known_ranks)
    write_jsonl_summaries(jsonl_path, updated_summaries)
    if not args.jsonl_only:
        write_csv_dataset(csv_path, updated_summaries)

    print(
        f"Updated {updated_players} player slots across {len(updated_summaries)} matches "
        f"using {len(fetched_ranks)} newly fetched Statlocker profiles"
    )
    return 0


def load_jsonl_summaries(path: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            summaries.append(payload)
    return summaries


def load_rank_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(account_id): normalize_rank_value(rank)
        for account_id, rank in payload.items()
        if normalize_rank_value(rank) != "Unknown"
    }


def write_rank_cache(path: Path, ranks: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        account_id: rank
        for account_id, rank in sorted(ranks.items())
        if normalize_rank_value(rank) != "Unknown"
    }
    path.write_text(json.dumps(serializable, indent=2, sort_keys=True), encoding="utf-8")


def collect_account_ids(summaries: list[dict[str, Any]]) -> list[str]:
    account_ids: list[str] = []
    seen: set[str] = set()
    for summary in summaries:
        for player in iter_summary_players(summary):
            account_id = str(player.get("account_id", "")).strip()
            if not account_id or account_id.startswith("unknown-") or account_id in seen:
                continue
            seen.add(account_id)
            account_ids.append(account_id)
    return account_ids


def collect_existing_ranks(summaries: list[dict[str, Any]]) -> dict[str, str]:
    known_ranks: dict[str, str] = {}
    for summary in summaries:
        for player in iter_summary_players(summary):
            account_id = str(player.get("account_id", "")).strip()
            rank = normalize_rank_value(player.get("pp_score"))
            if rank == "Unknown":
                rank = normalize_rank_value(player.get("rank"))
            if not account_id or account_id.startswith("unknown-") or rank == "Unknown":
                continue
            known_ranks[account_id] = rank
    return known_ranks


def fetch_ranks(
    *,
    client: StatlockerApiClient,
    account_ids: list[str],
    batch_size: int,
    sleep_seconds: float,
    rate_limit_sleep_seconds: float,
    on_batch_ranks: Callable[[dict[str, str]], None] | None = None,
) -> dict[str, str]:
    ranks: dict[str, str] = {}
    batches = chunked(account_ids, batch_size)
    total_batches = len(batches)

    for batch_index, batch in enumerate(batches, start=1):
        while True:
            try:
                payload = client.fetch_batch_profiles(batch)
                break
            except StatlockerRateLimitError as exc:
                wait_seconds = exc.retry_after_seconds or rate_limit_sleep_seconds
                print(
                    f"[batch {batch_index}/{total_batches}] rate limited, sleeping {wait_seconds:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(wait_seconds)
            except StatlockerApiError as exc:
                print(f"[batch {batch_index}/{total_batches}] failed: {exc}", file=sys.stderr)
                payload = None
                break

        if payload is None:
            time.sleep(sleep_seconds)
            continue

        batch_ranks = extract_batch_profile_ranks(payload)
        matched = 0
        for account_id in batch:
            rank = batch_ranks.get(str(account_id))
            if rank is None:
                continue
            ranks[str(account_id)] = rank
            matched += 1

        if batch_ranks and on_batch_ranks is not None:
            on_batch_ranks(batch_ranks)

        print(
            f"[batch {batch_index}/{total_batches}] requested={len(batch)} matched={matched} total={len(ranks)}"
        )
        time.sleep(sleep_seconds)

    return ranks


def extract_batch_profile_ranks(payload: Any) -> dict[str, str]:
    ranks: dict[str, str] = {}
    for candidate in iter_profile_candidates(payload):
        if not isinstance(candidate, dict):
            continue
        account_id = extract_account_id(candidate)
        if not account_id:
            continue
        rank = extract_rank(candidate)
        if rank:
            ranks[account_id] = rank
    return ranks


def iter_profile_candidates(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("profiles", "results", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return list(payload.values())
    return []


def extract_account_id(candidate: dict[str, Any]) -> str | None:
    direct = _pick_first(candidate, "account_id", "accountId", "steamAccountId", "steam_id")
    if direct is not None:
        return str(direct)

    for nested_key in ("profile", "player", "account", "stats", "aggregate_stats"):
        nested = candidate.get(nested_key)
        if not isinstance(nested, dict):
            continue
        direct = _pick_first(nested, "account_id", "accountId", "steamAccountId", "steam_id")
        if direct is not None:
            return str(direct)

    return None


def extract_rank(candidate: dict[str, Any]) -> str | None:
    direct = normalize_rank_value(
        _pick_first(
            candidate,
            "ppScore",
            "performanceRankMessage",
            "rank",
            "rank_name",
            "display_rank",
            "current_rank",
            "badge_text",
            "medal",
            "estimatedRankNumber",
            "averageMatchRankNumber",
        )
    )
    if direct != "Unknown":
        return direct

    for nested_key in ("profile", "stats", "aggregate_stats", "rank_info", "mmr", "data"):
        nested = candidate.get(nested_key)
        if not isinstance(nested, dict):
            continue
        nested_rank = normalize_rank_value(
            _pick_first(
                nested,
                "ppScore",
                "performanceRankMessage",
                "rank",
                "rank_name",
                "display_rank",
                "current_rank",
                "badge_text",
                "medal",
                "estimatedRankNumber",
                "averageMatchRankNumber",
            )
        )
        if nested_rank != "Unknown":
            return nested_rank

    return None


def apply_ranks_to_summaries(
    summaries: list[dict[str, Any]],
    known_ranks: dict[str, str],
) -> tuple[list[dict[str, Any]], int]:
    updated_players = 0
    for summary in summaries:
        for player in iter_summary_players(summary):
            account_id = str(player.get("account_id", "")).strip()
            if not account_id:
                continue
            rank = known_ranks.get(account_id)
            if not rank:
                continue
            previous = player.get("pp_score")
            if previous is None:
                previous = player.get("rank")
            if previous != rank:
                player["pp_score"] = rank
                if "rank" in player:
                    player.pop("rank", None)
                updated_players += 1
    return summaries, updated_players


def write_jsonl_summaries(path: Path, summaries: list[dict[str, Any]]) -> None:
    lines = [json.dumps(summary) for summary in summaries]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def persist_batch_ranks(
    *,
    batch_ranks: dict[str, str],
    known_ranks: dict[str, str],
    rank_cache_path: Path,
) -> None:
    known_ranks.update(batch_ranks)
    write_rank_cache(rank_cache_path, known_ranks)


def write_csv_dataset(path: Path, summaries: list[dict[str, Any]]) -> None:
    rows = [summary_payload_to_dataset_row(summary) for summary in summaries]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=dataset_fieldnames())
        writer.writeheader()
        writer.writerows(rows)


def iter_summary_players(summary: dict[str, Any]):
    for key in ("team_1_players", "team_2_players"):
        players = summary.get(key)
        if not isinstance(players, list):
            continue
        for player in players:
            if isinstance(player, dict):
                yield player


def normalize_rank_value(value: Any) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    if text.lower() in {"unknown", "none", "null", "n/a"}:
        return "Unknown"
    return text


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _pick_first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


if __name__ == "__main__":
    raise SystemExit(main())
