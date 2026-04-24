from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dl_team_comp_analyzer.bulk_extract import extract_match_ids_from_history_payload
from dl_team_comp_analyzer.deadlock_api import DeadlockApiClient, DeadlockApiError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch match IDs from one or more player match histories."
    )
    parser.add_argument("--account-id", action="append", help="Repeatable player account id.")
    parser.add_argument("--seed-file", help="Optional file with one account id per line.")
    parser.add_argument("--output", default="data/processed/seed_match_ids.txt")
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Fetch full history instead of only the stored/cached history.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--game-api-base", default="https://api.deadlock-api.com/v1")
    parser.add_argument("--assets-api-base", default="https://assets.deadlock-api.com/v2")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    account_ids = collect_account_ids(args)
    if not account_ids:
        print("Provide at least one --account-id or a --seed-file", file=sys.stderr)
        return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_existing_ids(output_path)
    client = DeadlockApiClient(
        game_api_base=args.game_api_base,
        assets_api_base=args.assets_api_base,
    )

    added = 0
    for index, account_id in enumerate(account_ids, start=1):
        try:
            payload = client.fetch_player_match_history(
                account_id,
                only_stored_history=not args.all_history,
            )
        except DeadlockApiError as exc:
            print(f"[{index}/{len(account_ids)}] account {account_id} failed: {exc}", file=sys.stderr)
            continue

        match_ids = extract_match_ids_from_history_payload(payload)
        new_ids = [match_id for match_id in match_ids if str(match_id) not in existing]
        if new_ids:
            with output_path.open("a", encoding="utf-8") as file:
                for match_id in new_ids:
                    file.write(f"{match_id}\n")
                    existing.add(str(match_id))
                    added += 1

        print(
            f"[{index}/{len(account_ids)}] account={account_id} returned={len(match_ids)} new={len(new_ids)} total_new={added}"
        )
        time.sleep(args.sleep_seconds)

    return 0


def collect_account_ids(args: argparse.Namespace) -> list[int]:
    account_ids: list[int] = []
    if args.account_id:
        for value in args.account_id:
            try:
                account_ids.append(int(value))
            except ValueError:
                continue

    if args.seed_file:
        for raw_line in Path(args.seed_file).read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                account_ids.append(int(line))
            except ValueError:
                continue

    deduped: list[int] = []
    seen: set[int] = set()
    for account_id in account_ids:
        if account_id in seen:
            continue
        deduped.append(account_id)
        seen.add(account_id)
    return deduped


def load_existing_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


if __name__ == "__main__":
    raise SystemExit(main())
