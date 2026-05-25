from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from common import ACCOUNTS_PATH, MATCHES_PATH, read_jsonl, write_ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract unique account IDs from match summaries.")
    parser.add_argument("--matches", type=Path, default=MATCHES_PATH)
    parser.add_argument("--output", type=Path, default=ACCOUNTS_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    accounts = sorted(collect_accounts(read_jsonl(args.matches)), key=int)
    write_ids(args.output, accounts)
    print(f"Wrote {len(accounts)} unique accounts to {args.output}")
    return 0


def collect_accounts(matches: list[dict[str, Any]]) -> set[str]:
    accounts: set[str] = set()
    for match in matches:
        for team_key in ("team_1_players", "team_2_players"):
            for player in match.get(team_key, []) or []:
                account_id = str(player.get("account_id", "")).strip()
                if account_id and not account_id.startswith("unknown-"):
                    accounts.add(account_id)
    return accounts


if __name__ == "__main__":
    raise SystemExit(main())
