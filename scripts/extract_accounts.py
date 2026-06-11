from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
from pathlib import Path  # Bestandspaden voor input/output.
from typing import Any  # Matchdata komt uit JSON en kan gemengd zijn.

from common import ACCOUNTS_PATH, MATCHES_PATH, read_jsonl, write_ids  # Gedeelde paden en file helpers.


def parse_args() -> argparse.Namespace:
    """Lees waar matches vandaan komen en waar account IDs heen moeten."""
    parser = argparse.ArgumentParser(description="Extract unique account IDs from match summaries.")
    parser.add_argument("--matches", type=Path, default=MATCHES_PATH)  # JSONL met opgeslagen matches.
    parser.add_argument("--output", type=Path, default=ACCOUNTS_PATH)  # Tekstbestand met unieke account IDs.
    return parser.parse_args()


def main() -> int:
    """Lees matches, verzamel unieke accounts en schrijf ze naar tekst."""
    args = parse_args()
    accounts = sorted(collect_accounts(read_jsonl(args.matches)), key=int)
    write_ids(args.output, accounts)
    print(f"Wrote {len(accounts)} unique accounts to {args.output}")
    return 0


def collect_accounts(matches: list[dict[str, Any]]) -> set[str]:
    """Verzamel echte account IDs uit team_1_players en team_2_players."""
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
