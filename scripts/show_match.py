from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dl_team_comp_analyzer.deadlock_api import DeadlockApiClient, DeadlockApiError
from dl_team_comp_analyzer.match_parser import (
    build_match_view,
    format_match_view,
    match_view_to_dict,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show the teams, player ranks, winner and patch for one Deadlock match."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--match-id", help="Deadlock match id to fetch from the API.")
    source.add_argument("--json", help="Path to a previously saved raw match JSON file.")

    parser.add_argument(
        "--save-raw",
        help="Optional path where the fetched raw API response should be saved.",
    )
    parser.add_argument(
        "--save-summary",
        help="Optional path where the normalized summary JSON should be saved.",
    )
    parser.add_argument(
        "--as-json",
        action="store_true",
        help="Print the normalized summary as JSON instead of a table.",
    )
    parser.add_argument(
        "--game-api-base",
        default="https://api.deadlock-api.com/v1",
        help="Base URL for the Deadlock game data API.",
    )
    parser.add_argument(
        "--assets-api-base",
        default="https://assets.deadlock-api.com/v2",
        help="Base URL for the Deadlock assets API.",
    )
    return parser.parse_args()


def load_payload(args: argparse.Namespace, client: DeadlockApiClient) -> dict:
    if args.json:
        return json.loads(Path(args.json).read_text(encoding="utf-8"))

    payload = client.fetch_match_metadata(args.match_id)
    if args.save_raw:
        client.save_json(payload, args.save_raw)
    return payload


def main() -> int:
    args = parse_args()
    client = DeadlockApiClient(
        game_api_base=args.game_api_base,
        assets_api_base=args.assets_api_base,
    )

    try:
        payload = load_payload(args, client)
        match_view = build_match_view(payload, hero_resolver=client.get_hero_name)
    except (DeadlockApiError, FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.as_json:
        summary = match_view_to_dict(match_view)
        if args.save_summary:
            Path(args.save_summary).parent.mkdir(parents=True, exist_ok=True)
            Path(args.save_summary).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
    else:
        print(format_match_view(match_view))
        if args.save_summary:
            summary = match_view_to_dict(match_view)
            Path(args.save_summary).parent.mkdir(parents=True, exist_ok=True)
            Path(args.save_summary).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
