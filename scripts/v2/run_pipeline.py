from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common import ACCOUNTS_PATH, DATASET_PATH, MATCHES_PATH, PP_SCORES_PATH


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the clean v2 data pipeline in order.")
    parser.add_argument("--matches", type=Path, default=MATCHES_PATH)
    parser.add_argument("--accounts", type=Path, default=ACCOUNTS_PATH)
    parser.add_argument("--pp-scores", type=Path, default=PP_SCORES_PATH)
    parser.add_argument("--dataset", type=Path, default=DATASET_PATH)
    parser.add_argument("--skip-fetch-pp", action="store_true")
    parser.add_argument("--require-complete-pp", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    commands = [
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_accounts.py"),
            "--matches",
            str(args.matches),
            "--output",
            str(args.accounts),
        ],
    ]

    if not args.skip_fetch_pp:
        commands.append(
            [
                sys.executable,
                str(SCRIPT_DIR / "fetch_pp_scores.py"),
                "--accounts",
                str(args.accounts),
                "--output",
                str(args.pp_scores),
            ]
        )

    build_command = [
        sys.executable,
        str(SCRIPT_DIR / "build_dataset.py"),
        "--matches",
        str(args.matches),
        "--pp-scores",
        str(args.pp_scores),
        "--output",
        str(args.dataset),
    ]
    if args.require_complete_pp:
        build_command.append("--require-complete-pp")
    commands.append(build_command)

    for command in commands:
        print(f"\n$ {' '.join(command)}", flush=True)
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            return result.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
