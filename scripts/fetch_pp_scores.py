from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
import sys  # Fouten naar stderr schrijven.
import time  # Wachten tussen requests en bij rate limits.
import traceback  # Onverwachte fouten volledig loggen.
from pathlib import Path  # Bestandspaden voor input/output/logs.
from typing import Any  # API-payload kan verschillende JSON-vormen hebben.

from common import ACCOUNTS_PATH, PP_SCORES_PATH, chunked, load_json, read_ids, write_json  # Gedeelde paden en file helpers.

from statlocker_api import (
    StatlockerApiClient,  # Client die Statlocker requests uitvoert.
    StatlockerApiError,  # Algemene Statlocker API-fout.
    StatlockerRateLimitError,  # Specifieke fout voor HTTP 429/rate limits.
)


def parse_args() -> argparse.Namespace:
    """Lees instellingen voor ppScore-fetching uit de terminal."""
    parser = argparse.ArgumentParser(description="Fetch Statlocker ppScore values for account IDs.")
    parser.add_argument("--accounts", type=Path, default=ACCOUNTS_PATH)  # Tekstbestand met account IDs.
    parser.add_argument("--output", type=Path, default=PP_SCORES_PATH)  # JSON-cache die ppScores bewaart.
    parser.add_argument("--log-file", type=Path, default=Path("data/fetch_pp_scores.log"))  # Logbestand voor voortgang/fouten.
    parser.add_argument("--batch-size", type=int, default=100)  # Aantal accounts per batch-request.
    parser.add_argument("--timeout-seconds", type=int, default=45)  # Maximale wachttijd per API-request.
    parser.add_argument("--sleep-seconds", type=float, default=0.1)  # Pauze tussen batches.
    parser.add_argument("--rate-limit-sleep-seconds", type=float, default=300.0)  # Fallback-wachttijd bij rate limit.
    parser.add_argument("--limit-accounts", type=int)  # Optioneel alleen de eerste N accounts verwerken.
    return parser.parse_args()


def main() -> int:
    """Start het script en log onverwachte fouten netjes."""
    args = parse_args()
    try:
        return run(args)
    except KeyboardInterrupt:
        log_message(args.log_file, "Stopped by user")
        print("Stopped by user")
        return 130
    except Exception:
        error_text = traceback.format_exc()
        log_message(args.log_file, error_text.rstrip())
        print(error_text, file=sys.stderr)
        return 1


def run(args: argparse.Namespace) -> int:
    """Haal ontbrekende ppScores op en werk de cache per batch bij."""
    accounts = read_ids(args.accounts)
    if args.limit_accounts is not None:
        accounts = accounts[: args.limit_accounts]

    pp_scores = {str(key): int(value) for key, value in load_json(args.output, {}).items()}
    pending_accounts = [account for account in accounts if account not in pp_scores]
    client = StatlockerApiClient(timeout_seconds=args.timeout_seconds)
    batches = chunked(pending_accounts, max(1, min(args.batch_size, 100)))

    log_message(
        args.log_file,
        f"Loaded {len(accounts)} accounts. {len(pending_accounts)} still need ppScore lookup.",
    )
    for batch_index, batch in enumerate(batches, start=1):
        while True:
            try:
                payload = client.fetch_batch_profiles(batch)
                break
            except StatlockerRateLimitError as exc:
                wait_seconds = exc.retry_after_seconds or args.rate_limit_sleep_seconds
                log_message(
                    args.log_file,
                    f"[batch {batch_index}/{len(batches)}] rate limited, sleeping {wait_seconds:.1f}s",
                )
                time.sleep(wait_seconds)
            except StatlockerApiError as exc:
                log_message(args.log_file, f"[batch {batch_index}/{len(batches)}] failed: {exc}")
                payload = []
                break

        fetched = extract_pp_scores(payload)
        pp_scores.update(fetched)
        write_json(args.output, pp_scores)
        log_message(
            args.log_file,
            f"[batch {batch_index}/{len(batches)}] requested={len(batch)} matched={len(fetched)} cached={len(pp_scores)}",
        )
        time.sleep(args.sleep_seconds)

    return 0


def extract_pp_scores(payload: Any) -> dict[str, int]:
    """Haal account_id -> ppScore uit de Statlocker response."""
    profiles = payload if isinstance(payload, list) else []
    pp_scores: dict[str, int] = {}
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        account_id = profile.get("accountId") or profile.get("account_id")
        pp_score = profile.get("ppScore") or profile.get("pp_score")
        try:
            if account_id is not None and pp_score is not None:
                pp_scores[str(account_id)] = int(pp_score)
        except (TypeError, ValueError):
            continue
    return pp_scores


def log_message(path: Path, message: str) -> None:
    """Schrijf een bericht naar logbestand en terminal."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(message + "\n")
    print(message, flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
