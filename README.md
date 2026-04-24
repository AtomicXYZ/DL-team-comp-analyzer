# DL-team-comp-analyzer

First prototype for a `Deadlock Team Comp Analyzer`.

## Current Goal

Show the most important fields from one match:

- team 1 heroes and rank per player
- team 2 heroes and rank per player
- winner
- patch

This gives us a clean first step toward building the training dataset for the model.

For the MVP, player rank may stay unavailable for some matches. In that case we still keep:

- both team comps
- winner
- match start time
- inferred patch
- average badge per team

## What Is Included

- `scripts/show_match.py`
  Fetches one match from the Deadlock API or reads a saved raw JSON file.
- `scripts/fetch_bulk_summaries.py`
  Efficiently downloads many lightweight match summaries and appends them to `JSONL + CSV`.
- `scripts/fetch_player_match_ids.py`
  Fallback helper that harvests match IDs from one or more player match-history endpoints.
- `src/dl_team_comp_analyzer/deadlock_api.py`
  Small Deadlock API client for match metadata and hero names.
- `src/dl_team_comp_analyzer/match_parser.py`
  Normalizes raw match JSON into a simple structure we can reuse later for dataset creation.

## Usage

Fetch one match directly from the API:

```bash
python scripts/show_match.py --match-id 123456789
```

Fetch one match and save the raw response for debugging:

```bash
python scripts/show_match.py --match-id 123456789 --save-raw data/raw/match_123456789.json
```

Fetch one match and also save a compact normalized summary:

```bash
python scripts/show_match.py --match-id 123456789 --save-raw data/raw/match_123456789.json --save-summary data/processed/match_123456789.summary.json
```

Read a previously saved raw response:

```bash
python scripts/show_match.py --json data/raw/match_123456789.json
```

Print the normalized output as JSON:

```bash
python scripts/show_match.py --match-id 123456789 --as-json
```

Fetch many lightweight summaries directly from the bulk endpoint:

```bash
python scripts/fetch_bulk_summaries.py --target-count 10000 --batch-size 100 --resume
```

Fetch match IDs from seed players first, then resolve those exact match IDs:

```bash
python scripts/fetch_player_match_ids.py --account-id 17964440 --account-id 9261994 --output data/processed/seed_match_ids.txt
python scripts/fetch_bulk_summaries.py --match-ids-file data/processed/seed_match_ids.txt --batch-size 100
```

Merge Statlocker profile ranks into the already collected dataset without re-fetching matches:

```bash
cp .env.example .env
# put your real Statlocker key in .env as STATLOCKER_API_KEY=...
/root/DL-team-comp-analyzer/.venv/bin/python scripts/enrich_statlocker_ranks.py --jsonl-only
```

This script does not call the Statlocker match endpoints. It reads existing `account_id`s from
`match_summaries.jsonl`, fetches unique profile ranks in batches of up to `100` accounts, stores a
local cache in `data/processed/statlocker_profile_ranks.json`, and writes the rank back into each
player entry.

## Notes

- The Deadlock API endpoints used here are based on the public match metadata and assets APIs:
  - `https://api.deadlock-api.com/v1/matches/{match_id}/metadata`
  - `https://assets.deadlock-api.com/v2/heroes/{hero_id}`
- The parser is intentionally defensive because public docs confirm the endpoints, but the exact match response shape can vary over time.
- If a field is missing, the script falls back to `Unknown` instead of crashing immediately.
- If the patch is not present in the API response, it is inferred from `start_time` using a local patch release table.
- For large-scale collection, prefer the bulk metadata endpoint with player stats/items/death details disabled.

## Recommended Collection Strategy

If you want `10,000+` matches on a VM, use this order:

1. Start with `scripts/fetch_bulk_summaries.py`
   This is the most bandwidth-efficient route because it requests only match info + player info.
2. Use `--resume`
   The script stores a cursor in `data/processed/fetch_state.json` so it can keep running across restarts.
3. Keep the batch size moderate
   `100` is a good starting point.
4. Keep a small delay between bulk calls
   `0.35s` is a safe default and matches the published bulk endpoint limits much better than hammering it.
5. Use `scripts/fetch_player_match_ids.py` only as a fallback
   This is useful if you want to target matches around specific players or if the bulk endpoint does not give you the slice you want.

Notes:

- Your `20 GB VRAM` is not important for data collection. For this step, network stability, disk space, and resumable scripts matter much more.
- Avoid storing raw match JSON for all games. A single raw match can be several megabytes, so `10,000` raw matches would become unreasonably large.
- The dataset CSV is the main artifact you will train on; the JSONL file is there for debugging and future feature engineering.

## Next Step

Once this works for a few thousand matches, the next step is to train a first baseline model on columns like:

- `match_id`
- `patch`
- `team_1_hero_1` ... `team_1_hero_6`
- `team_2_hero_1` ... `team_2_hero_6`
- `team_1_average_badge`
- `team_2_average_badge`
- `winner`
