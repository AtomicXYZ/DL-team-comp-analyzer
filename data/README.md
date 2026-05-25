# Data

The active pipeline uses local generated files in `data/`. These generated
artifacts are ignored by Git because match data and Statlocker lookups can be
large and are reproducible from the fetch scripts.

Useful local artifacts:

- `new_patch_matches.jsonl`: clean normal-mode matches from patch `2026-04-30`.
- `new_patch_team_comp_dataset.csv`: flat training dataset.
- `pp_scores.json`: cached Statlocker ppScore values.
- `accounts.txt`: account IDs extracted from the clean matches for ppScore fetching.

Do not commit API keys, fetched raw match files, fetch state, or generated datasets.
