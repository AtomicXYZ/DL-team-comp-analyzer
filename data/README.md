# Data

The active pipeline uses local generated files in `data/`. These generated
artifacts are ignored by Git because match data and Statlocker lookups can be
large and are reproducible from the fetch scripts.

Useful local artifacts:

- `matches_2026-05-22.jsonl`: current clean normal-mode matches for training.
- `team_comp_dataset_2026-05-22.csv`: current flat training dataset.
- `pp_scores.json`: cached Statlocker ppScore values.
- `accounts_2026-05-22.txt`: account IDs extracted from the current matches for ppScore fetching.

The earlier `2026-04-30` files may be retained locally as an archived comparison
dataset, but models for the current patch should be trained on the `2026-05-22`
files.

Do not commit API keys, fetched raw match files, fetch state, or generated datasets.
