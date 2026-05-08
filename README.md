# DL-team-comp-analyzer

Deep-learning prototype that predicts the winning side of a Deadlock match from both
team compositions.

## Current Project Flow

The project now has one clear v2 pipeline:

```text
Deadlock API normal matches
  -> data/v2/new_patch_matches.jsonl
  -> data/v2/new_patch_team_comp_dataset.csv
  -> PyTorch neural network
  -> models/v2/neural_teamcomp_heroes_only.pt
```

## Main Commands

Fetch 10k normal matches, newest first:

```bash
cd /root/DL-team-comp-analyzer
/root/DL-team-comp-analyzer/.venv/bin/python scripts/v2/fetch_matches.py \
  --output data/v2/new_patch_matches.jsonl \
  --state-file data/v2/new_patch_fetch_state.json \
  --target-count 10000 \
  --batch-size 100 \
  --order-direction desc \
  --sleep-seconds 6.2
```

Build the training CSV:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/v2/build_dataset.py \
  --matches data/v2/new_patch_matches.jsonl \
  --pp-scores data/v2/pp_scores.json \
  --output data/v2/new_patch_team_comp_dataset.csv
```

Train the neural network:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/v2/train_neural_teamcomp.py \
  --dataset data/v2/new_patch_team_comp_dataset.csv \
  --model-output models/v2/neural_teamcomp_heroes_only.pt \
  --epochs 25
```

## Neural Model

The main model is in:

- `scripts/v2/train_neural_teamcomp.py`
- `models/v2/neural_teamcomp_heroes_only.pt`
- `models/v2/neural_teamcomp_heroes_only.json`

Input:

- 6 hero IDs for team 1
- 6 hero IDs for team 2

Target:

- `winner_team_index`
- `0` means team 1 won
- `1` means team 2 won

Architecture:

```text
hero_id -> nn.Embedding
team 1 embeddings -> team representation
team 2 embeddings -> team representation
team comparison features
MLP -> probability that team 2 wins
```

By default the model does not use `average_badge`, Statlocker ranks, or ppScore. It
learns from hero composition only. You can pass `--use-badge` for a comparison
experiment, but the main model intentionally avoids that team-average rank summary.

## Optional Statlocker ppScore Step

These scripts are kept because individual player ppScore/rank can be added later:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/v2/extract_accounts.py \
  --matches data/v2/new_patch_matches.jsonl \
  --output data/v2/accounts.txt

/root/DL-team-comp-analyzer/.venv/bin/python scripts/v2/fetch_pp_scores.py \
  --accounts data/v2/accounts.txt \
  --output data/v2/pp_scores.json
```

You need a `.env` file with:

```text
STATLOCKER_API_KEY=your_key_here
```

## Kept Scripts

- `scripts/v2/fetch_matches.py`: fetch normal Deadlock match summaries.
- `scripts/v2/build_dataset.py`: convert JSONL matches to training CSV.
- `scripts/v2/train_neural_teamcomp.py`: train the PyTorch model.
- `scripts/v2/extract_accounts.py`: optional account list for Statlocker.
- `scripts/v2/fetch_pp_scores.py`: optional Statlocker ppScore fetch.
- `scripts/v2/common.py`: shared file helpers and paths.

Old v1/debug scripts were removed to keep the project focused.
