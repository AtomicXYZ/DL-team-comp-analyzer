# DL-team-comp-analyzer

Deep-learning prototype that predicts the winning side of a Deadlock match from both
team compositions.

## Current Project Flow

The project has one clear pipeline:

```text
Deadlock API normal matches
  -> data/new_patch_matches.jsonl
  -> data/new_patch_team_comp_dataset.csv
  -> PyTorch neural network
  -> models/neural_teamcomp_heroes_only.pt
```

## Main Commands

Fetch 10k normal matches, newest first:

```bash
cd /root/DL-team-comp-analyzer
/root/DL-team-comp-analyzer/.venv/bin/python scripts/fetch_matches.py \
  --output data/new_patch_matches.jsonl \
  --state-file data/new_patch_fetch_state.json \
  --target-count 10000 \
  --batch-size 100 \
  --order-direction desc \
  --sleep-seconds 6.2
```

Build the training CSV:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/build_dataset.py \
  --matches data/new_patch_matches.jsonl \
  --pp-scores data/pp_scores.json \
  --output data/new_patch_team_comp_dataset.csv
```

Train the neural network:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/train_neural_teamcomp.py \
  --dataset data/new_patch_team_comp_dataset.csv \
  --model-output models/neural_teamcomp_heroes_only.pt \
  --epochs 25
```

Run the Streamlit demo:

```bash
/root/DL-team-comp-analyzer/.venv/bin/streamlit run app/streamlit_app.py \
  --server.address 0.0.0.0 \
  --server.port 8501
```

## Neural Model

The main model is in:

- `scripts/train_neural_teamcomp.py`
- `models/neural_teamcomp_heroes_only.pt`
- `models/neural_teamcomp_heroes_only.json`

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
/root/DL-team-comp-analyzer/.venv/bin/python scripts/extract_accounts.py \
  --matches data/new_patch_matches.jsonl \
  --output data/accounts.txt

/root/DL-team-comp-analyzer/.venv/bin/python scripts/fetch_pp_scores.py \
  --accounts data/accounts.txt \
  --output data/pp_scores.json
```

You need a `.env` file with:

```text
STATLOCKER_API_KEY=your_key_here
```

## Kept Scripts

- `scripts/fetch_matches.py`: fetch normal Deadlock match summaries.
- `scripts/build_dataset.py`: convert JSONL matches to training CSV.
- `scripts/train_neural_teamcomp.py`: train the PyTorch model.
- `scripts/extract_accounts.py`: optional account list for Statlocker.
- `scripts/fetch_pp_scores.py`: optional Statlocker ppScore fetch.
- `scripts/common.py`: shared file helpers and paths.
- `app/streamlit_app.py`: interactive lineup-vs-lineup demo.

Old v1/debug scripts were removed to keep the project focused.

## Project Layout

- `scripts/`: commands you run for fetching, dataset building and training.
- `src/`: small reusable API and match-parsing modules used by the scripts.
- `data/`: local fetched/cached data; generated files are ignored by Git.
- `models/`: the selected application models and retained experiment results.

## Saved Models

- `models/neural_teamcomp_heroes_only.pt`: main model used by the Streamlit app.
- `models/neural_teamcomp_heroes_badge.pt`: comparison model using average badge.
- `models/experiments/*.json` and `summary.csv`: retained experiment results.

Experimental `.pt` checkpoints are not kept in Git; train them again from their
recorded configuration when needed.
