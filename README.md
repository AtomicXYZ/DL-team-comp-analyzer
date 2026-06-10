# DL-team-comp-analyzer

Deep-learning prototype that predicts the winning side of a Deadlock match from both
team compositions.

## Current Project Flow

The project has one clear pipeline:

```text
Deadlock API normal matches
  -> data/matches_2026-05-22.jsonl
  -> data/team_comp_dataset_2026-05-22.csv
  -> PyTorch neural network
  -> models/2026-05-22/neural_teamcomp_heroes_ppscore_context.pt
```

## Main Commands

Fetch 10k normal matches, newest first:

```bash
cd /root/DL-team-comp-analyzer
/root/DL-team-comp-analyzer/.venv/bin/python scripts/fetch_matches.py \
  --output data/matches_2026-05-22.jsonl \
  --state-file data/fetch_state_2026-05-22.json \
  --target-count 10000 \
  --batch-size 100 \
  --required-patch 2026-05-22 \
  --order-direction desc \
  --sleep-seconds 6.2
```

The fetch defaults to normal matches and only stores matches identified as patch
`2026-05-22`, which went live on May 22, 2026.

Build the training CSV:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/build_dataset.py \
  --matches data/matches_2026-05-22.jsonl \
  --pp-scores data/pp_scores.json \
  --output data/team_comp_dataset_2026-05-22.csv
```

Train the neural network:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/train_neural_teamcomp.py \
  --dataset data/team_comp_dataset_2026-05-22.csv \
  --model-output models/2026-05-22/neural_teamcomp_heroes_only.pt \
  --epochs 25
```

Run the Streamlit demo:

```bash
/root/DL-team-comp-analyzer/.venv/bin/streamlit run app/streamlit_app.py \
  --server.address 0.0.0.0 \
  --server.port 8501
```

The demo uses the rank-aware model and accepts either one average Statlocker
ppScore per team or an individual ppScore for each selected hero.

The app shows local hero portraits from `app/assets/heroes`. Refresh those
assets from the Deadlock API with:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/download_hero_assets.py
```

## Neural Model

The main model is in:

- `scripts/train_neural_teamcomp.py`
- `models/2026-05-22/neural_teamcomp_heroes_only.pt`
- `models/2026-05-22/neural_teamcomp_heroes_only.json`

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

The heroes-only model remains the baseline. The current Streamlit model adds
Statlocker ppScore features because that comparison performed better on the
current-patch test split.

Current `2026-05-22` heroes-only result:

- architecture: `pool`
- best validation epoch: `12`
- test accuracy: `0.5885`
- test log loss: `0.6741`

Rank-aware comparison using available ppScores on all `10000` matches:

- ppScore slot coverage: `74.94%`
- test accuracy: `0.6005`
- test log loss: `0.6687`

The relative-only rank-aware comparison scored highest on the test split, but
cannot adjust a hero composition when both teams move to the same higher rank.
The Streamlit model therefore uses the context-aware variant in
`models/2026-05-22/neural_teamcomp_heroes_ppscore_context.pt`; it includes
absolute lobby ppScore. A tuning sweep over activation, layer size, learning
rate and regularisation selected a `pool` model with embedding dimension `24`
and hidden dimension `128`. After expanding the patch dataset to `20000`
matches and testing more variants, the selected model now uses `SiLU` and
reached validation log loss `0.6611`, test accuracy `0.6082` and test log loss
`0.6573` on its `4000`-match holdout.

Re-run that focused sweep with:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/run_ppscore_context_experiments.py
```

A corrected comparison on only the `2698` fully ranked matches did not improve
validation performance: the best ppScore context model reached validation log
loss `0.6877`, versus `0.6860` for heroes-only on that subset. The app
therefore keeps the `10000`-match context model.

## Optional Statlocker ppScore Step

These scripts are kept because individual player ppScore/rank can be added later:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/extract_accounts.py \
  --matches data/matches_2026-05-22.jsonl \
  --output data/accounts_2026-05-22.txt

/root/DL-team-comp-analyzer/.venv/bin/python scripts/fetch_pp_scores.py \
  --accounts data/accounts_2026-05-22.txt \
  --output data/pp_scores.json
```

After ppScores are complete, rebuild the dataset and train a separate comparison
model without overwriting the Streamlit model:

```bash
/root/DL-team-comp-analyzer/.venv/bin/python scripts/build_dataset.py \
  --matches data/matches_2026-05-22.jsonl \
  --pp-scores data/pp_scores.json \
  --output data/team_comp_dataset_2026-05-22_ppscore_complete.csv \
  --require-complete-pp

/root/DL-team-comp-analyzer/.venv/bin/python scripts/train_neural_teamcomp.py \
  --dataset data/team_comp_dataset_2026-05-22_ppscore_complete.csv \
  --model-output models/2026-05-22/experiments/heroes_ppscore_complete.pt \
  --use-pp-score \
  --epochs 80 \
  --architecture pool \
  --embedding-dim 16 \
  --hidden-dim 96 \
  --dropout 0.40 \
  --learning-rate 0.0005 \
  --weight-decay 0.002 \
  --l1-lambda 0.0000005 \
  --patience 8 \
  --batch-size 256
```

You need a `.env` file with:

```text
STATLOCKER_API_KEY=your_key_here
```

## Kept Scripts

- `scripts/fetch_matches.py`: fetch normal Deadlock match summaries.
- `scripts/build_dataset.py`: convert JSONL matches to training CSV.
- `scripts/train_neural_teamcomp.py`: train the PyTorch model.
- `scripts/run_ppscore_context_experiments.py`: compare rank-context model hyperparameters.
- `scripts/download_hero_assets.py`: download hero portraits for the Streamlit app.
- `scripts/extract_accounts.py`: optional account list for Statlocker.
- `scripts/fetch_pp_scores.py`: optional Statlocker ppScore fetch.
- `scripts/common.py`: shared file helpers and paths.
- `app/streamlit_app.py`: interactive lineup-vs-lineup demo.

Old v1/debug scripts were removed to keep the project focused.

## Project Layout

- `scripts/`: commands you run for fetching, dataset building and training.
- `src/`: small reusable API and match-parsing modules used by the scripts.
- `data/`: local fetched/cached data; generated files are ignored by Git.
- `models/<patch>/`: selected application models and experiment results for each patch.

## Saved Models

- `models/2026-05-22/neural_teamcomp_heroes_only.pt`: heroes-only baseline model.
- `models/2026-05-22/neural_teamcomp_heroes_ppscore_partial.pt`: relative-only rank-aware comparison model.
- `models/2026-05-22/neural_teamcomp_heroes_ppscore_context.pt`: context-aware rank model used by the Streamlit app.
- `models/2026-05-22/experiments/`: new-patch comparisons as they are trained.
- `models/2026-04-30/`: archived previous-patch model and experiment results.

Experimental `.pt` checkpoints are not kept in Git; train them again from their
recorded configuration when needed.
