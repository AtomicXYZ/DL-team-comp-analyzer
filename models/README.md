# Models

Models are grouped by the gameplay patch used for their training data.

## Current Patch: 2026-05-22

- `2026-05-22/neural_teamcomp_heroes_only.pt`: lineup-only baseline model.
- `2026-05-22/neural_teamcomp_heroes_only.json`: baseline metrics and training configuration.
- `2026-05-22/neural_teamcomp_heroes_ppscore_partial.pt`: relative-only
  rank-aware comparison model.
- `2026-05-22/neural_teamcomp_heroes_ppscore_context.pt`: current Streamlit
  model; it also sees the absolute lobby ppScore level.
- `2026-05-22/experiments/summary.csv`: candidate comparison on this patch.

Current selected heroes-only model: `pool` architecture, test accuracy `0.5885`
and test log loss `0.6741`. Candidate checkpoints are not retained unless they
are promoted to an application model; their JSON metrics remain available.

The first ppScore comparison on the `2698` matches with ppScores available for
all 12 players used an older relative-only feature format. A new controlled
complete-subset sweep retrained the corrected lobby-context features. Its best
ppScore validation loss was `0.6877`, while a heroes-only model on the same
subset reached `0.6860`. The clean subset is currently too small to improve on
the partially filled `10000`-match training set, so it is not promoted.

A rank-aware experiment using relative ppScore differences on all `10000`
matches obtained test accuracy `0.6005` and test log loss `0.6687`. However,
equal rank changes could not affect a lineup prediction in that representation.
The Streamlit model now also includes absolute lobby ppScore level, allowing
hero compositions to vary by skill context. A context-model sweep tested
activation functions and regularisation settings. The selected `pool` model
uses `GELU`, embedding dimension `24`, hidden dimension `128`, dropout `0.40`,
learning rate `0.0004` and weight decay `0.0015`.

After expanding the patch dataset to `20000` matches, another tuning pass tested
`SiLU`, lower regularisation and a `matchup` architecture. The selected context
model now uses `SiLU` with the same embedding and hidden sizes. It achieved
validation log loss `0.6611`, test accuracy `0.6082` and test log loss `0.6573`
on its `4000` match holdout. On the previous `10000`-match holdout, it also
improved validation loss (`0.6563` versus `0.6580`) and accuracy (`0.6095`
versus `0.6000`) compared with the previous 20k `GELU` checkpoint.

Reproduce the rank-context tuning sweep with:

```bash
.venv/bin/python scripts/run_ppscore_context_experiments.py
```

Reproduce the complete-ppScore subset comparison with:

```bash
.venv/bin/python scripts/run_ppscore_context_experiments.py \
  --dataset data/team_comp_dataset_2026-05-22_ppscore_complete.csv \
  --name-suffix _complete
```

## Archived Patch: 2026-04-30

- `2026-04-30/neural_teamcomp_heroes_badge.pt`: archived comparison model.
- `2026-04-30/experiments/`: retained metrics from the earlier patch.

Do not compare accuracy across patches as a controlled architecture experiment:
the matches and patch balance differ. Compare candidate models within the same
patch and split.
