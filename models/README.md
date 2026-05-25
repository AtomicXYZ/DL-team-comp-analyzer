# Models

Models are grouped by the gameplay patch used for their training data.

## Current Patch: 2026-05-22

- `2026-05-22/neural_teamcomp_heroes_only.pt`: current Streamlit model.
- `2026-05-22/neural_teamcomp_heroes_only.json`: metrics and training configuration.
- `2026-05-22/experiments/summary.csv`: candidate comparison on this patch.

Current selected heroes-only model: `pool` architecture, test accuracy `0.5885`
and test log loss `0.6741`. Candidate checkpoints are not retained unless they
are promoted to an application model; their JSON metrics remain available.

## Archived Patch: 2026-04-30

- `2026-04-30/neural_teamcomp_heroes_badge.pt`: archived comparison model.
- `2026-04-30/experiments/`: retained metrics from the earlier patch.

Do not compare accuracy across patches as a controlled architecture experiment:
the matches and patch balance differ. Compare candidate models within the same
patch and split.
