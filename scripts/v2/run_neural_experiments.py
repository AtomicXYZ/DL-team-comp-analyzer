from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


DEFAULT_DATASET = Path("data/v2/new_patch_team_comp_dataset.csv")
DEFAULT_OUTPUT_DIR = Path("models/v2/experiments")


EXPERIMENTS = [
    {
        "name": "tiny_heroes_only",
        "epochs": "20",
        "embedding_dim": "8",
        "hidden_dim": "64",
        "dropout": "0.10",
        "architecture": "mean",
        "learning_rate": "0.001",
        "weight_decay": "0.0001",
        "batch_size": "256",
    },
    {
        "name": "medium_heroes_only",
        "epochs": "25",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.15",
        "architecture": "mean",
        "learning_rate": "0.001",
        "weight_decay": "0.0001",
        "batch_size": "256",
    },
    {
        "name": "large_heroes_only",
        "epochs": "35",
        "embedding_dim": "32",
        "hidden_dim": "256",
        "dropout": "0.25",
        "architecture": "mean",
        "learning_rate": "0.0007",
        "weight_decay": "0.0002",
        "batch_size": "256",
    },
    {
        "name": "medium_long_regularized",
        "epochs": "60",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.35",
        "architecture": "mean",
        "learning_rate": "0.0005",
        "weight_decay": "0.001",
        "batch_size": "256",
    },
    {
        "name": "large_strong_regularized",
        "epochs": "60",
        "embedding_dim": "32",
        "hidden_dim": "256",
        "dropout": "0.45",
        "architecture": "mean",
        "learning_rate": "0.0004",
        "weight_decay": "0.002",
        "batch_size": "256",
    },
    {
        "name": "medium_no_swap",
        "epochs": "35",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.20",
        "architecture": "mean",
        "learning_rate": "0.001",
        "weight_decay": "0.0002",
        "batch_size": "256",
        "extra_flags": ["--no-swap-augmentation"],
    },
    {
        "name": "medium_random_split",
        "epochs": "35",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.20",
        "architecture": "mean",
        "learning_rate": "0.001",
        "weight_decay": "0.0002",
        "batch_size": "256",
        "extra_flags": ["--split", "random"],
    },
    {
        "name": "medium_with_badge_reference",
        "epochs": "35",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.20",
        "architecture": "mean",
        "learning_rate": "0.001",
        "weight_decay": "0.0002",
        "batch_size": "256",
        "extra_flags": ["--use-badge"],
    },
    {
        "name": "medium_pool",
        "epochs": "45",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.30",
        "architecture": "pool",
        "learning_rate": "0.0007",
        "weight_decay": "0.0007",
        "batch_size": "256",
    },
    {
        "name": "medium_matchup",
        "epochs": "45",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.35",
        "architecture": "matchup",
        "learning_rate": "0.0005",
        "weight_decay": "0.001",
        "batch_size": "256",
    },
    {
        "name": "large_matchup",
        "epochs": "55",
        "embedding_dim": "24",
        "hidden_dim": "192",
        "dropout": "0.40",
        "architecture": "matchup",
        "learning_rate": "0.0004",
        "weight_decay": "0.0015",
        "batch_size": "256",
    },
    {
        "name": "medium_ppscore_reference",
        "epochs": "45",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.30",
        "architecture": "pool",
        "learning_rate": "0.0007",
        "weight_decay": "0.0007",
        "batch_size": "256",
        "extra_flags": ["--use-pp-score"],
    },
    {
        "name": "medium_badge_ppscore_reference",
        "epochs": "45",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.30",
        "architecture": "pool",
        "learning_rate": "0.0007",
        "weight_decay": "0.0007",
        "batch_size": "256",
        "extra_flags": ["--use-badge", "--use-pp-score"],
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small neural model-size experiment sweep.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, help="Run only the first N experiments.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script = Path(__file__).resolve().parent / "train_neural_teamcomp.py"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    selected = EXPERIMENTS[: args.limit] if args.limit else EXPERIMENTS
    summary_rows: list[dict[str, str]] = []

    for experiment in selected:
        name = experiment["name"]
        model_path = args.output_dir / f"{name}.pt"
        command = [
            sys.executable,
            str(script),
            "--dataset",
            str(args.dataset),
            "--model-output",
            str(model_path),
            "--epochs",
            experiment["epochs"],
            "--embedding-dim",
            experiment["embedding_dim"],
            "--hidden-dim",
            experiment["hidden_dim"],
            "--dropout",
            experiment["dropout"],
            "--architecture",
            experiment["architecture"],
            "--learning-rate",
            experiment["learning_rate"],
            "--weight-decay",
            experiment["weight_decay"],
            "--batch-size",
            experiment["batch_size"],
        ]
        command.extend(experiment.get("extra_flags", []))
        print(f"\n=== {name} ===", flush=True)
        print("$ " + " ".join(command), flush=True)
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            return result.returncode

        metadata = json.loads(model_path.with_suffix(".json").read_text(encoding="utf-8"))
        test = metadata["metrics"]["test"]
        train = metadata["metrics"]["train"]
        summary_rows.append(
            {
                "name": name,
                "embedding_dim": experiment["embedding_dim"],
                "hidden_dim": experiment["hidden_dim"],
                "architecture": experiment["architecture"],
                "dropout": experiment["dropout"],
                "batch_size": experiment["batch_size"],
                "epochs": experiment["epochs"],
                "extra_flags": " ".join(experiment.get("extra_flags", [])),
                "train_accuracy": f"{train['accuracy']:.4f}",
                "train_log_loss": f"{train['log_loss']:.4f}",
                "test_accuracy": f"{test['accuracy']:.4f}",
                "test_log_loss": f"{test['log_loss']:.4f}",
                "model_path": str(model_path),
            }
        )

    summary_path = args.output_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\nWrote experiment summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
