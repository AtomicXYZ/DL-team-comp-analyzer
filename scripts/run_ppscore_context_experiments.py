from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


DEFAULT_DATASET = Path("data/team_comp_dataset_2026-05-22.csv")
DEFAULT_OUTPUT_DIR = Path("models/2026-05-22/experiments")


EXPERIMENTS = [
    {
        "name": "context_gelu_balanced",
        "activation": "gelu",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.35",
        "learning_rate": "0.0005",
        "weight_decay": "0.001",
        "l1_lambda": "0.0000003",
    },
    {
        "name": "context_silu_balanced",
        "activation": "silu",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.35",
        "learning_rate": "0.0005",
        "weight_decay": "0.001",
        "l1_lambda": "0.0000003",
    },
    {
        "name": "context_gelu_medium",
        "activation": "gelu",
        "embedding_dim": "24",
        "hidden_dim": "128",
        "dropout": "0.40",
        "learning_rate": "0.0004",
        "weight_decay": "0.0015",
        "l1_lambda": "0.0000003",
    },
    {
        "name": "context_relu_lower_reg",
        "activation": "relu",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.30",
        "learning_rate": "0.0007",
        "weight_decay": "0.001",
        "l1_lambda": "0.0000003",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tune rank-context ppScore models on the current patch.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--name-suffix", default="", help="Append a label such as _complete to output names.")
    parser.add_argument("--limit", type=int, help="Run only the first N candidates.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trainer = Path(__file__).resolve().parent / "train_neural_teamcomp.py"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    experiments = EXPERIMENTS[: args.limit] if args.limit else EXPERIMENTS
    summary_rows: list[dict[str, str]] = []

    for experiment in experiments:
        name = f"{experiment['name']}{args.name_suffix}"
        model_path = args.output_dir / f"{name}.pt"
        command = [
            sys.executable,
            str(trainer),
            "--dataset",
            str(args.dataset),
            "--model-output",
            str(model_path),
            "--epochs",
            "80",
            "--architecture",
            "pool",
            "--activation",
            experiment["activation"],
            "--embedding-dim",
            experiment["embedding_dim"],
            "--hidden-dim",
            experiment["hidden_dim"],
            "--dropout",
            experiment["dropout"],
            "--learning-rate",
            experiment["learning_rate"],
            "--weight-decay",
            experiment["weight_decay"],
            "--l1-lambda",
            experiment["l1_lambda"],
            "--patience",
            "8",
            "--batch-size",
            "256",
            "--use-pp-score",
        ]
        print(f"\n=== {name} ===", flush=True)
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            return result.returncode

        metadata = json.loads(model_path.with_suffix(".json").read_text(encoding="utf-8"))
        validation = metadata["metrics"]["validation"]
        test = metadata["metrics"]["test"]
        summary_rows.append(
            {
                "name": name,
                "activation": experiment["activation"],
                "embedding_dim": experiment["embedding_dim"],
                "hidden_dim": experiment["hidden_dim"],
                "dropout": experiment["dropout"],
                "learning_rate": experiment["learning_rate"],
                "weight_decay": experiment["weight_decay"],
                "l1_lambda": experiment["l1_lambda"],
                "best_epoch": str(metadata["best_epoch"]),
                "validation_accuracy": f"{validation['accuracy']:.4f}",
                "validation_log_loss": f"{validation['log_loss']:.4f}",
                "test_accuracy": f"{test['accuracy']:.4f}",
                "test_log_loss": f"{test['log_loss']:.4f}",
                "model_path": str(model_path),
            }
        )

    summary_path = args.output_dir / f"context_tuning{args.name_suffix}_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"\nWrote context tuning summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
