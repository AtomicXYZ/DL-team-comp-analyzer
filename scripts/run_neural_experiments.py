from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
import csv  # Samenvatting als CSV schrijven.
import json  # Modelmetadata na elke run lezen.
import subprocess  # Trainingsscript als aparte processen starten.
import sys  # Huidige Python executable gebruiken.
from pathlib import Path  # Bestandspaden voor dataset/models.


DEFAULT_DATASET = Path("data/team_comp_dataset_2026-05-22.csv")
DEFAULT_OUTPUT_DIR = Path("models/2026-05-22/experiments")


EXPERIMENTS = [
    {
        "name": "pool_regularized",
        "epochs": "80",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.40",
        "architecture": "pool",
        "learning_rate": "0.0005",
        "weight_decay": "0.002",
        "l1_lambda": "0.0000005",
        "patience": "8",
        "batch_size": "256",
    },
    {
        "name": "pool_medium_less_dropout",
        "epochs": "80",
        "embedding_dim": "16",
        "hidden_dim": "128",
        "dropout": "0.35",
        "architecture": "pool",
        "learning_rate": "0.0005",
        "weight_decay": "0.001",
        "l1_lambda": "0.0000005",
        "patience": "8",
        "batch_size": "256",
    },
    {
        "name": "pool_balanced",
        "epochs": "80",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.30",
        "architecture": "pool",
        "learning_rate": "0.0007",
        "weight_decay": "0.001",
        "l1_lambda": "0.0000003",
        "patience": "8",
        "batch_size": "256",
    },
    {
        "name": "mean_regularized",
        "epochs": "80",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.35",
        "architecture": "mean",
        "learning_rate": "0.0005",
        "weight_decay": "0.002",
        "l1_lambda": "0.0000005",
        "patience": "8",
        "batch_size": "256",
    },
    {
        "name": "matchup_regularized",
        "epochs": "80",
        "embedding_dim": "16",
        "hidden_dim": "96",
        "dropout": "0.40",
        "architecture": "matchup",
        "learning_rate": "0.0005",
        "weight_decay": "0.002",
        "l1_lambda": "0.0000005",
        "patience": "8",
        "batch_size": "256",
    },
]


def parse_args() -> argparse.Namespace:
    """Lees instellingen voor het draaien van experimenten."""
    parser = argparse.ArgumentParser(description="Run heroes-only model candidates on the current patch dataset.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)  # CSV dataset voor alle experimenten.
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)  # Map voor modellen en summary.
    parser.add_argument("--limit", type=int, help="Run only the first N experiments.")  # Optioneel alleen eerste N configs.
    return parser.parse_args()


def main() -> int:
    """Run elke experimentconfig en schrijf een summary CSV."""
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
            "--l1-lambda",
            experiment["l1_lambda"],
            "--patience",
            experiment["patience"],
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
        validation = metadata["metrics"]["validation"]
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
                "best_epoch": str(metadata["best_epoch"]),
                "train_accuracy": f"{train['accuracy']:.4f}",
                "train_log_loss": f"{train['log_loss']:.4f}",
                "validation_accuracy": f"{validation['accuracy']:.4f}",
                "validation_log_loss": f"{validation['log_loss']:.4f}",
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
