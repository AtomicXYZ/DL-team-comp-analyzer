from __future__ import annotations

import argparse
import csv
import json
import math
import random
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset


DEFAULT_DATASET = Path("data/v2/new_patch_team_comp_dataset.csv")
DEFAULT_MODEL = Path("models/v2/neural_teamcomp.pt")


@dataclass
class ModelConfig:
    embedding_dim: int
    hidden_dim: int
    dropout: float
    use_badge: bool
    num_heroes: int


class TeamCompDataset(Dataset[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]):
    def __init__(
        self,
        rows: list[dict[str, str]],
        hero_to_index: dict[str, int],
        *,
        use_badge: bool,
        augment_swap: bool,
    ) -> None:
        self.samples: list[tuple[list[int], list[int], list[float], float]] = []
        for row in rows:
            team_1 = [hero_to_index[row[f"team_1_hero_{index}"]] for index in range(1, 7)]
            team_2 = [hero_to_index[row[f"team_2_hero_{index}"]] for index in range(1, 7)]
            badge = badge_features(row, use_badge)
            target = 1.0 if row["winner_team_index"] == "1" else 0.0
            self.samples.append((team_1, team_2, badge, target))
            if augment_swap:
                swapped_badge = [-value for value in badge]
                self.samples.append((team_2, team_1, swapped_badge, 1.0 - target))

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        team_1, team_2, badge, target = self.samples[index]
        return (
            torch.tensor(team_1, dtype=torch.long),
            torch.tensor(team_2, dtype=torch.long),
            torch.tensor(badge, dtype=torch.float32),
            torch.tensor([target], dtype=torch.float32),
        )


class TeamCompNet(nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.hero_embedding = nn.Embedding(config.num_heroes, config.embedding_dim)
        input_dim = config.embedding_dim * 5 + (1 if config.use_badge else 0)
        self.network = nn.Sequential(
            nn.Linear(input_dim, config.hidden_dim),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_dim, config.hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_dim // 2, 1),
        )

    def forward(
        self,
        team_1: torch.Tensor,
        team_2: torch.Tensor,
        badge: torch.Tensor,
    ) -> torch.Tensor:
        team_1_embeddings = self.hero_embedding(team_1)
        team_2_embeddings = self.hero_embedding(team_2)
        team_1_mean = team_1_embeddings.mean(dim=1)
        team_2_mean = team_2_embeddings.mean(dim=1)
        features = [
            team_1_mean,
            team_2_mean,
            team_2_mean - team_1_mean,
            torch.abs(team_2_mean - team_1_mean),
            team_1_mean * team_2_mean,
        ]
        if badge.shape[1] > 0:
            features.append(badge)
        return self.network(torch.cat(features, dim=1))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a PyTorch neural network for Deadlock team comps.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--model-output", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--epochs", type=int, default=35)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--learning-rate", type=float, default=0.001)
    parser.add_argument("--weight-decay", type=float, default=0.0001)
    parser.add_argument("--embedding-dim", type=int, default=16)
    parser.add_argument("--hidden-dim", type=int, default=128)
    parser.add_argument("--dropout", type=float, default=0.15)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--split", choices=("time", "random"), default="time")
    parser.add_argument("--use-badge", action="store_true")
    parser.add_argument("--no-swap-augmentation", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    set_seed(args.seed)
    rows = load_rows(args.dataset)
    if len(rows) < 100:
        raise SystemExit(f"Need at least 100 usable rows, got {len(rows)}")

    train_rows, test_rows = split_rows(rows, args.test_fraction, args.split, args.seed)
    hero_to_index = build_hero_vocab(rows)
    train_dataset = TeamCompDataset(
        train_rows,
        hero_to_index,
        use_badge=args.use_badge,
        augment_swap=not args.no_swap_augmentation,
    )
    test_dataset = TeamCompDataset(
        test_rows,
        hero_to_index,
        use_badge=args.use_badge,
        augment_swap=False,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    config = ModelConfig(
        embedding_dim=args.embedding_dim,
        hidden_dim=args.hidden_dim,
        dropout=args.dropout,
        use_badge=args.use_badge,
        num_heroes=len(hero_to_index),
    )
    model = TeamCompNet(config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=args.learning_rate,
        weight_decay=args.weight_decay,
    )
    criterion = nn.BCEWithLogitsLoss()

    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)
    history: list[dict[str, float]] = []
    best_state: dict[str, torch.Tensor] | None = None
    best_log_loss = float("inf")

    for epoch in range(1, args.epochs + 1):
        model.train()
        train_loss = 0.0
        for team_1, team_2, badge, target in train_loader:
            team_1 = team_1.to(device)
            team_2 = team_2.to(device)
            badge = badge.to(device)
            target = target.to(device)

            optimizer.zero_grad()
            logits = model(team_1, team_2, badge)
            loss = criterion(logits, target)
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * target.size(0)

        train_metrics = evaluate(model, train_loader, device)
        test_metrics = evaluate(model, test_loader, device)
        epoch_summary = {
            "epoch": float(epoch),
            "train_loss": train_loss / len(train_dataset),
            "train_accuracy": train_metrics["accuracy"],
            "train_log_loss": train_metrics["log_loss"],
            "test_accuracy": test_metrics["accuracy"],
            "test_log_loss": test_metrics["log_loss"],
        }
        history.append(epoch_summary)
        print(
            f"epoch={epoch:03d} "
            f"train_acc={train_metrics['accuracy']:.4f} "
            f"train_log_loss={train_metrics['log_loss']:.4f} "
            f"test_acc={test_metrics['accuracy']:.4f} "
            f"test_log_loss={test_metrics['log_loss']:.4f}"
        )

        if test_metrics["log_loss"] < best_log_loss:
            best_log_loss = test_metrics["log_loss"]
            best_state = {name: value.detach().cpu().clone() for name, value in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)

    final_train_metrics = evaluate(model, train_loader, device)
    final_test_metrics = evaluate(model, test_loader, device)
    args.model_output.parent.mkdir(parents=True, exist_ok=True)
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "config": asdict(config),
            "hero_to_index": hero_to_index,
            "metrics": {"train": final_train_metrics, "test": final_test_metrics},
            "history": history,
            "training_args": vars(args),
        },
        args.model_output,
    )
    metadata_path = args.model_output.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(
            {
                "config": asdict(config),
                "metrics": {"train": final_train_metrics, "test": final_test_metrics},
                "rows": {
                    "total": len(rows),
                    "train": len(train_rows),
                    "test": len(test_rows),
                    "train_samples_after_augmentation": len(train_dataset),
                },
                "hero_to_index": hero_to_index,
                "training_args": stringify_paths(vars(args)),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(f"Device: {device}")
    print(f"Final test accuracy={final_test_metrics['accuracy']:.4f} log_loss={final_test_metrics['log_loss']:.4f}")
    print(f"Wrote model to {args.model_output}")
    print(f"Wrote metadata to {metadata_path}")
    return 0


def load_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as file:
        for row in csv.DictReader(file):
            if row.get("winner_team_index") not in {"0", "1"}:
                continue
            if not all(row.get(f"team_1_hero_{index}") for index in range(1, 7)):
                continue
            if not all(row.get(f"team_2_hero_{index}") for index in range(1, 7)):
                continue
            rows.append(row)
    return rows


def split_rows(
    rows: list[dict[str, str]],
    test_fraction: float,
    split: str,
    seed: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows = list(rows)
    if split == "time":
        rows.sort(key=lambda row: int_or_zero(row.get("start_time_s")))
    else:
        random.Random(seed).shuffle(rows)
    test_size = max(1, min(len(rows) - 1, round(len(rows) * test_fraction)))
    return rows[:-test_size], rows[-test_size:]


def build_hero_vocab(rows: list[dict[str, str]]) -> dict[str, int]:
    hero_ids = sorted(
        {
            row[f"team_{team}_hero_{index}"]
            for row in rows
            for team in (1, 2)
            for index in range(1, 7)
        },
        key=int,
    )
    return {hero_id: index for index, hero_id in enumerate(hero_ids)}


def badge_features(row: dict[str, str], use_badge: bool) -> list[float]:
    if not use_badge:
        return []
    team_1_badge = int_or_none(row.get("team_1_average_badge"))
    team_2_badge = int_or_none(row.get("team_2_average_badge"))
    if team_1_badge is None or team_2_badge is None:
        return [0.0]
    return [(team_2_badge - team_1_badge) / 100.0]


@torch.no_grad()
def evaluate(model: nn.Module, loader: DataLoader, device: torch.device) -> dict[str, float]:
    model.eval()
    correct = 0
    total = 0
    total_loss = 0.0
    positives = 0
    probability_sum = 0.0
    criterion = nn.BCEWithLogitsLoss(reduction="sum")

    for team_1, team_2, badge, target in loader:
        team_1 = team_1.to(device)
        team_2 = team_2.to(device)
        badge = badge.to(device)
        target = target.to(device)
        logits = model(team_1, team_2, badge)
        probabilities = torch.sigmoid(logits)
        predictions = (probabilities >= 0.5).float()
        correct += (predictions == target).sum().item()
        total += target.numel()
        positives += target.sum().item()
        probability_sum += probabilities.sum().item()
        total_loss += criterion(logits, target).item()

    return {
        "accuracy": correct / total,
        "log_loss": total_loss / total,
        "positive_rate": positives / total,
        "avg_predicted_team2_win_probability": probability_sum / total,
    }


def int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def int_or_zero(value: Any) -> int:
    return int_or_none(value) or 0


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def stringify_paths(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: str(value) if isinstance(value, Path) else value for key, value in payload.items()}


if __name__ == "__main__":
    raise SystemExit(main())
