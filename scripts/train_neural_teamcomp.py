from __future__ import annotations  # Maakt moderne type hints mogelijk.

import argparse  # Command-line opties parsen.
import csv  # Dataset CSV lezen.
import json  # Modelmetadata schrijven.
import math  # Log loss veilig berekenen.
import random  # Random split en seeding.
from dataclasses import asdict, dataclass  # Config als simpel data-object bewaren.
from pathlib import Path  # Bestandspaden voor dataset/model.
from typing import Any  # Metadata en CSV-waarden kunnen gemengd zijn.

import numpy as np  # NumPy random seed zetten.
import torch  # PyTorch tensors, training en model opslaan.
from torch import nn  # Neural-network lagen en losses.
from torch.utils.data import DataLoader, Dataset  # Batching en dataset-interface.


DEFAULT_DATASET = Path("data/team_comp_dataset_2026-05-22.csv")
DEFAULT_MODEL = Path("models/2026-05-22/neural_teamcomp_heroes_only.pt")


@dataclass
class ModelConfig:
    """Instellingen die nodig zijn om hetzelfde model later te laden."""
    embedding_dim: int
    hidden_dim: int
    dropout: float
    architecture: str
    use_badge: bool
    use_pp_score: bool
    extra_feature_dim: int
    num_heroes: int
    activation: str = "relu"


class TeamCompDataset(Dataset[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]):
    """PyTorch dataset die CSV-rijen omzet naar tensors voor training."""
    def __init__(
        self,
        rows: list[dict[str, str]],
        hero_to_index: dict[str, int],
        *,
        use_badge: bool,
        use_pp_score: bool,
        augment_swap: bool,
    ) -> None:
        """Bouw samples met hero indices, extra features en target winnaar."""
        self.samples: list[tuple[list[int], list[int], list[float], float]] = []
        for row in rows:
            team_1 = [hero_to_index[row[f"team_1_hero_{index}"]] for index in range(1, 7)]
            team_2 = [hero_to_index[row[f"team_2_hero_{index}"]] for index in range(1, 7)]
            extra = extra_features(row, use_badge=use_badge, use_pp_score=use_pp_score)
            target = 1.0 if row["winner_team_index"] == "1" else 0.0
            self.samples.append((team_1, team_2, extra, target))
            if augment_swap:
                swapped_extra = swap_extra_features(
                    extra,
                    use_badge=use_badge,
                    use_pp_score=use_pp_score,
                )
                self.samples.append((team_2, team_1, swapped_extra, 1.0 - target))

    def __len__(self) -> int:
        """Geef het aantal trainingssamples."""
        return len(self.samples)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """Geef één sample terug als PyTorch tensors."""
        team_1, team_2, extra, target = self.samples[index]
        return (
            torch.tensor(team_1, dtype=torch.long),
            torch.tensor(team_2, dtype=torch.long),
            torch.tensor(extra, dtype=torch.float32),
            torch.tensor([target], dtype=torch.float32),
        )


class TeamCompNet(nn.Module):
    """Neuraal netwerk dat team 2 win-kans voorspelt uit heroes en context."""
    def __init__(self, config: ModelConfig) -> None:
        """Maak embedding-laag en feed-forward netwerk."""
        super().__init__()
        self.config = config
        self.hero_embedding = nn.Embedding(config.num_heroes, config.embedding_dim)
        input_dim = self._encoded_dim(config) + config.extra_feature_dim
        self.network = nn.Sequential(
            nn.Linear(input_dim, config.hidden_dim),
            activation_layer(config.activation),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_dim, config.hidden_dim // 2),
            activation_layer(config.activation),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_dim // 2, 1),
        )

    def forward(
        self,
        team_1: torch.Tensor,
        team_2: torch.Tensor,
        extra: torch.Tensor,
    ) -> torch.Tensor:
        """Bereken logits voor team 2 winst."""
        team_1_embeddings = self.hero_embedding(team_1)
        team_2_embeddings = self.hero_embedding(team_2)
        team_1_mean = team_1_embeddings.mean(dim=1)
        team_2_mean = team_2_embeddings.mean(dim=1)

        features = self._encode_teams(
            team_1_embeddings,
            team_2_embeddings,
            team_1_mean,
            team_2_mean,
        )
        if extra.shape[1] > 0:
            features.append(extra)
        return self.network(torch.cat(features, dim=1))

    def _encode_teams(
        self,
        team_1_embeddings: torch.Tensor,
        team_2_embeddings: torch.Tensor,
        team_1_mean: torch.Tensor,
        team_2_mean: torch.Tensor,
    ) -> list[torch.Tensor]:
        """Maak feature-vectoren uit team embeddings."""
        features = [
            team_1_mean,
            team_2_mean,
            team_2_mean - team_1_mean,
            torch.abs(team_2_mean - team_1_mean),
            team_1_mean * team_2_mean,
        ]

        if self.config.architecture in {"pool", "matchup"}:
            team_1_max = team_1_embeddings.max(dim=1).values
            team_2_max = team_2_embeddings.max(dim=1).values
            features.extend(
                [
                    team_1_max,
                    team_2_max,
                    team_2_max - team_1_max,
                    torch.abs(team_2_max - team_1_max),
                ]
            )

        if self.config.architecture == "matchup":
            features.append(pairwise_summary(team_1_embeddings, team_2_embeddings))

        return features

    @staticmethod
    def _encoded_dim(config: ModelConfig) -> int:
        """Bereken hoeveel embedding-features de gekozen architectuur oplevert."""
        if config.architecture == "mean":
            return config.embedding_dim * 5
        if config.architecture == "pool":
            return config.embedding_dim * 9
        if config.architecture == "matchup":
            return config.embedding_dim * 9 + 8
        raise ValueError(f"Unknown architecture: {config.architecture}")


def parse_args() -> argparse.Namespace:
    """Lees trainingsinstellingen uit de command line."""
    parser = argparse.ArgumentParser(description="Train a PyTorch neural network for Deadlock team comps.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)  # CSV dataset om te trainen.
    parser.add_argument("--model-output", type=Path, default=DEFAULT_MODEL)  # .pt modelbestand dat geschreven wordt.
    parser.add_argument("--epochs", type=int, default=35)  # Maximaal aantal training epochs.
    parser.add_argument("--batch-size", type=int, default=256)  # Aantal samples per optimizer stap.
    parser.add_argument("--learning-rate", type=float, default=0.001)  # AdamW learning rate.
    parser.add_argument("--weight-decay", type=float, default=0.0001)  # L2 regularisatie via AdamW.
    parser.add_argument("--l1-lambda", type=float, default=0.0)  # Extra L1 regularisatie.
    parser.add_argument("--grad-clip", type=float, default=1.0)  # Maximale gradient norm.
    parser.add_argument("--embedding-dim", type=int, default=16)  # Grootte van hero embeddings.
    parser.add_argument("--hidden-dim", type=int, default=128)  # Grootte van hidden layer.
    parser.add_argument("--dropout", type=float, default=0.15)  # Dropout tegen overfitting.
    parser.add_argument("--architecture", choices=("mean", "pool", "matchup"), default="mean")  # Welke team encoding gebruikt wordt.
    parser.add_argument("--activation", choices=("relu", "gelu", "silu"), default="relu")  # Activatiefunctie tussen layers.
    parser.add_argument("--test-fraction", type=float, default=0.2)  # Deel van data voor testset.
    parser.add_argument("--validation-fraction", type=float, default=0.15)  # Deel van train-rest voor validatie.
    parser.add_argument("--patience", type=int, default=8)  # Early stopping na zoveel slechte epochs.
    parser.add_argument("--min-delta", type=float, default=0.0001)  # Minimale validatieverbetering.
    parser.add_argument("--split", choices=("time", "random"), default="time")  # Tijdgebaseerde of random split.
    parser.add_argument("--use-badge", action="store_true")  # Voeg average badge features toe.
    parser.add_argument("--use-pp-score", action="store_true")  # Voeg ppScore features toe.
    parser.add_argument("--no-swap-augmentation", action="store_true")  # Zet team-swap augmentatie uit.
    parser.add_argument("--seed", type=int, default=42)  # Seed voor reproduceerbaarheid.
    return parser.parse_args()


def main() -> int:
    """Train model, evalueer splits en schrijf model plus metadata."""
    args = parse_args()
    set_seed(args.seed)
    rows = load_rows(args.dataset)
    if len(rows) < 100:
        raise SystemExit(f"Need at least 100 usable rows, got {len(rows)}")

    train_rows, validation_rows, test_rows = split_rows(
        rows,
        test_fraction=args.test_fraction,
        validation_fraction=args.validation_fraction,
        split=args.split,
        seed=args.seed,
    )
    hero_to_index = build_hero_vocab(rows)
    train_dataset = TeamCompDataset(
        train_rows,
        hero_to_index,
        use_badge=args.use_badge,
        use_pp_score=args.use_pp_score,
        augment_swap=not args.no_swap_augmentation,
    )
    test_dataset = TeamCompDataset(
        test_rows,
        hero_to_index,
        use_badge=args.use_badge,
        use_pp_score=args.use_pp_score,
        augment_swap=False,
    )
    validation_dataset = TeamCompDataset(
        validation_rows,
        hero_to_index,
        use_badge=args.use_badge,
        use_pp_score=args.use_pp_score,
        augment_swap=False,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    config = ModelConfig(
        embedding_dim=args.embedding_dim,
        hidden_dim=args.hidden_dim,
        dropout=args.dropout,
        architecture=args.architecture,
        use_badge=args.use_badge,
        use_pp_score=args.use_pp_score,
        extra_feature_dim=extra_feature_dim(args.use_badge, args.use_pp_score),
        num_heroes=len(hero_to_index),
        activation=args.activation,
    )
    model = TeamCompNet(config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=args.learning_rate,
        weight_decay=args.weight_decay,
    )
    criterion = nn.BCEWithLogitsLoss()

    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    validation_loader = DataLoader(validation_dataset, batch_size=args.batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)
    history: list[dict[str, float]] = []
    best_state: dict[str, torch.Tensor] | None = None
    best_validation_log_loss = float("inf")
    best_epoch = 0
    epochs_without_improvement = 0

    for epoch in range(1, args.epochs + 1):
        model.train()
        train_loss = 0.0
        for team_1, team_2, extra, target in train_loader:
            team_1 = team_1.to(device)
            team_2 = team_2.to(device)
            extra = extra.to(device)
            target = target.to(device)

            optimizer.zero_grad()
            logits = model(team_1, team_2, extra)
            loss = criterion(logits, target)
            if args.l1_lambda > 0:
                loss = loss + args.l1_lambda * l1_penalty(model)
            loss.backward() # backward pass
            if args.grad_clip > 0:
                nn.utils.clip_grad_norm_(model.parameters(), args.grad_clip)
            optimizer.step() # weight update
            train_loss += loss.item() * target.size(0)

        train_metrics = evaluate(model, train_loader, device)
        validation_metrics = evaluate(model, validation_loader, device)
        test_metrics = evaluate(model, test_loader, device)
        epoch_summary = {
            "epoch": float(epoch),
            "train_loss": train_loss / len(train_dataset),
            "train_accuracy": train_metrics["accuracy"],
            "train_log_loss": train_metrics["log_loss"],
            "validation_accuracy": validation_metrics["accuracy"],
            "validation_log_loss": validation_metrics["log_loss"],
            "test_accuracy": test_metrics["accuracy"],
            "test_log_loss": test_metrics["log_loss"],
        }
        history.append(epoch_summary)
        print(
            f"epoch={epoch:03d} "
            f"train_acc={train_metrics['accuracy']:.4f} "
            f"train_log_loss={train_metrics['log_loss']:.4f} "
            f"val_acc={validation_metrics['accuracy']:.4f} "
            f"val_log_loss={validation_metrics['log_loss']:.4f} "
            f"test_acc={test_metrics['accuracy']:.4f} "
            f"test_log_loss={test_metrics['log_loss']:.4f}"
        )

        if validation_metrics["log_loss"] < best_validation_log_loss - args.min_delta:
            best_validation_log_loss = validation_metrics["log_loss"]
            best_epoch = epoch
            epochs_without_improvement = 0
            best_state = {name: value.detach().cpu().clone() for name, value in model.state_dict().items()}
        else:
            epochs_without_improvement += 1
            if epochs_without_improvement >= args.patience:
                print(f"Early stopping at epoch {epoch}; best validation epoch was {best_epoch}")
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    final_train_metrics = evaluate(model, train_loader, device)
    final_validation_metrics = evaluate(model, validation_loader, device)
    final_test_metrics = evaluate(model, test_loader, device)
    args.model_output.parent.mkdir(parents=True, exist_ok=True)
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "config": asdict(config),
            "hero_to_index": hero_to_index,
            "metrics": {
                "train": final_train_metrics,
                "validation": final_validation_metrics,
                "test": final_test_metrics,
            },
            "history": history,
            "training_args": vars(args),
            "best_epoch": best_epoch,
        },
        args.model_output,
    )
    metadata_path = args.model_output.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(
            {
                "config": asdict(config),
                "metrics": {
                    "train": final_train_metrics,
                    "validation": final_validation_metrics,
                    "test": final_test_metrics,
                },
                "rows": {
                    "total": len(rows),
                    "train": len(train_rows),
                    "validation": len(validation_rows),
                    "test": len(test_rows),
                    "train_samples_after_augmentation": len(train_dataset),
                },
                "hero_to_index": hero_to_index,
                "training_args": stringify_paths(vars(args)),
                "best_epoch": best_epoch,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(f"Device: {device}")
    print(f"Best validation epoch={best_epoch}")
    print(f"Final test accuracy={final_test_metrics['accuracy']:.4f} log_loss={final_test_metrics['log_loss']:.4f}")
    print(f"Wrote model to {args.model_output}")
    print(f"Wrote metadata to {metadata_path}")
    return 0


def load_rows(path: Path) -> list[dict[str, str]]:
    """Lees bruikbare CSV-rijen met winnaar en 12 hero picks."""
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
    *,
    test_fraction: float,
    validation_fraction: float,
    split: str,
    seed: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    """Splits rijen in train, validatie en test."""
    rows = list(rows)
    if split == "time":
        rows.sort(key=lambda row: int_or_zero(row.get("start_time_s")))
    else:
        random.Random(seed).shuffle(rows)
    test_size = max(1, min(len(rows) - 1, round(len(rows) * test_fraction)))
    remaining_rows = rows[:-test_size]
    validation_size = max(1, min(len(remaining_rows) - 1, round(len(remaining_rows) * validation_fraction)))
    return remaining_rows[:-validation_size], remaining_rows[-validation_size:], rows[-test_size:]


def build_hero_vocab(rows: list[dict[str, str]]) -> dict[str, int]:
    """Maak mapping hero_id -> embedding index."""
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


def extra_features(row: dict[str, str], *, use_badge: bool, use_pp_score: bool) -> list[float]:
    """Combineer optionele badge- en ppScore-features."""
    features: list[float] = []
    if use_badge:
        features.extend(badge_features(row))
    if use_pp_score:
        features.extend(pp_score_features(row))
    return features


def extra_feature_dim(use_badge: bool, use_pp_score: bool) -> int:
    """Bereken hoeveel extra numerieke features worden toegevoegd."""
    return (1 if use_badge else 0) + (6 if use_pp_score else 0)


def swap_extra_features(features: list[float], *, use_badge: bool, use_pp_score: bool) -> list[float]:
    """Draai team-gerichte extra features om voor swap augmentatie."""
    swapped: list[float] = []
    index = 0
    if use_badge:
        swapped.append(-features[index])
        index += 1
    if use_pp_score:
        pp_features = features[index : index + 6]
        swapped.extend(
            [
                -pp_features[0],
                -pp_features[1],
                -pp_features[2],
                -pp_features[3],
                pp_features[4],
                pp_features[5],
            ]
        )
    return swapped


def badge_features(row: dict[str, str]) -> list[float]:
    """Maak feature voor verschil in average badge tussen teams."""
    team_1_badge = int_or_none(row.get("team_1_average_badge"))
    team_2_badge = int_or_none(row.get("team_2_average_badge"))
    if team_1_badge is None or team_2_badge is None:
        return [0.0]
    return [(team_2_badge - team_1_badge) / 100.0]


def pp_score_features(row: dict[str, str]) -> list[float]:
    """Maak features voor ppScore verschil, spreiding en dekking."""
    team_1_scores = [
        value
        for index in range(1, 7)
        if (value := int_or_none(row.get(f"team_1_pp_score_{index}"))) is not None
    ]
    team_2_scores = [
        value
        for index in range(1, 7)
        if (value := int_or_none(row.get(f"team_2_pp_score_{index}"))) is not None
    ]
    observed_scores = team_1_scores + team_2_scores
    coverage = len(observed_scores) / 12.0
    lobby_mean = sum(observed_scores) / len(observed_scores) / 10000.0 if observed_scores else 0.0
    if not team_1_scores or not team_2_scores:
        return [0.0, 0.0, 0.0, 0.0, coverage, lobby_mean]

    team_1_mean = sum(team_1_scores) / len(team_1_scores)
    team_2_mean = sum(team_2_scores) / len(team_2_scores)
    return [
        (team_2_mean - team_1_mean) / 10000.0,
        (min(team_2_scores) - min(team_1_scores)) / 10000.0,
        (max(team_2_scores) - max(team_1_scores)) / 10000.0,
        (len(team_2_scores) - len(team_1_scores)) / 6.0,
        coverage,
        lobby_mean,
    ]


def pairwise_summary(team_1_embeddings: torch.Tensor, team_2_embeddings: torch.Tensor) -> torch.Tensor:
    """Vat hero-vs-hero matchups en team synergy samen."""
    matchup = torch.matmul(team_1_embeddings, team_2_embeddings.transpose(1, 2))
    team_1_synergy = torch.matmul(team_1_embeddings, team_1_embeddings.transpose(1, 2))
    team_2_synergy = torch.matmul(team_2_embeddings, team_2_embeddings.transpose(1, 2))

    return torch.cat(
        [
            matchup.mean(dim=(1, 2), keepdim=False).unsqueeze(1),
            matchup.std(dim=(1, 2), keepdim=False).unsqueeze(1),
            matchup.amax(dim=(1, 2), keepdim=False).unsqueeze(1),
            matchup.amin(dim=(1, 2), keepdim=False).unsqueeze(1),
            (team_2_synergy.mean(dim=(1, 2)) - team_1_synergy.mean(dim=(1, 2))).unsqueeze(1),
            (team_2_synergy.std(dim=(1, 2)) - team_1_synergy.std(dim=(1, 2))).unsqueeze(1),
            (team_2_synergy.amax(dim=(1, 2)) - team_1_synergy.amax(dim=(1, 2))).unsqueeze(1),
            (team_2_synergy.amin(dim=(1, 2)) - team_1_synergy.amin(dim=(1, 2))).unsqueeze(1),
        ],
        dim=1,
    )


def activation_layer(name: str) -> nn.Module:
    """Maak de gekozen activatielaag."""
    if name == "relu":
        return nn.ReLU()
    if name == "gelu":
        return nn.GELU()
    if name == "silu":
        return nn.SiLU()
    raise ValueError(f"Unknown activation: {name}")


def l1_penalty(model: nn.Module) -> torch.Tensor:
    """Bereken L1 penalty over trainbare non-bias parameters."""
    penalty = torch.zeros((), device=next(model.parameters()).device)
    for name, parameter in model.named_parameters():
        if parameter.requires_grad and "bias" not in name:
            penalty = penalty + parameter.abs().sum()
    return penalty


@torch.no_grad()
def evaluate(model: nn.Module, loader: DataLoader, device: torch.device) -> dict[str, float]:
    """Bereken accuracy en log loss zonder gradients."""
    model.eval()
    correct = 0
    total = 0
    total_loss = 0.0
    positives = 0
    probability_sum = 0.0
    criterion = nn.BCEWithLogitsLoss(reduction="sum")

    for team_1, team_2, extra, target in loader:
        team_1 = team_1.to(device)
        team_2 = team_2.to(device)
        extra = extra.to(device)
        target = target.to(device)
        logits = model(team_1, team_2, extra)
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
    """Parse int of geef None terug."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def int_or_zero(value: Any) -> int:
    """Parse int of gebruik 0 als fallback."""
    return int_or_none(value) or 0


def set_seed(seed: int) -> None:
    """Zet random seeds voor reproduceerbare runs."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def stringify_paths(payload: dict[str, Any]) -> dict[str, Any]:
    """Zet Path-objecten om naar tekst voor JSON metadata."""
    return {key: str(value) if isinstance(value, Path) else value for key, value in payload.items()}


if __name__ == "__main__":
    raise SystemExit(main())
