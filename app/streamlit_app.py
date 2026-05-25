from __future__ import annotations

import json
import random
import sys
from pathlib import Path

import streamlit as st
import torch


REPO_ROOT = Path(__file__).resolve().parents[1]
TRAINING_DIR = REPO_ROOT / "scripts"
if str(TRAINING_DIR) not in sys.path:
    sys.path.insert(0, str(TRAINING_DIR))

from train_neural_teamcomp import ModelConfig, TeamCompNet  # noqa: E402


MODEL_PATH = REPO_ROOT / "models" / "neural_teamcomp_heroes_only.pt"
METADATA_PATH = MODEL_PATH.with_suffix(".json")

HERO_NAMES = {
    "1": "Infernus",
    "2": "Seven",
    "3": "Vindicta",
    "4": "Lady Geist",
    "6": "Abrams",
    "7": "Wraith",
    "8": "McGinnis",
    "10": "Paradox",
    "11": "Dynamo",
    "12": "Kelvin",
    "13": "Haze",
    "14": "Holliday",
    "15": "Bebop",
    "16": "Calico",
    "17": "Grey Talon",
    "18": "Mo & Krill",
    "19": "Shiv",
    "20": "Ivy",
    "25": "Warden",
    "27": "Yamato",
    "31": "Lash",
    "35": "Viscous",
    "50": "Pocket",
    "52": "Mirage",
    "58": "Vyper",
    "60": "Sinclair",
    "63": "Mina",
    "64": "Drifter",
    "65": "Venator",
    "66": "Victor",
    "67": "Paige",
    "69": "The Doorman",
    "72": "Billy",
    "76": "Graves",
    "77": "Apollo",
    "79": "Rem",
    "80": "Silver",
    "81": "Celeste",
}


@st.cache_resource
def load_model() -> tuple[TeamCompNet, dict[str, int], dict]:
    checkpoint = torch.load(MODEL_PATH, map_location="cpu", weights_only=False)
    config = ModelConfig(**checkpoint["config"])
    model = TeamCompNet(config)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    return model, checkpoint["hero_to_index"], metadata


def hero_label(hero_id: str) -> str:
    return f"{HERO_NAMES.get(hero_id, 'Hero ' + hero_id)} ({hero_id})"


def predict_team_2_probability(
    model: TeamCompNet,
    hero_to_index: dict[str, int],
    team_1: list[str],
    team_2: list[str],
) -> float:
    team_1_tensor = torch.tensor([[hero_to_index[hero_id] for hero_id in team_1]], dtype=torch.long)
    team_2_tensor = torch.tensor([[hero_to_index[hero_id] for hero_id in team_2]], dtype=torch.long)
    extra_tensor = torch.empty((1, 0), dtype=torch.float32)

    with torch.no_grad():
        logits = model(team_1_tensor, team_2_tensor, extra_tensor)
        return float(torch.sigmoid(logits).item())


def lineup_picker(title: str, options: list[str], defaults: list[str], key_prefix: str) -> list[str]:
    st.subheader(title)
    picks: list[str] = []
    columns = st.columns(3)
    for index in range(6):
        with columns[index % 3]:
            pick = st.selectbox(
                f"Slot {index + 1}",
                options,
                index=options.index(st.session_state.get(f"{key_prefix}_{index}", defaults[index])),
                format_func=hero_label,
                key=f"{key_prefix}_{index}",
            )
            picks.append(pick)
    return picks


def main() -> None:
    st.set_page_config(page_title="Deadlock Team Comp Analyzer", page_icon="DL", layout="wide")
    model, hero_to_index, metadata = load_model()
    options = sorted(hero_to_index, key=lambda value: HERO_NAMES.get(value, value))

    st.title("Deadlock Team Comp Analyzer")
    st.caption("PyTorch model trained on normal matches from the 2026-04-30 patch. Current model uses hero picks only.")

    with st.sidebar:
        st.header("Model")
        metrics = metadata["metrics"]["test"]
        config = metadata["config"]
        st.metric("Test accuracy", f"{metrics['accuracy'] * 100:.2f}%")
        st.metric("Test log loss", f"{metrics['log_loss']:.4f}")
        st.write("Architecture:", config["architecture"])
        st.write("Embedding dim:", config["embedding_dim"])
        st.write("Hidden dim:", config["hidden_dim"])
        st.write("Uses badge:", config["use_badge"])
        st.write("Uses ppScore:", config["use_pp_score"])

    default_team_1 = ["13", "2", "31", "15", "63", "7"]
    default_team_2 = ["11", "52", "65", "76", "1", "77"]

    controls_left, controls_right = st.columns([1, 5])
    with controls_left:
        if st.button("Randomize", use_container_width=True):
            randomized = random.sample(options, 12)
            for index, hero_id in enumerate(randomized[:6]):
                st.session_state[f"team_1_{index}"] = hero_id
            for index, hero_id in enumerate(randomized[6:]):
                st.session_state[f"team_2_{index}"] = hero_id
            st.rerun()
    with controls_right:
        st.caption("Randomize fills both teams with 12 unique heroes from the model vocabulary.")

    left, right = st.columns(2)
    with left:
        team_1 = lineup_picker("Team 1", options, default_team_1, "team_1")
    with right:
        team_2 = lineup_picker("Team 2", options, default_team_2, "team_2")

    all_picks = team_1 + team_2
    duplicate_ids = sorted({hero_id for hero_id in all_picks if all_picks.count(hero_id) > 1}, key=int)
    if duplicate_ids:
        st.warning("Duplicate heroes selected: " + ", ".join(hero_label(hero_id) for hero_id in duplicate_ids))

    team_2_probability = predict_team_2_probability(model, hero_to_index, team_1, team_2)
    team_1_probability = 1.0 - team_2_probability

    st.divider()
    result_left, result_mid, result_right = st.columns([1, 1, 1])
    result_left.metric("Team 1 win chance", f"{team_1_probability * 100:.1f}%")
    result_mid.metric("Team 2 win chance", f"{team_2_probability * 100:.1f}%")
    result_right.progress(team_2_probability, text="Team 2 probability")

    if team_1_probability > team_2_probability:
        st.success("Model lean: Team 1")
    elif team_2_probability > team_1_probability:
        st.success("Model lean: Team 2")
    else:
        st.info("Model lean: even")

    with st.expander("Selected lineups"):
        st.write("Team 1:", ", ".join(hero_label(hero_id) for hero_id in team_1))
        st.write("Team 2:", ", ".join(hero_label(hero_id) for hero_id in team_2))

    st.caption("This is a team-composition prototype. It does not yet use individual player rank or Statlocker ppScore.")


if __name__ == "__main__":
    main()
