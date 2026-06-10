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

from train_neural_teamcomp import ModelConfig, TeamCompNet, pp_score_features  # noqa: E402


MODEL_PATH = REPO_ROOT / "models" / "2026-05-22" / "neural_teamcomp_heroes_ppscore_context.pt"
METADATA_PATH = MODEL_PATH.with_suffix(".json")
HERO_IMAGE_MANIFEST_PATH = REPO_ROOT / "app" / "assets" / "hero_images.json"
DEFAULT_PP_SCORE = 3200
MIN_PP_SCORE = 0

RANK_TIERS = [
    "Initiate",
    "Seeker",
    "Alchemist",
    "Arcanist",
    "Ritualist",
    "Emissary",
    "Archon",
    "Oracle",
    "Phantom",
    "Ascendant",
]

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


@st.cache_data
def load_hero_images() -> dict[str, Path]:
    if not HERO_IMAGE_MANIFEST_PATH.exists():
        return {}
    payload = json.loads(HERO_IMAGE_MANIFEST_PATH.read_text(encoding="utf-8"))
    images: dict[str, Path] = {}
    for hero_id, metadata in payload.items():
        image_path = REPO_ROOT / "app" / "assets" / str(metadata.get("image", ""))
        if image_path.exists():
            images[str(hero_id)] = image_path
    return images


def trained_patch_label(metadata: dict) -> str:
    dataset = metadata.get("training_args", {}).get("dataset", "")
    filename = Path(dataset).stem
    prefix = "team_comp_dataset_"
    return filename[len(prefix) :] if filename.startswith(prefix) else "unknown"


def hero_label(hero_id: str) -> str:
    return f"{HERO_NAMES.get(hero_id, 'Hero ' + hero_id)} ({hero_id})"


def render_hero_portrait(hero_id: str, images: dict[str, Path]) -> None:
    image_path = images.get(hero_id)
    hero_name = HERO_NAMES.get(hero_id, f"Hero {hero_id}")
    if image_path:
        st.image(str(image_path), caption=hero_name, width="stretch")
        return

    st.markdown(
        f"""
        <div class="hero-fallback">
          <div class="hero-fallback-id">{hero_id}</div>
          <div class="hero-fallback-name">{hero_name}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def pp_score_rank_label(pp_score: int) -> str:
    if pp_score >= 6600:
        return "Eternus 6+"
    if pp_score >= 6000:
        return f"Eternus {(pp_score - 6000) // 100 + 1}"

    tier_index = min(pp_score // 600, len(RANK_TIERS) - 1)
    sub_rank = (pp_score % 600) // 100 + 1
    return f"{RANK_TIERS[tier_index]} {sub_rank}"


def formatted_pp_score(pp_score: int) -> str:
    return f"{pp_score} PP ({pp_score_rank_label(pp_score)})"


def predict_team_2_probability(
    model: TeamCompNet,
    hero_to_index: dict[str, int],
    team_1: list[str],
    team_2: list[str],
    team_1_scores: list[int],
    team_2_scores: list[int],
) -> float:
    team_1_tensor = torch.tensor([[hero_to_index[hero_id] for hero_id in team_1]], dtype=torch.long)
    team_2_tensor = torch.tensor([[hero_to_index[hero_id] for hero_id in team_2]], dtype=torch.long)
    score_row = {
        **{f"team_1_pp_score_{index}": str(score) for index, score in enumerate(team_1_scores, start=1)},
        **{f"team_2_pp_score_{index}": str(score) for index, score in enumerate(team_2_scores, start=1)},
    }
    extra_tensor = torch.tensor([pp_score_features(score_row)], dtype=torch.float32)

    with torch.no_grad():
        logits = model(team_1_tensor, team_2_tensor, extra_tensor)
        return float(torch.sigmoid(logits).item())


def lineup_picker(
    title: str,
    options: list[str],
    defaults: list[str],
    key_prefix: str,
    *,
    score_mode: str,
    team_score: int | None,
    hero_images: dict[str, Path],
) -> tuple[list[str], list[int]]:
    st.subheader(title)
    picks: list[str] = []
    scores: list[int] = []
    columns = st.columns(6)
    for index in range(6):
        with columns[index]:
            current_pick = st.session_state.get(f"{key_prefix}_{index}", defaults[index])
            render_hero_portrait(str(current_pick), hero_images)
            pick = st.selectbox(
                f"Slot {index + 1}",
                options,
                index=options.index(current_pick),
                format_func=hero_label,
                key=f"{key_prefix}_{index}",
            )
            picks.append(pick)
            if score_mode == "Per player":
                score = st.number_input(
                    "ppScore",
                    min_value=MIN_PP_SCORE,
                    value=int(st.session_state.get(f"{key_prefix}_score_{index}", DEFAULT_PP_SCORE)),
                    step=50,
                    key=f"{key_prefix}_score_{index}",
                )
                score = int(score)
                st.caption(pp_score_rank_label(score))
                scores.append(score)
            else:
                scores.append(int(team_score or DEFAULT_PP_SCORE))
    return picks, scores


def main() -> None:
    st.set_page_config(page_title="Deadlock Team Comp Analyzer", page_icon="DL", layout="wide")
    st.markdown(
        """
        <style>
        .hero-fallback {
            align-items: center;
            aspect-ratio: 16 / 9;
            background: #20232d;
            border: 1px solid #3b4050;
            border-radius: 6px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            margin-bottom: 0.35rem;
            min-height: 92px;
        }
        .hero-fallback-id {
            color: #f2f2f2;
            font-size: 1.35rem;
            font-weight: 700;
            line-height: 1;
        }
        .hero-fallback-name {
            color: #aeb4c4;
            font-size: 0.8rem;
            margin-top: 0.35rem;
            text-align: center;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    model, hero_to_index, metadata = load_model()
    hero_images = load_hero_images()
    options = sorted(hero_to_index, key=lambda value: HERO_NAMES.get(value, value))

    st.title("Deadlock Team Comp Analyzer")
    patch = trained_patch_label(metadata)
    st.caption(f"Rank-aware PyTorch model trained on normal matches from patch {patch}, using hero picks and Statlocker ppScore.")

    with st.sidebar:
        st.header("Model")
        metrics = metadata["metrics"]["test"]
        config = metadata["config"]
        st.metric("Test accuracy", f"{metrics['accuracy'] * 100:.2f}%")
        st.metric("Test log loss", f"{metrics['log_loss']:.4f}")
        st.write("Architecture:", config["architecture"])
        st.write("Activation:", config.get("activation", "relu").upper())
        st.write("Embedding dim:", config["embedding_dim"])
        st.write("Hidden dim:", config["hidden_dim"])
        st.write("Uses badge:", config["use_badge"])
        st.write("Uses ppScore:", config["use_pp_score"])

    default_team_1 = ["13", "2", "31", "15", "63", "7"]
    default_team_2 = ["11", "52", "65", "76", "1", "77"]

    controls_left, controls_right = st.columns([1, 3])
    with controls_left:
        if st.button("Randomize", use_container_width=True):
            randomized = random.sample(options, 12)
            for index, hero_id in enumerate(randomized[:6]):
                st.session_state[f"team_1_{index}"] = hero_id
            for index, hero_id in enumerate(randomized[6:]):
                st.session_state[f"team_2_{index}"] = hero_id
            st.rerun()
    with controls_right:
        score_mode = st.segmented_control(
            "ppScore input",
            ["Team average", "Per player"],
            default="Team average",
        )

    team_1_score: int | None = None
    team_2_score: int | None = None
    if score_mode == "Team average":
        with st.container():
            team_1_score = int(
                st.number_input(
                    "Team 1 average ppScore",
                    min_value=MIN_PP_SCORE,
                    value=DEFAULT_PP_SCORE,
                    step=50,
                    key="team_1_global_score",
                )
            )
            st.caption(pp_score_rank_label(team_1_score))
        with st.container():
            team_2_score = int(
                st.number_input(
                    "Team 2 average ppScore",
                    min_value=MIN_PP_SCORE,
                    value=DEFAULT_PP_SCORE,
                    step=50,
                    key="team_2_global_score",
                )
            )
            st.caption(pp_score_rank_label(team_2_score))

    team_1, team_1_scores = lineup_picker(
        "Team 1",
        options,
        default_team_1,
        "team_1",
        score_mode=score_mode,
        team_score=team_1_score,
        hero_images=hero_images,
    )
    st.divider()
    team_2, team_2_scores = lineup_picker(
        "Team 2",
        options,
        default_team_2,
        "team_2",
        score_mode=score_mode,
        team_score=team_2_score,
        hero_images=hero_images,
    )

    all_picks = team_1 + team_2
    duplicate_ids = sorted({hero_id for hero_id in all_picks if all_picks.count(hero_id) > 1}, key=int)
    if duplicate_ids:
        st.warning("Duplicate heroes selected: " + ", ".join(hero_label(hero_id) for hero_id in duplicate_ids))

    team_2_probability = predict_team_2_probability(
        model,
        hero_to_index,
        team_1,
        team_2,
        team_1_scores,
        team_2_scores,
    )
    team_1_probability = 1.0 - team_2_probability

    st.divider()
    result_left, result_mid, result_right = st.columns([1, 1, 1])
    result_left.metric("Team 1 win chance", f"{team_1_probability * 100:.2f}%")
    result_mid.metric("Team 2 win chance", f"{team_2_probability * 100:.2f}%")
    result_right.progress(team_2_probability, text="Team 2 probability")

    if team_1_probability > team_2_probability:
        st.success("Model lean: Team 1")
    elif team_2_probability > team_1_probability:
        st.success("Model lean: Team 2")
    else:
        st.info("Model lean: even")

    with st.expander("Selected lineups"):
        st.write("Team 1:", ", ".join(hero_label(hero_id) for hero_id in team_1))
        st.write("Team 1 ppScore:", ", ".join(formatted_pp_score(score) for score in team_1_scores))
        st.write("Team 2:", ", ".join(hero_label(hero_id) for hero_id in team_2))
        st.write("Team 2 ppScore:", ", ".join(formatted_pp_score(score) for score in team_2_scores))


if __name__ == "__main__":
    main()
