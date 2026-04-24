from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from dl_team_comp_analyzer.patch_history import infer_patch_from_start_time


HeroResolver = Callable[[int | str | None], str]

_PLAYER_LIST_KEYS = (
    "players",
    "match_players",
    "player_slots",
    "participants",
    "members",
)

_PATCH_KEYS = (
    "patch",
    "patch_name",
    "version",
    "client_version",
    "build_id",
)

_WINNER_KEYS = (
    "winner",
    "winner_team",
    "winning_team",
    "winning_side",
    "victorious_team",
)


@dataclass
class PlayerView:
    player_name: str
    account_id: str
    hero_id: str
    hero_name: str
    rank: str
    team_key: str
    player_slot: int | None


@dataclass
class MatchView:
    match_id: str
    start_time_s: int | None
    start_time_utc: str
    patch: str
    patch_source: str
    winner: str
    winner_team_index: int | None
    team_1_label: str
    team_2_label: str
    team_1_average_badge: str
    team_2_average_badge: str
    team_1_players: list[PlayerView]
    team_2_players: list[PlayerView]


def build_match_view(
    metadata: dict[str, Any], hero_resolver: HeroResolver | None = None
) -> MatchView:
    hero_lookup = hero_resolver or (lambda hero_id: f"Hero {hero_id}" if hero_id is not None else "Unknown Hero")
    root = _unwrap_match_payload(metadata)
    players = _find_players(root)
    if not players:
        raise ValueError(
            "No player list found in the match JSON. Save the raw response and inspect it first."
        )

    grouped: dict[str, list[PlayerView]] = {}
    ordered_team_keys: list[str] = []

    for index, raw_player in enumerate(players, start=1):
        team_key = _extract_team_key(raw_player, fallback=f"team_{1 if index <= len(players) / 2 else 2}")
        if team_key not in grouped:
            grouped[team_key] = []
            ordered_team_keys.append(team_key)

        hero_id = _extract_hero_id(raw_player)
        grouped[team_key].append(
            PlayerView(
                player_name=_extract_player_name(raw_player, index=index),
                account_id=_extract_account_id(raw_player, index=index),
                hero_id="" if hero_id is None else str(hero_id),
                hero_name=hero_lookup(hero_id),
                rank=_extract_rank(raw_player),
                team_key=team_key,
                player_slot=_extract_player_slot(raw_player),
            )
        )

    if len(ordered_team_keys) < 2:
        ordered_team_keys = list(grouped.keys())

    team_1_key = ordered_team_keys[0]
    team_2_key = ordered_team_keys[1] if len(ordered_team_keys) > 1 else "team_2"
    grouped[team_1_key] = _sorted_players(grouped.get(team_1_key, []))
    grouped[team_2_key] = _sorted_players(grouped.get(team_2_key, []))

    winner = _extract_winner(root)
    winner_label = _format_winner(winner, team_1_key, team_2_key)
    winner_team_index = _normalize_winner_index(winner)
    start_time_s = _extract_start_time(root)
    patch, patch_source = _resolve_patch(root, start_time_s)

    return MatchView(
        match_id=_extract_match_id(root),
        start_time_s=start_time_s,
        start_time_utc=_format_start_time_utc(start_time_s),
        patch=patch,
        patch_source=patch_source,
        winner=winner_label,
        winner_team_index=winner_team_index,
        team_1_label=_humanize_team_label(team_1_key, fallback="Team 1"),
        team_2_label=_humanize_team_label(team_2_key, fallback="Team 2"),
        team_1_average_badge=_extract_average_badge(root, team_1_key),
        team_2_average_badge=_extract_average_badge(root, team_2_key),
        team_1_players=grouped.get(team_1_key, []),
        team_2_players=grouped.get(team_2_key, []),
    )


def match_view_to_dict(match_view: MatchView) -> dict[str, Any]:
    return {
        "match_id": match_view.match_id,
        "start_time_s": match_view.start_time_s,
        "start_time_utc": match_view.start_time_utc,
        "patch": match_view.patch,
        "patch_source": match_view.patch_source,
        "winner": match_view.winner,
        "winner_team_index": match_view.winner_team_index,
        "team_1_label": match_view.team_1_label,
        "team_1_average_badge": match_view.team_1_average_badge,
        "team_1_hero_ids": [player.hero_id for player in match_view.team_1_players],
        "team_1_hero_names": [player.hero_name for player in match_view.team_1_players],
        "team_1_players": [asdict(player) for player in match_view.team_1_players],
        "team_2_label": match_view.team_2_label,
        "team_2_average_badge": match_view.team_2_average_badge,
        "team_2_hero_ids": [player.hero_id for player in match_view.team_2_players],
        "team_2_hero_names": [player.hero_name for player in match_view.team_2_players],
        "team_2_players": [asdict(player) for player in match_view.team_2_players],
    }


def format_match_view(match_view: MatchView) -> str:
    lines = [
        f"Match ID : {match_view.match_id}",
        f"Played   : {match_view.start_time_utc}",
        f"Patch    : {match_view.patch}",
        f"PatchSrc : {match_view.patch_source}",
        f"Winner   : {match_view.winner}",
        "",
        _format_team_block(
            match_view.team_1_label,
            match_view.team_1_players,
            match_view.team_1_average_badge,
        ),
        "",
        _format_team_block(
            match_view.team_2_label,
            match_view.team_2_players,
            match_view.team_2_average_badge,
        ),
    ]
    return "\n".join(lines)


def match_view_to_dataset_row(match_view: MatchView) -> dict[str, str]:
    team_1_heroes = _hero_columns(match_view.team_1_players, prefix="team_1")
    team_2_heroes = _hero_columns(match_view.team_2_players, prefix="team_2")

    return {
        "match_id": match_view.match_id,
        "start_time_s": "" if match_view.start_time_s is None else str(match_view.start_time_s),
        "start_time_utc": match_view.start_time_utc,
        "patch": match_view.patch,
        "patch_source": match_view.patch_source,
        "winner": match_view.winner,
        "winner_team_index": "" if match_view.winner_team_index is None else str(match_view.winner_team_index),
        "team_1_average_badge": match_view.team_1_average_badge,
        "team_2_average_badge": match_view.team_2_average_badge,
        **team_1_heroes,
        **team_2_heroes,
    }


def dataset_fieldnames() -> list[str]:
    return [
        "match_id",
        "start_time_s",
        "start_time_utc",
        "patch",
        "patch_source",
        "winner",
        "winner_team_index",
        "team_1_average_badge",
        "team_2_average_badge",
        *[f"team_1_hero_{index}" for index in range(1, 7)],
        *[f"team_2_hero_{index}" for index in range(1, 7)],
    ]


def _format_team_block(
    team_label: str, players: list[PlayerView], average_badge: str
) -> str:
    rows = []
    for player in players:
        rows.append(
            {
                "Player": player.player_name,
                "Hero": player.hero_name,
                "Rank": player.rank,
                "Account": player.account_id,
            }
        )

    badge_line = f"{team_label} (avg badge: {average_badge})"
    return _format_table(badge_line, rows)


def _format_table(title: str, rows: list[dict[str, str]]) -> str:
    if not rows:
        return f"{title}\n(no players found)"

    columns = ["Player", "Hero", "Rank", "Account"]
    widths = {
        column: max(len(column), *(len(str(row[column])) for row in rows))
        for column in columns
    }

    header = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    body = [
        " | ".join(str(row[column]).ljust(widths[column]) for column in columns)
        for row in rows
    ]
    return "\n".join([title, header, separator, *body])


def _hero_columns(players: list[PlayerView], prefix: str) -> dict[str, str]:
    hero_ids = [player.hero_id for player in players[:6]]
    padded = hero_ids + [""] * (6 - len(hero_ids))
    return {f"{prefix}_hero_{index}": hero_id for index, hero_id in enumerate(padded, start=1)}


def _sorted_players(players: list[PlayerView]) -> list[PlayerView]:
    return sorted(
        players,
        key=lambda player: (
            player.player_slot is None,
            player.player_slot if player.player_slot is not None else 999,
            player.account_id,
        ),
    )


def _unwrap_match_payload(payload: dict[str, Any]) -> dict[str, Any]:
    current = payload
    for key in ("data", "result", "match", "metadata", "match_info"):
        candidate = current.get(key)
        if isinstance(candidate, dict):
            current = candidate
    return current


def _find_players(node: Any, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 5:
        return []

    if isinstance(node, dict):
        for key in _PLAYER_LIST_KEYS:
            candidate = node.get(key)
            if _looks_like_player_list(candidate):
                return candidate

        for value in node.values():
            result = _find_players(value, depth + 1)
            if result:
                return result

    if isinstance(node, list):
        for item in node:
            result = _find_players(item, depth + 1)
            if result:
                return result

    return []


def _looks_like_player_list(candidate: Any) -> bool:
    if not isinstance(candidate, list) or not candidate:
        return False
    if not all(isinstance(item, dict) for item in candidate):
        return False

    interesting_keys = {
        "hero_id",
        "hero",
        "heroId",
        "team",
        "team_id",
        "player_team",
        "account_id",
        "steam_account_id",
    }
    score = 0
    for item in candidate:
        if any(key in item for key in interesting_keys):
            score += 1
    return score >= max(2, len(candidate) // 3)


def _extract_match_id(payload: dict[str, Any]) -> str:
    value = _pick_first(payload, "match_id", "matchId", "id")
    return str(value) if value is not None else "Unknown"


def _extract_patch(payload: dict[str, Any]) -> str:
    value = _pick_first(payload, *_PATCH_KEYS)
    return str(value) if value is not None else "Unknown"


def _extract_start_time(payload: dict[str, Any]) -> int | None:
    value = _pick_first(payload, "start_time", "start_time_s", "match_start_time")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_patch(payload: dict[str, Any], start_time_s: int | None) -> tuple[str, str]:
    explicit_patch = _extract_patch(payload)
    if explicit_patch != "Unknown":
        return explicit_patch, "explicit field in API response"
    return infer_patch_from_start_time(start_time_s)


def _format_start_time_utc(start_time_s: int | None) -> str:
    if start_time_s is None:
        return "Unknown"
    return datetime.fromtimestamp(start_time_s, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _extract_winner(payload: dict[str, Any]) -> Any:
    return _pick_first(payload, *_WINNER_KEYS)


def _extract_player_name(player: dict[str, Any], index: int) -> str:
    value = _pick_first(
        player,
        "player_name",
        "name",
        "persona_name",
        "steam_name",
        "nickname",
    )
    if value is not None:
        return str(value)

    account_id = _pick_first(player, "account_id", "steam_account_id", "accountId")
    if account_id is not None:
        return f"Player {account_id}"

    return f"Player {index}"


def _extract_account_id(player: dict[str, Any], index: int) -> str:
    value = _pick_first(player, "account_id", "steam_account_id", "accountId")
    if value is not None:
        return str(value)
    return f"unknown-{index}"


def _extract_hero_id(player: dict[str, Any]) -> int | str | None:
    direct = _pick_first(player, "hero_id", "heroId")
    if direct is not None:
        return direct

    hero = player.get("hero")
    if isinstance(hero, dict):
        return _pick_first(hero, "id", "hero_id", "heroId", "class_name", "name")

    if isinstance(hero, (int, str)):
        return hero

    return None


def _extract_rank(player: dict[str, Any]) -> str:
    value = _pick_first(
        player,
        "rank_name",
        "rank",
        "rank_tier",
        "ranked_rank",
        "medal",
        "badge_level",
    )
    if value is not None:
        return str(value)

    rank_info = player.get("rank_info") or player.get("ranked_badge")
    if isinstance(rank_info, dict):
        nested = _pick_first(rank_info, "name", "tier", "rank", "badge_level")
        if nested is not None:
            return str(nested)

    match_history = player.get("match_history") or player.get("player_card")
    if isinstance(match_history, dict):
        nested = _pick_first(match_history, "rank", "rank_name", "badge_level")
        if nested is not None:
            return str(nested)

    return "Unknown"


def _extract_player_slot(player: dict[str, Any]) -> int | None:
    value = _pick_first(player, "player_slot", "playerSlot", "slot")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_team_key(player: dict[str, Any], fallback: str) -> str:
    value = _pick_first(player, "team", "team_id", "player_team", "side", "side_id")
    if value is None:
        return fallback

    if isinstance(value, str):
        lowered = value.strip().lower().replace(" ", "_")
        if lowered:
            return lowered
        return fallback

    if isinstance(value, bool):
        return "team_1" if value else "team_2"

    if isinstance(value, (int, float)):
        if int(value) == 0:
            return "team_1"
        if int(value) == 1:
            return "team_2"
        return f"team_{int(value)}"

    return fallback


def _humanize_team_label(team_key: str, fallback: str) -> str:
    known = {
        "team_1": "Team 1",
        "team_2": "Team 2",
        "hidden_king": "Hidden King",
        "archmother": "Archmother",
        "archon": "Archon",
        "eternus": "Eternus",
    }
    if team_key in known:
        return known[team_key]
    return team_key.replace("_", " ").title() or fallback


def _format_winner(winner: Any, team_1_key: str, team_2_key: str) -> str:
    if winner is None:
        return "Unknown"

    if isinstance(winner, str):
        normalized = winner.strip().lower().replace(" ", "_")
        if normalized in {team_1_key, "team_1", "0"}:
            return _humanize_team_label(team_1_key, "Team 1")
        if normalized in {team_2_key, "team_2", "1"}:
            return _humanize_team_label(team_2_key, "Team 2")
        return winner

    if isinstance(winner, (int, float)):
        if int(winner) == 0:
            return _humanize_team_label(team_1_key, "Team 1")
        if int(winner) == 1:
            return _humanize_team_label(team_2_key, "Team 2")
        return str(winner)

    return str(winner)


def _normalize_winner_index(winner: Any) -> int | None:
    if isinstance(winner, bool):
        return 1 if winner else 0

    if isinstance(winner, (int, float)):
        normalized = int(winner)
        if normalized in {0, 1}:
            return normalized
        return None

    if isinstance(winner, str):
        normalized = winner.strip().lower().replace(" ", "_")
        if normalized in {"0", "team_1", "team0", "team_a", "hidden_king", "archon"}:
            return 0
        if normalized in {"1", "team_2", "team1", "team_b", "archmother", "eternus"}:
            return 1

    return None


def _extract_average_badge(payload: dict[str, Any], team_key: str) -> str:
    badge_key_map = {
        "team_1": "average_badge_team0",
        "team_2": "average_badge_team1",
    }
    badge_key = badge_key_map.get(team_key)
    if badge_key is None:
        return "Unknown"

    badge = payload.get(badge_key)
    if badge is None:
        return "Unknown"
    return str(badge)


def _pick_first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None
