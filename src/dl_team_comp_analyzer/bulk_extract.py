from __future__ import annotations

from typing import Any


def extract_match_payloads(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        direct = [item for item in payload if _looks_like_match(item)]
        if direct:
            return direct

        nested: list[dict[str, Any]] = []
        for item in payload:
            nested.extend(extract_match_payloads(item))
        return nested

    if isinstance(payload, dict):
        for key in ("matches", "results", "items", "data"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                direct = [item for item in candidate if _looks_like_match(item)]
                if direct:
                    return direct

        if _looks_like_match(payload):
            return [payload]

        nested: list[dict[str, Any]] = []
        for value in payload.values():
            nested.extend(extract_match_payloads(value))
        return nested

    return []


def _looks_like_match(candidate: Any) -> bool:
    if not isinstance(candidate, dict):
        return False
    if "match_info" in candidate and isinstance(candidate["match_info"], dict):
        return True
    if {"match_id", "players"} <= set(candidate.keys()):
        return True
    if "players" in candidate and isinstance(candidate["players"], list) and _pick_first(candidate, "winning_team", "winner") is not None:
        return True
    return False


def _pick_first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None
