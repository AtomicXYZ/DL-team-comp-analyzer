from __future__ import annotations  # Maakt moderne type hints mogelijk.

from typing import Any  # API-response kan dict, list of iets anders zijn.


def extract_match_payloads(payload: Any) -> list[dict[str, Any]]:
    """Zoek in een API-response naar losse match dictionaries."""
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
    """Controleer of een object genoeg velden heeft om een match te zijn."""
    if not isinstance(candidate, dict):
        return False
    if "match_info" in candidate and isinstance(candidate["match_info"], dict):
        return True
    if {"match_id", "players"} <= set(candidate.keys()):
        return True
    return (
        "players" in candidate
        and isinstance(candidate["players"], list)
        and _pick_first(candidate, "winning_team", "winner") is not None
    )


def _pick_first(payload: dict[str, Any], *keys: str) -> Any:
    """Pak de eerste bestaande, niet-lege waarde uit meerdere mogelijke keys."""
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None
