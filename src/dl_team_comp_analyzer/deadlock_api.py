from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_GAME_API_BASE = "https://api.deadlock-api.com/v1"
DEFAULT_ASSETS_API_BASE = "https://assets.deadlock-api.com/v2"


class DeadlockApiError(RuntimeError):
    """Raised when the Deadlock API request fails."""


class DeadlockApiClient:
    def __init__(
        self,
        game_api_base: str = DEFAULT_GAME_API_BASE,
        assets_api_base: str = DEFAULT_ASSETS_API_BASE,
        timeout_seconds: int = 20,
    ) -> None:
        self.game_api_base = game_api_base.rstrip("/")
        self.assets_api_base = assets_api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._hero_cache: dict[str, dict[str, Any]] = {}

    def fetch_match_metadata(self, match_id: int | str) -> dict[str, Any]:
        return self._get_json(f"{self.game_api_base}/matches/{match_id}/metadata")

    def fetch_bulk_match_metadata(self, **query_params: Any) -> Any:
        query = _encode_query_params(query_params)
        url = f"{self.game_api_base}/matches/metadata"
        if query:
            url = f"{url}?{query}"
        return self._get_json(url)

    def fetch_player_match_history(self, account_id: int | str, **query_params: Any) -> Any:
        query = _encode_query_params(query_params)
        url = f"{self.game_api_base}/players/{account_id}/match-history"
        if query:
            url = f"{url}?{query}"
        return self._get_json(url)

    def save_json(self, payload: Any, output_path: str | Path) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output

    def get_hero(self, hero_id: int | str) -> dict[str, Any]:
        cache_key = str(hero_id)
        if cache_key not in self._hero_cache:
            self._hero_cache[cache_key] = self._get_json(
                f"{self.assets_api_base}/heroes/{hero_id}"
            )
        return self._hero_cache[cache_key]

    def get_hero_name(self, hero_id: int | str | None) -> str:
        if hero_id is None:
            return "Unknown Hero"

        try:
            hero = self.get_hero(hero_id)
        except DeadlockApiError:
            return f"Hero {hero_id}"

        return (
            hero.get("name")
            or hero.get("display_name")
            or hero.get("class_name")
            or f"Hero {hero_id}"
        )

    def _get_json(self, url: str) -> Any:
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "dl-team-comp-analyzer/0.1",
            },
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise DeadlockApiError(
                f"Deadlock API returned HTTP {exc.code} for {url}"
            ) from exc
        except URLError as exc:
            raise DeadlockApiError(f"Could not reach Deadlock API at {url}") from exc

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise DeadlockApiError(f"Deadlock API did not return valid JSON for {url}") from exc

        return payload


def _encode_query_params(query_params: dict[str, Any]) -> str:
    cleaned: list[tuple[str, str]] = []
    for key, value in query_params.items():
        if value is None:
            continue

        if isinstance(value, bool):
            cleaned.append((key, "true" if value else "false"))
            continue

        if isinstance(value, (list, tuple, set)):
            if not value:
                continue
            cleaned.append((key, ",".join(str(item) for item in value)))
            continue

        cleaned.append((key, str(value)))

    return urlencode(cleaned)
