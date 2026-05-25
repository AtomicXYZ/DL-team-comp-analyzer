from __future__ import annotations

import json
import math
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_GAME_API_BASE = "https://api.deadlock-api.com/v1"

class DeadlockApiError(RuntimeError):
    """Raised when the Deadlock API request fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class DeadlockRateLimitError(DeadlockApiError):
    """Raised when the Deadlock API rate limit is exceeded."""


class DeadlockApiClient:
    def __init__(
        self,
        game_api_base: str = DEFAULT_GAME_API_BASE,
        timeout_seconds: int = 20,
    ) -> None:
        self.game_api_base = game_api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch_bulk_match_metadata(self, **query_params: Any) -> Any:
        query = _encode_query_params(query_params)
        url = f"{self.game_api_base}/matches/metadata"
        if query:
            url = f"{url}?{query}"
        return self._get_json(url)

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
            response_body = ""
            try:
                response_body = exc.read().decode("utf-8")
            except Exception:
                response_body = ""

            retry_after_seconds = _extract_retry_after_seconds(exc, response_body)
            if exc.code == 429:
                raise DeadlockRateLimitError(
                    f"Deadlock API rate limit hit for {url}",
                    status_code=exc.code,
                    retry_after_seconds=retry_after_seconds,
                ) from exc

            raise DeadlockApiError(
                f"Deadlock API returned HTTP {exc.code} for {url}",
                status_code=exc.code,
                retry_after_seconds=retry_after_seconds,
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


def _extract_retry_after_seconds(exc: HTTPError, response_body: str) -> float | None:
    retry_after = exc.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass

    try:
        payload = json.loads(response_body) if response_body else {}
    except json.JSONDecodeError:
        payload = {}

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            quota = error.get("quota")
            if isinstance(quota, dict):
                limit = quota.get("limit")
                period = quota.get("period")
                try:
                    if limit and period:
                        return float(math.ceil(float(period) / float(limit)) + 1)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

    return None
