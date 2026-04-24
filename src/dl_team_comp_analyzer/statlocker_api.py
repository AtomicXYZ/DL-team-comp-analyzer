from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dl_team_comp_analyzer.env_utils import load_repo_env


DEFAULT_STATLOCKER_API_BASE = "https://statlocker.gg/api"


class StatlockerApiError(RuntimeError):
    """Raised when the Statlocker API request fails."""

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


class StatlockerRateLimitError(StatlockerApiError):
    """Raised when the Statlocker API rate limit is exceeded."""


class StatlockerApiClient:
    def __init__(
        self,
        api_base: str = DEFAULT_STATLOCKER_API_BASE,
        timeout_seconds: int = 20,
        api_key: str | None = None,
    ) -> None:
        load_repo_env()
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key or os.getenv("STATLOCKER_API_KEY")
        if not self.api_key:
            raise StatlockerApiError(
                "Missing STATLOCKER_API_KEY. Add it to .env or export it in your shell."
            )

    def fetch_profile(self, account_id: int | str) -> Any:
        return self._get_json(f"{self.api_base}/profile/aggregate-stats/{account_id}")

    def fetch_batch_profiles(self, account_ids: list[int | str]) -> Any:
        return self._post_json(f"{self.api_base}/profile/batch-profiles", account_ids)

    def _get_json(self, url: str) -> Any:
        request = Request(url, headers=self._headers(), method="GET")
        return self._read_json(request)

    def _post_json(self, url: str, payload: Any) -> Any:
        body = json.dumps(payload).encode("utf-8")
        headers = self._headers()
        headers["Content-Type"] = "application/json"
        request = Request(url, headers=headers, data=body, method="POST")
        return self._read_json(request)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": "dl-team-comp-analyzer/0.1",
            "X-API-Key": self.api_key,
        }

    def _read_json(self, request: Request) -> Any:
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            response_body = ""
            try:
                response_body = exc.read().decode("utf-8")
            except Exception:
                response_body = ""

            retry_after = exc.headers.get("Retry-After")
            retry_after_seconds: float | None = None
            if retry_after:
                try:
                    retry_after_seconds = float(retry_after)
                except ValueError:
                    retry_after_seconds = None

            error_cls = StatlockerRateLimitError if exc.code == 429 else StatlockerApiError
            raise error_cls(
                f"Statlocker API returned HTTP {exc.code} for {request.full_url}: {response_body[:200]}",
                status_code=exc.code,
                retry_after_seconds=retry_after_seconds,
            ) from exc
        except URLError as exc:
            raise StatlockerApiError(
                f"Could not reach Statlocker API at {request.full_url}"
            ) from exc

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise StatlockerApiError(
                f"Statlocker API did not return valid JSON for {request.full_url}"
            ) from exc
