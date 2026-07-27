"""Shared HTTP client for the Jira integrations."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterator, Optional

import requests
from requests.auth import HTTPBasicAuth

RETRY_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 60


class JiraClient:
    """Authenticated Jira REST client with retry and pagination helpers."""

    def __init__(self, base_jira_url: str) -> None:
        """Create a client using JIRA_USERNAME and JIRA_TOKEN from the environment."""
        self.logger = logging.getLogger(self.__class__.__name__)
        jira_username = os.environ.get("JIRA_USERNAME")
        jira_token = os.environ.get("JIRA_TOKEN")

        if not jira_username:
            raise ValueError("JIRA_USERNAME environment variable not set")
        if not jira_token:
            raise ValueError("JIRA_TOKEN environment variable not set")

        self.base_jira_url = base_jira_url.rstrip("/")
        self.auth = HTTPBasicAuth(jira_username, jira_token)

    @staticmethod
    def to_bq_timestamp(value: Optional[str]) -> Optional[str]:
        """Convert a Jira timestamp into a UTC ISO-8601 string, or None if unparseable."""
        if not value:
            return None

        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return (
                    datetime.strptime(value, fmt)
                    .astimezone(timezone.utc)
                    .isoformat(timespec="seconds")
                )
            except ValueError:
                continue

        return None

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float:
        """Return seconds to wait, preferring the server's Retry-After header."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))

    def get(self, path: str, params: dict) -> dict:
        """GET a Jira API path, retrying transient failures, and return parsed JSON."""
        url = f"{self.base_jira_url}{path}"
        headers = {"Accept": "application/json"}

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    auth=self.auth,
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                # Transport-level failures (connection reset, DNS blip, read
                # timeout) retry on the same schedule as a 5xx. Over the
                # thousands of calls a seed makes, a dropped connection is at
                # least as likely as a server error, and failing on the first
                # one would abort a 15-30 minute run that restarts from zero.
                if attempt < MAX_ATTEMPTS:
                    delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                    self.logger.warning(
                        "Retrying %s after %s in %.1fs (attempt %s/%s)",
                        path,
                        exc.__class__.__name__,
                        delay,
                        attempt,
                        MAX_ATTEMPTS,
                    )
                    time.sleep(delay)
                    continue

                raise RuntimeError(f"Failed while requesting {path}") from exc

            if 200 <= response.status_code <= 299:
                return response.json()

            if response.status_code in RETRY_STATUS_CODES and attempt < MAX_ATTEMPTS:
                delay = self._retry_delay(response, attempt)
                self.logger.warning(
                    "Retrying %s after status %s in %.1fs (attempt %s/%s)",
                    path,
                    response.status_code,
                    delay,
                    attempt,
                    MAX_ATTEMPTS,
                )
                time.sleep(delay)
                continue

            raise RuntimeError(
                f"Failed while requesting {path}: "
                f"status_code={response.status_code}, reason={response.reason}, "
                f"response_text={response.text}"
            )

        raise RuntimeError(f"Exhausted retries for {path}")

    def paginate_start_at(
        self,
        path: str,
        params: dict,
        key: str,
        first_page: Optional[dict] = None,
    ) -> Iterator[dict]:
        """Yield items from a startAt/maxResults/total paginated Jira endpoint.

        `first_page` lets a caller that has already fetched `startAt=0` — to
        validate its shape, say — hand that response in rather than paying for a
        second identical request.

        A missing `total` raises: every Jira `PageBean*` response carries it, so
        treating its absence as "this was the last page" would silently drop
        every item past page 1.
        """
        start_at = 0
        page = first_page

        while True:
            if page is None:
                page = self.get(path, {**params, "startAt": start_at})

            total = page.get("total")
            if total is None:
                raise RuntimeError(
                    f"Paginated response for {path} at startAt={start_at} has no 'total' field; "
                    f"keys were {sorted(page)}. Jira PageBean responses always carry it, so "
                    "stopping here could silently drop every remaining page."
                )

            items = page.get(key) or []
            yield from items

            start_at += len(items)
            if not items or start_at >= total:
                return

            page = None

    def paginate_token(self, path: str, params: dict, key: str) -> Iterator[dict]:
        """Yield items from a nextPageToken paginated Jira endpoint."""
        next_page_token = None

        while True:
            page_params = dict(params)
            if next_page_token:
                page_params["nextPageToken"] = next_page_token

            page = self.get(path, page_params)
            items = page.get(key) or []
            yield from items

            next_page_token = page.get("nextPageToken")
            if not items or not next_page_token:
                return
