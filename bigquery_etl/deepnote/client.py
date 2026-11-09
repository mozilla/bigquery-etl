"""Shared client for the Deepnote public API (https://deepnote.com/docs/api-reference).

Deepnote API keys are scoped to a single workspace, so each workspace to import is
configured by its own API key. Callers pass the names of the environment variables
holding the keys; the workspace a key belongs to is looked up through GET /v2/me.
"""

import os
from typing import Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

API_URL = "https://api.deepnote.com"
REQUEST_TIMEOUT_SECONDS = 60


class DeepnoteWorkspace:
    """Deepnote API session for the workspace an API key belongs to."""

    def __init__(self, api_key: str) -> None:
        """Create a session that retries rate limited requests and look up the workspace."""
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        workspace = self.get("/v2/me")["workspace"]
        self.id = workspace["id"]
        self.name = workspace["name"]

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET a Deepnote API path and return the parsed response."""
        response = self.session.get(
            f"{API_URL}{path}", params=params, timeout=REQUEST_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return response.json()

    def paginate(
        self, path: str, key: str, params: Optional[dict] = None, page_size=100
    ) -> Iterator[dict]:
        """Yield the items of a paginated Deepnote API list endpoint."""
        page_token = None
        while True:
            page_params = {"pageSize": page_size, **(params or {})}
            if page_token:
                page_params["pageToken"] = page_token
            page = self.get(path, page_params)
            yield from page[key]
            if not page["pagination"].get("hasMore"):
                return
            page_token = page["pagination"]["nextPageToken"]


def workspaces(api_key_env_vars: List[str]) -> List[DeepnoteWorkspace]:
    """Return a session per workspace for the API keys in the given environment variables."""
    return [DeepnoteWorkspace(os.environ[name]) for name in api_key_env_vars]
