"""Client for the Searchfox endpoints the gecko trace event mapper uses.

Two endpoints are used:

- ``/<tree>/hgrev/<hg_rev>/`` redirects to ``/<tree>/rev/<git_rev>/`` and is
  the way to turn the hg changeset of a Firefox build into a git commit.
- ``/<tree>/blame-lines/<git_rev>/<path>[:<line>...][,<path>...]`` returns,
  for the requested lines of the requested files, the revision that
  introduced the line and the path and line number the line had in that
  revision.

The client only reads. It sends requests one at a time and pauses between
them, because the Searchfox web server serves four requests at a time for
everybody.
"""

import logging
import re
import time
from typing import Dict, Iterable, List, Mapping, Optional

import requests

SEARCHFOX_BASE_URL = "https://searchfox.org"
# Keep URLs well below common proxy limits.
MAX_URL_LENGTH = 6000
PAUSE_SECONDS = 0.2
RETRIES = 3
RETRY_PAUSE_SECONDS = 5

# buildhub2 repository URL -> Searchfox tree. Nightly builds come from
# mozilla-central, which Searchfox indexes as firefox-main. Autoland commits
# become ancestors of central once they merge, so they use the same tree.
TREE_BY_REPOSITORY = {
    "mozilla-central": "firefox-main",
    "integration/autoland": "firefox-main",
    "releases/mozilla-beta": "firefox-beta",
    "releases/mozilla-release": "firefox-release",
}
ESR_REPOSITORY = re.compile(r"releases/mozilla-(esr\d+)$")

# Event source paths can carry a checkout prefix. Everything up to and
# including one of these markers is removed to get a repository-relative path.
SOURCE_PATH_MARKERS = ("/checkouts/gecko/", "/gecko/", "/mozilla-unified/", "/src/")


class SearchfoxError(Exception):
    """A request failed for a reason a retry will not fix."""


class BlameUnavailable(SearchfoxError):
    """The revision exists but has no blame yet, for example a fresh nightly."""


def tree_for_repository(repository: Optional[str]) -> Optional[str]:
    """Return the Searchfox tree for a buildhub2 repository URL, or None."""
    if not repository:
        return None
    path = repository.rstrip("/")
    path = re.sub(r"^https?://hg\.mozilla\.org/", "", path)
    if path in TREE_BY_REPOSITORY:
        return TREE_BY_REPOSITORY[path]
    match = ESR_REPOSITORY.search(path)
    if match:
        return f"firefox-{match.group(1)}"
    return None


def normalize_source_path(source_file: str) -> str:
    """Turn an event source path into a repository-relative Searchfox path."""
    path = source_file.replace("\\", "/")
    for marker in SOURCE_PATH_MARKERS:
        index = path.find(marker)
        if index >= 0:
            path = path[index + len(marker) :]
            break
    while path.startswith("./"):
        path = path[2:]
    return path.lstrip("/")


class SearchfoxClient:
    """Read hg-to-git mappings and blame lines from Searchfox."""

    def __init__(
        self,
        base_url: str = SEARCHFOX_BASE_URL,
        session: Optional[requests.Session] = None,
        timeout: int = 60,
        pause: float = PAUSE_SECONDS,
    ):
        """Set the server, the HTTP session and the pause between requests."""
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.setdefault(
            "User-Agent", "bigquery-etl gecko-trace event mapper"
        )
        self.timeout = timeout
        self.pause = pause
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_count = 0

    def _get(self, url: str, allow_redirects: bool = True) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(RETRIES):
            if self.request_count:
                time.sleep(self.pause)
            self.request_count += 1
            try:
                response = self.session.get(
                    url, timeout=self.timeout, allow_redirects=allow_redirects
                )
            except requests.RequestException as error:
                last_error = error
                self.logger.warning("Request %s failed: %s", url, error)
            else:
                if response.status_code in (502, 503, 504):
                    last_error = SearchfoxError(
                        f"{url} returned {response.status_code}"
                    )
                    self.logger.warning(
                        "Request %s returned %s", url, response.status_code
                    )
                else:
                    return response
            if attempt + 1 < RETRIES:
                time.sleep(RETRY_PAUSE_SECONDS)
        raise SearchfoxError(f"Giving up on {url}: {last_error}")

    def resolve_hg_rev(self, tree: str, hg_rev: str) -> Optional[str]:
        """Return the git commit for an hg changeset, or None if unknown."""
        url = f"{self.base_url}/{tree}/hgrev/{hg_rev}/"
        response = self._get(url, allow_redirects=False)
        if response.status_code == 404:
            return None
        if response.status_code not in (301, 302):
            raise SearchfoxError(f"{url} returned {response.status_code}")
        location = response.headers.get("Location", "")
        match = re.search(r"/rev/([0-9a-f]{40})/", location)
        if not match:
            raise SearchfoxError(f"{url} redirected to unexpected {location!r}")
        return match.group(1)

    def blame_lines(
        self, tree: str, git_rev: str, selectors: Mapping[str, Optional[Iterable[int]]]
    ) -> Dict[str, Optional[List[dict]]]:
        """Return blame lines per path at git_rev.

        ``selectors`` maps a repository-relative path to the 1-based line
        numbers wanted, or to None for the whole file. The value for a path
        is a list of ``{"line", "rev", "path", "lineno"}`` dicts, one per
        requested line, or None when the file does not exist at that
        revision. A line past the end of the file has None origin fields.
        Raises BlameUnavailable when Searchfox has no blame for the revision.
        """
        result: Dict[str, Optional[List[dict]]] = {}
        for chunk in self._chunk_selectors(tree, git_rev, selectors):
            url = f"{self.base_url}/{tree}/blame-lines/{git_rev}/{','.join(chunk)}"
            response = self._get(url)
            if response.status_code != 200:
                text = response.text.strip()
                if "Unable to find blame" in text:
                    raise BlameUnavailable(f"{tree}@{git_rev}: {text}")
                raise SearchfoxError(f"{url} returned {response.status_code}: {text}")
            body = response.json()
            for entry in body["files"]:
                result[entry["path"]] = entry["lines"]
        return result

    def _chunk_selectors(
        self, tree: str, git_rev: str, selectors: Mapping[str, Optional[Iterable[int]]]
    ) -> Iterable[List[str]]:
        prefix_length = len(f"{self.base_url}/{tree}/blame-lines/{git_rev}/")
        chunk: List[str] = []
        length = prefix_length
        for path in sorted(selectors):
            if "," in path or ":" in path:
                self.logger.warning("Skipping path with a comma or colon: %s", path)
                continue
            lines = selectors[path]
            selector = path
            if lines is not None:
                selector += "".join(f":{line}" for line in sorted(set(lines)))
            if chunk and length + len(selector) + 1 > MAX_URL_LENGTH:
                yield chunk
                chunk, length = [], prefix_length
            chunk.append(selector)
            length += len(selector) + 1
        if chunk:
            yield chunk
