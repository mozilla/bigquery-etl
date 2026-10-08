"""Downstream lineage of BigQuery tables, read from DataHub."""

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

import click
import requests

from bigquery_etl.datahub import auth

DEFAULT_DATAHUB_URL = "https://mozilla.acryl.io"
PAGE_SIZE = 1000
USAGE_BATCH_SIZE = 100
MAX_ATTEMPTS = 5
# GraphQL error messages DataHub returns when it is overloaded.
TRANSIENT_ERRORS = (
    "Connection lease request time out",
    "error while performing request",
    "Failed to execute PIT search",
)
REQUEST_TIMEOUT_SECONDS = 180
DEFAULT_LOOKER_TMP_DAYS = 3
# Values of DataHub's lineage degree facet, nearest first.
DATAHUB_LINEAGE_DEGREES = ["1", "2", "3+"]

# Looker aggregate tables: a new copy (with a new timestamp in epoch ms) is
# built on each refresh, and DataHub keeps lineage for every old copy.
LOOKER_TMP_RE = re.compile(r"looker_tmp\.LR_[0-9A-Z]{5}(\d{13})_")

# 256-color terminal codes for `click.style`, by platform.
PLATFORM_COLORS: dict[str, int | str] = {
    "redash": 208,  # orange
    "bigquery": "blue",
    "looker": "green",
}
TYPE_ORDER = ["DATASET", "DATA_JOB", "DATA_FLOW", "CHART", "DASHBOARD"]
# Entity types the lineage query can return.
ENTITY_TYPES = TYPE_ORDER + ["MLMODEL", "MLFEATURE_TABLE"]

LINEAGE_QUERY = """
query downstream($input: SearchAcrossLineageInput!) {
  searchAcrossLineage(input: $input) {
    start
    count
    total
    searchResults {
      degree
      entity {
        urn
        type
        ... on Dataset {
          name
          platform { name }
          properties { name qualifiedName }
          subTypes { typeNames }
          status { removed }
        }
        ... on Chart {
          platform { name }
          properties { name }
          subTypes { typeNames }
          browsePathV2 { ...browsePath }
          status { removed }
        }
        ... on Dashboard {
          platform { name }
          properties { name }
          subTypes { typeNames }
          browsePathV2 { ...browsePath }
          status { removed }
        }
        ... on DataJob {
          jobId
          dataFlow { platform { name } }
          properties { name }
          status { removed }
        }
        ... on DataFlow {
          platform { name }
          properties { name }
          status { removed }
        }
        ... on MLModel {
          name
          platform { name }
          status { removed }
        }
        ... on MLFeatureTable {
          name
          platform { name }
          status { removed }
        }
      }
    }
  }
}

fragment browsePath on BrowsePathV2 {
  path {
    name
    entity {
      ... on Container { properties { name } }
      ... on Dashboard { properties { name } }
    }
  }
}
"""


USAGE_QUERY = """
query usage($urns: [String!]!) {
  entities(urns: $urns) {
    urn
    ... on Dataset {
      statsSummary { queryCountLast30Days uniqueUserCountLast30Days }
      usageStats(range: MONTH) { buckets { bucket metrics { totalSqlQueries } } }
    }
  }
}
"""


class LineageTooLargeError(Exception):
    """DataHub refused a lineage query (maxRelations limit)."""


class AuthError(Exception):
    """DataHub rejected the token (HTTP 401/403)."""


class DataHubClient:
    """Authenticated DataHub GraphQL client with retries."""

    def __init__(self, token: Optional[str] = None, url: Optional[str] = None):
        """Create a client for DATAHUB_GMS_URL (default: Mozilla's DataHub).

        The token is, in order: the token argument, DATAHUB_GMS_TOKEN, or a
        browser sign-in (cached and refreshed between runs).
        """
        self.url = (
            url or os.environ.get("DATAHUB_GMS_URL") or DEFAULT_DATAHUB_URL
        ).rstrip("/")
        self.endpoint = self.url + "/api/graphql"
        token = token or os.environ.get("DATAHUB_GMS_TOKEN")
        self.signed_in = not token
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self._set_token(token or auth.get_token(self.url))

    def _set_token(self, token: str) -> None:
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _sign_in_again(self) -> None:
        """Drop the cached sign-in (e.g. revoked) and sign in through the browser."""
        auth.clear_tokens(self.url)
        self._set_token(auth.get_token(self.url))

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Run a GraphQL query and return its data."""
        signed_in_again = False
        for attempt in range(MAX_ATTEMPTS):
            last_attempt = attempt == MAX_ATTEMPTS - 1
            try:
                resp = self.session.post(
                    self.endpoint,
                    json={"query": query, "variables": variables},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except (requests.ConnectionError, requests.Timeout):
                if last_attempt:
                    raise
                time.sleep(2 ** (attempt + 1))
                continue

            if resp.status_code == 401 and self.signed_in and not signed_in_again:
                self._sign_in_again()
                signed_in_again = True
                continue
            # Retrying won't fix a bad or expired token.
            if resp.status_code in (401, 403):
                raise AuthError(f"HTTP {resp.status_code} from {self.endpoint}")
            if resp.status_code == 429 or resp.status_code >= 500:
                if last_attempt:
                    resp.raise_for_status()
                time.sleep(2 ** (attempt + 1))
                continue
            resp.raise_for_status()

            body = resp.json()
            if not body.get("errors"):
                return body["data"]
            message = body["errors"][0].get("message") or ""
            if "maxRelations" in message:
                raise LineageTooLargeError(message)
            # DataHub reports overload as a GraphQL error on an HTTP 200.
            if last_attempt or not any(m in message for m in TRANSIENT_ERRORS):
                raise RuntimeError(message)
            time.sleep(2 ** (attempt + 1))
        raise AssertionError("unreachable")

    def search_downstream(
        self,
        urn: str,
        include_deleted: bool = False,
        since_ms: Optional[int] = None,
        max_degree: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Return the downstream assets of urn, up to max_degree hops.

        max_degree 3 or None means all, since DataHub's degree facet ends at 3+.
        """
        lineage_flags = {}
        if since_ms is not None:
            lineage_flags = {
                "startTimeMillis": since_ms,
                "endTimeMillis": int(time.time() * 1000),
            }
        results = []
        start = 0
        while True:
            variables = {
                "input": {
                    "urn": urn,
                    "direction": "DOWNSTREAM",
                    "query": "*",
                    "start": start,
                    "count": PAGE_SIZE,
                    # Without this filter DataHub may only return direct (1st degree) children.
                    "orFilters": [
                        {
                            "and": [
                                {
                                    "field": "degree",
                                    "values": DATAHUB_LINEAGE_DEGREES[:max_degree],
                                }
                            ]
                        }
                    ],
                    "searchFlags": {"includeSoftDeleted": include_deleted},
                    "lineageFlags": lineage_flags,
                }
            }
            page = self.query(LINEAGE_QUERY, variables)["searchAcrossLineage"]
            results += [to_asset(r) for r in page["searchResults"]]
            start += page["count"]
            if page["count"] == 0 or start >= page["total"]:
                break

        # Paging can return the same asset twice; keep the closest occurrence.
        found: dict[str, dict[str, Any]] = {}
        for r in results:
            if r["urn"] not in found or r["degree"] < found[r["urn"]]["degree"]:
                found[r["urn"]] = r
        return sorted(found.values(), key=lambda r: (r["degree"], r["type"], r["name"]))

    def usage(self, urns: list[str]) -> dict[str, dict[str, Any]]:
        """Return query count, user count and last query date (last 30 days) per dataset.

        Counts come from DataHub's usage summary and are None when DataHub has
        no usage data for the dataset. The last query date is the latest day
        with queries in the last 30 days, or None.
        """
        usage = {}
        for i in range(0, len(urns), USAGE_BATCH_SIZE):
            data = self.query(USAGE_QUERY, {"urns": urns[i : i + USAGE_BATCH_SIZE]})
            for entity in data["entities"]:
                if not entity:
                    continue
                summary = entity.get("statsSummary") or {}
                buckets = (entity.get("usageStats") or {}).get("buckets") or []
                days = [
                    b["bucket"]
                    for b in buckets
                    if (b.get("metrics") or {}).get("totalSqlQueries")
                ]
                usage[entity["urn"]] = {
                    "queries_30d": summary.get("queryCountLast30Days"),
                    "users_30d": summary.get("uniqueUserCountLast30Days"),
                    "last_query": (
                        datetime.fromtimestamp(max(days) / 1000, timezone.utc)
                        .date()
                        .isoformat()
                        if days
                        else None
                    ),
                }
        return usage


def table_urn(table: str) -> str:
    """Return the DataHub URN of a project.dataset.table."""
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{table},PROD)"


def is_stale_looker_tmp(urn: str, cutoff_ms: Optional[int]) -> bool:
    """Return True for a Looker aggregate table copy built before cutoff_ms."""
    if cutoff_ms is None:
        return False
    m = LOOKER_TMP_RE.search(urn)
    return m is not None and int(m.group(1)) < cutoff_ms


def entity_name(entity: dict[str, Any]) -> str:
    """Return the display name of an entity."""
    props = entity.get("properties") or {}
    return (
        props.get("name") or entity.get("name") or entity.get("jobId") or entity["urn"]
    )


def entity_platform(entity: dict[str, Any]) -> Optional[str]:
    """Return the platform name of an entity."""
    platform = entity.get("platform") or (entity.get("dataFlow") or {}).get("platform")
    return (platform or {}).get("name")


def entity_subtype(entity: dict[str, Any]) -> Optional[str]:
    """Return the first DataHub subtype of an entity, e.g. Table or Explore."""
    names = (entity.get("subTypes") or {}).get("typeNames") or []
    return names[0] if names else None


def qualified_name(entity: dict[str, Any]) -> str:
    """Return the fully-qualified name where the platform has one, else the display name.

    BigQuery: project.dataset.table. Looker views: project + file path; Looker
    explores: model.explore. Looker dashboards and charts: folder path plus
    title. Redash has no folders, so it keeps the title.
    """
    urn = entity["urn"]
    if entity["type"] == "DATASET":
        qualified = (entity.get("properties") or {}).get("qualifiedName")
        if qualified:
            return qualified
        # urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)
        name = urn.split(",", 1)[1].rsplit(",", 1)[0]
        if entity_platform(entity) == "looker":
            if ".view." in name:  # looker-hub.<dir>.views.<file>.view.<view>
                return name.rsplit(".view.", 1)[0]
            if ".explore." in name:  # <model>.explore.<explore>
                return ".".join(name.split(".explore.", 1))
        return name
    path = (entity.get("browsePathV2") or {}).get("path") or []
    # Entries without an entity are fixed labels like "Folders" or "Default".
    folders = [
        ((p["entity"].get("properties") or {}).get("name") or p["name"])
        for p in path
        if p.get("entity")
    ]
    return " / ".join(folders + [entity_name(entity)])


def to_asset(result: dict[str, Any]) -> dict[str, Any]:
    """Flatten a searchAcrossLineage result into an asset record."""
    entity = result["entity"]
    return {
        "degree": result["degree"],
        "type": entity["type"],
        "subtype": entity_subtype(entity),
        "platform": entity_platform(entity),
        "name": entity_name(entity),
        "qualified_name": qualified_name(entity),
        "urn": entity["urn"],
        "deleted": bool((entity.get("status") or {}).get("removed")),
        # Filled in for BigQuery datasets by get_downstream_lineage.
        "queries_30d": None,
        "users_30d": None,
        "last_query": None,
    }


def get_downstream_lineage(
    client: DataHubClient,
    table: str,
    since_days: Optional[int] = None,
    include_deleted: bool = False,
    looker_tmp_days: Optional[int] = DEFAULT_LOOKER_TMP_DAYS,
    max_degree: Optional[int] = None,
    include_usage: bool = False,
    types: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    """Return the downstream assets of a project.dataset.table.

    With include_usage, BigQuery tables and views also get their query count,
    user count and last query date for the last 30 days (one extra request
    per 100 of them).
    """
    now = time.time()
    since_ms = int((now - since_days * 86400) * 1000) if since_days else None
    cutoff_ms = int((now - looker_tmp_days * 86400) * 1000) if looker_tmp_days else None

    assets = client.search_downstream(
        table_urn(table), include_deleted, since_ms, max_degree=max_degree
    )
    assets = [a for a in assets if not is_stale_looker_tmp(a["urn"], cutoff_ms)]
    if not include_deleted:
        assets = [a for a in assets if not a["deleted"]]
    if types:
        assets = [a for a in assets if a["type"] in types]

    if include_usage:
        # Usage is only recorded for BigQuery tables and views.
        bigquery = [a["urn"] for a in assets if is_bigquery_dataset(a)]
        usage = client.usage(bigquery) if bigquery else {}
        for a in assets:
            a.update(usage.get(a["urn"], {}))
    return assets


def is_bigquery_dataset(asset: dict[str, Any]) -> bool:
    """Return True for BigQuery tables and views."""
    return asset["platform"] == "bigquery" and asset["type"] == "DATASET"


def last_query_text(asset: dict[str, Any]) -> str:
    """Show the last query date, "none" if there was none in 30 days, or "n/a"."""
    if asset["last_query"]:
        return asset["last_query"]
    return "none" if asset["queries_30d"] == 0 else "n/a"


def type_rank(type_: str) -> int:
    """Sort key putting datasets first and dashboards last."""
    return TYPE_ORDER.index(type_) if type_ in TYPE_ORDER else len(TYPE_ORDER)


def format_text(
    table: str,
    assets: list[dict[str, Any]],
    show_deleted: bool = False,
    show_usage: bool = False,
) -> str:
    """Render assets as one table per platform, colored by platform.

    With show_usage, the BigQuery section gets the usage columns.
    """
    lines = [f"Downstream lineage of {table}", f"{len(assets)} assets"]

    groups: dict[str, list[dict[str, Any]]] = {}
    for a in assets:
        groups.setdefault(a["platform"] or "unknown", []).append(a)

    for platform in sorted(groups):
        rows = sorted(
            groups[platform],
            key=lambda a: (type_rank(a["type"]), a["degree"], a["qualified_name"]),
        )
        usage_columns = show_usage and any(is_bigquery_dataset(a) for a in rows)
        headers = (
            ["degree", "type", "name"]
            + (["deleted"] if show_deleted else [])
            + (["queries_30d", "users_30d", "last_query"] if usage_columns else [])
        )
        cells = [
            [
                str(a["degree"]),
                a["subtype"] or a["type"].replace("_", " ").capitalize(),
                a["qualified_name"],
            ]
            + ([str(a["deleted"])] if show_deleted else [])
            + (
                [
                    "n/a" if a["queries_30d"] is None else str(a["queries_30d"]),
                    "n/a" if a["users_30d"] is None else str(a["users_30d"]),
                    last_query_text(a),
                ]
                if usage_columns
                else []
            )
            for a in rows
        ]
        widths = [
            max(len(h), *(len(row[i]) for row in cells)) for i, h in enumerate(headers)
        ]
        color = PLATFORM_COLORS.get(platform)
        lines.append("")
        lines.append(
            click.style(f"{platform.upper()} ({len(rows)})", fg=color, bold=True)
        )
        lines.append(
            "  ".join(
                h.replace("_", " ").upper().ljust(w) for h, w in zip(headers, widths)
            ).rstrip()
        )
        lines.append("  ".join("-" * w for w in widths))
        for row in cells:
            line = "  ".join(v.ljust(w) for v, w in zip(row, widths)).rstrip()
            lines.append(click.style(line, fg=color))
    return "\n".join(lines)
