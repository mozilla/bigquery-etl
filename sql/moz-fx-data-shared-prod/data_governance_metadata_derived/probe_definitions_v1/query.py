#!/usr/bin/env python3

"""Fetch probe definitions for every distinct ping in lineage_mapping_v1.

Reads the latest ``lineage_mapping_v1`` partition, groups by
``(ping_platform, source_ping)``, and fetches probe definitions from:

* the Glean Dictionary JSON API, for Glean pings, or
* the closest upstream ``_stable`` table's DataHub schema, for Legacy Telemetry.

Glean rows additionally carry ``data_sensitivity`` and ``send_in_pings``, merged
in from probe-info (the app's own metrics plus its libraries'), and ``tags``,
which the Dictionary response already provides.

Writes one row per probe to ``probe_definitions_v1``, overwriting the run's
``fetched_at`` date partition.

The fetch logic is vendored from
``data-shared-llm-agents/agents/schema_enricher/src/schema_enricher/tools/probe_fetcher.py``
so this script depends only on ``google.cloud.bigquery`` + ``requests``.

Behavioral parity with the agent's probe_fetcher:

* ``fetch_legacy_probes`` matches the agent's helper on the GraphQL query and on
  the probe name, description, and type mapping, and adds the sensitivity fields
  below as empty. ``fetch_glean_probes`` matches on the same three and populates
  them, which the column classification pipeline needs.
* The 500-probe cap (``_MAX_UNFILTERED_PROBES``) and column-name fuzzy
  filtering from ``fetch_probe_definitions`` are intentionally **not**
  replicated. Those exist to keep LLM context small at agent read time; the
  cache layer materializes everything and lets the agent filter at SELECT
  time via WHERE clauses on probe_name.
* The 7-day TTL in the original agent cache is replaced by weekly partition
  overwrites; older partitions live for 90 days per metadata.yaml.

Requires DATAHUB_GMS_TOKEN in the environment for the legacy-telemetry path.
"""

import argparse
import datetime
import functools
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_DATAHUB_URL = "https://mozilla.acryl.io/api/graphql"
_GLEAN_DICT_URL = (
    "https://dictionary.telemetry.mozilla.org/data/{app}/pings/{ping}.json"
)
# probe-info carries data_sensitivity, which the Dictionary per-ping endpoint
# omits. Slugs are dashed here and underscored in _GLEAN_DICT_URL.
_PROBE_INFO_URL = "https://probeinfo.telemetry.mozilla.org/glean/{app}/metrics"
_PROBE_INFO_REPOS_URL = "https://probeinfo.telemetry.mozilla.org/glean/repositories"
_PROBE_INFO_APP_LISTINGS_URL = (
    "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
)

_SCHEMA_FIELDS_QUERY = """
query GetSchemaFields($urn: String!) {
  dataset(urn: $urn) {
    schemaMetadata {
      fields {
        fieldPath
        nativeDataType
        description
      }
    }
  }
}
"""

_PROBE_DEFINITIONS_SCHEMA = [
    bigquery.SchemaField("ping_platform", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_ping", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("probe_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("probe_description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("probe_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fetched_at", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("data_sensitivity", "STRING", mode="REPEATED"),
    bigquery.SchemaField("send_in_pings", "STRING", mode="REPEATED"),
    bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
]


def _datahub_graphql(query: str, variables: dict, token: str) -> dict:
    """Execute a DataHub GraphQL query and return the response data dict."""
    response = requests.post(
        _DATAHUB_URL,
        json={"query": query, "variables": variables},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {body['errors']}")
    return body["data"]


def _get_json(url: str, description: str, missing_ok: bool = False) -> Any:
    """GET and parse JSON, returning None for a 404 when ``missing_ok``.

    Every other failure raises. A timeout or 5xx is indistinguishable from an
    empty result once it has been turned into one, and the run would go on to
    write a partition whose fields are silently missing.
    """
    try:
        response = requests.get(url, timeout=30)
        if missing_ok and response.status_code == 404:
            logger.warning(f"{description} not found: {url}")
            return None
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        raise RuntimeError(f"Could not fetch {description} from {url}") from e


_CATALOG_CACHE: dict[str, Any] = {}


def _fetch_catalog(url: str, description: str) -> list[dict[str, Any]]:
    """Fetch a probe-info catalog endpoint, caching only successful results."""
    if url not in _CATALOG_CACHE:
        data = _get_json(url, description)
        if not data:
            raise RuntimeError(f"{description} is empty: {url}")
        _CATALOG_CACHE[url] = data
    return _CATALOG_CACHE[url]


@functools.lru_cache(maxsize=None)
def _fetch_metrics_map(slug: str) -> dict[str, dict[str, Any]]:
    """Fetch one probe-info metrics file, mapping metric name to latest history.

    A 404 yields an empty map, since a repository without a metrics file is a
    data gap rather than an outage. Cached per slug so a shared library is
    fetched once; treat the result as read-only.
    """
    metrics = _get_json(
        _PROBE_INFO_URL.format(app=slug), f"probe-info {slug}", missing_ok=True
    )
    if not metrics:
        return {}

    result = {}
    for name, definition in metrics.items():
        history = (definition or {}).get("history") or []
        if history:
            result[name] = history[-1]
    return result


def _repository_slugs(app: str) -> tuple[str, ...]:
    """Map a Glean Dictionary app name to its probe-info repository names.

    app-listings maps one ``app_name`` to a ``v1_name`` per channel and per
    platform: ``mozilla_vpn`` has one each for desktop, Android and iOS, plus the
    iOS network extension.
    Metrics can be unique to one of them, so read all of them, as
    ``glean_usage`` does for its per-app artifacts. Release entries come first so
    their definitions win, treating an absent ``app_channel`` as release.
    """
    listings = [
        listing
        for listing in _fetch_catalog(
            _PROBE_INFO_APP_LISTINGS_URL, "probe-info app listings"
        )
        if listing.get("app_name") == app and listing.get("v1_name")
    ]
    if not listings:
        logger.warning(f"No probe-info app listing for Glean app {app!r}")
        return (app.replace("_", "-"),)

    slugs: list[str] = []
    for listing in sorted(
        listings, key=lambda entry: (entry.get("app_channel") or "release") != "release"
    ):
        if listing["v1_name"] not in slugs:
            slugs.append(listing["v1_name"])
    return tuple(slugs)


def _metric_sources(app: str) -> tuple[str, ...]:
    """Return the probe-info slugs to read for an app: the app plus its libraries."""
    repositories = _fetch_catalog(_PROBE_INFO_REPOS_URL, "probe-info repositories")

    by_name = {repo["name"]: repo for repo in repositories if repo.get("name")}
    slug_by_library = {
        library_name: name
        for name, repo in by_name.items()
        for library_name in repo.get("library_names") or []
    }

    sources: list[str] = []
    queue = list(_repository_slugs(app))
    while queue:
        slug = queue.pop(0)
        if slug in sources:
            continue
        sources.append(slug)
        for dependency in (by_name.get(slug) or {}).get("dependencies") or []:
            if dependency in slug_by_library:
                queue.append(slug_by_library[dependency])
            elif dependency in by_name:
                queue.append(dependency)
            else:
                logger.warning(f"Unknown probe-info dependency {dependency!r}")
    return tuple(sources)


@functools.lru_cache(maxsize=None)
def fetch_probe_info(app: str) -> dict[str, dict[str, Any]]:
    """Fetch probe-info metric definitions for a Glean app, keyed by metric name.

    ``app`` is the Glean Dictionary app name (e.g. ``firefox_desktop``). Merges
    the app's own metrics with its libraries', app winning on conflict. Read-only.
    """
    merged: dict[str, dict[str, Any]] = {}
    for slug in _metric_sources(app):
        for name, definition in _fetch_metrics_map(slug).items():
            merged.setdefault(name, definition)
    return merged


def fetch_glean_probes(source_ping: str) -> list[dict[str, Any]]:
    """Fetch probe definitions for a Glean ping from the Glean Dictionary.

    ``source_ping`` is expected in ``app.ping`` form (e.g. ``fenix.baseline``).
    Returns an empty list for 404s (typically: Glean Dictionary doesn't have an
    entry for that app/ping pair).

    ``data_sensitivity`` and ``send_in_pings`` come from probe-info, matched on
    the dotted metric name, falling back to the Dictionary's ``pings``; ``tags``
    comes from the Dictionary.
    """
    if "." not in source_ping:
        logger.warning(
            f"Glean source_ping {source_ping!r} missing app prefix; skipping."
        )
        return []
    app, ping = source_ping.split(".", 1)
    url = _GLEAN_DICT_URL.format(app=app, ping=ping)
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        logger.warning(f"Glean Dictionary entry not found: {url}")
        return []
    response.raise_for_status()
    probe_info = fetch_probe_info(app)
    probes = []
    for m in response.json().get("metrics", []):
        info = probe_info.get(m.get("name")) or {}
        probes.append(
            {
                "probe_name": m.get("name"),
                "probe_description": m.get("description"),
                "probe_type": m.get("type"),
                "data_sensitivity": info.get("data_sensitivity") or [],
                "send_in_pings": info.get("send_in_pings") or m.get("pings") or [],
                "tags": m.get("tags") or [],
            }
        )
    return probes


def fetch_legacy_probes(stable_urn: str, token: str) -> list[dict[str, Any]]:
    """Fetch probe definitions from a Legacy Telemetry stable table schema."""
    data = _datahub_graphql(_SCHEMA_FIELDS_QUERY, {"urn": stable_urn}, token)
    schema = (data.get("dataset") or {}).get("schemaMetadata")
    if not schema:
        return []
    return [
        {
            "probe_name": f.get("fieldPath"),
            "probe_description": f.get("description"),
            "probe_type": f.get("nativeDataType"),
            "data_sensitivity": [],
            "send_in_pings": [],
            "tags": [],
        }
        for f in schema["fields"]
    ]


def list_pings(
    client: bigquery.Client,
    source_table_fq: str,
    resolved_at: str,
) -> list[dict[str, Any]]:
    """Return distinct (ping_platform, source_ping, stable_urn) from lineage_mapping_v1.

    Reads the most recent ``resolved_at`` partition within the last 14 days
    (the lineage job runs weekly; this job may run on a date with no fresh
    resolution).
    """
    # MAX(stable_urn) picks the alphabetically-latest URN when multiple source
    # tables sharing a legacy ping point to different _stable tables (rare —
    # usually only across schema version bumps, e.g. main_v4 vs main_v5).
    # Deterministic (unlike ANY_VALUE) and typically selects the newest version.
    # Glean rows are unaffected because their stable_urn is always NULL.
    query = f"""
        WITH latest AS (
          SELECT MAX(resolved_at) AS d
          FROM `{source_table_fq}`
          WHERE resolved_at BETWEEN
            DATE_SUB(@target, INTERVAL 14 DAY) AND @target
            AND source_ping IS NOT NULL
        )
        SELECT
          ping_platform,
          source_ping,
          MAX(stable_urn) AS stable_urn
        FROM `{source_table_fq}`
        WHERE resolved_at = (SELECT d FROM latest)
          AND source_ping IS NOT NULL
        GROUP BY ping_platform, source_ping
        ORDER BY ping_platform, source_ping
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("target", "DATE", resolved_at)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return [
        {
            "ping_platform": r.ping_platform,
            "source_ping": r.source_ping,
            "stable_urn": r.stable_urn,
        }
        for r in rows
    ]


def fetch_for_ping(
    ping: dict[str, Any], token: str, fetched_at: str
) -> list[dict[str, Any]]:
    """Fetch all probes for one ping and return destination-row dicts.

    Returns an empty list for benign "no probes" cases (Glean Dictionary 404,
    legacy ping without a stable_urn, unknown platform). Transport / HTTP
    errors propagate so the caller can distinguish hard failures from
    legitimately-empty pings (mirrors the lineage_mapping skip-on-error
    semantics).
    """
    platform = ping["ping_platform"]
    source_ping = ping["source_ping"]
    if platform == "glean":
        probes = fetch_glean_probes(source_ping)
    elif platform == "legacy_telemetry":
        if not ping.get("stable_urn"):
            logger.warning(f"Legacy ping {source_ping} has no stable_urn; skipping.")
            return []
        probes = fetch_legacy_probes(ping["stable_urn"], token)
    else:
        logger.warning(f"Unknown ping_platform {platform!r}; skipping.")
        return []

    return [
        {
            "ping_platform": platform,
            "source_ping": source_ping,
            "probe_name": p.get("probe_name"),
            "probe_description": p.get("probe_description"),
            "probe_type": p.get("probe_type"),
            "fetched_at": fetched_at,
            # REPEATED fields reject a JSON null, so always emit a list.
            "data_sensitivity": p.get("data_sensitivity") or [],
            "send_in_pings": p.get("send_in_pings") or [],
            "tags": p.get("tags") or [],
        }
        for p in probes
    ]


def save_probes(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    date: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
) -> None:
    """Overwrite the run's date partition with all fetched probe rows."""
    job_config = bigquery.LoadJobConfig()
    job_config.schema = _PROBE_DEFINITIONS_SCHEMA
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="fetched_at"
    )
    job_config.clustering_fields = ["ping_platform", "source_ping"]

    partition_date = date.replace("-", "")
    client.load_table_from_json(
        rows,
        f"{destination_project}.{destination_dataset}.{destination_table}"
        f"${partition_date}",
        job_config=job_config,
    ).result()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", required=True, help="Run date (YYYY-MM-DD); the partition key."
    )
    parser.add_argument("--source-project", default="moz-fx-data-shared-prod")
    parser.add_argument("--source-dataset", default="data_governance_metadata_derived")
    parser.add_argument("--source-table", default="lineage_mapping_v1")
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument("--destination-table", default="probe_definitions_v1")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument(
        "--pings",
        nargs="+",
        help=(
            "Optional list of pings to fetch instead of reading from "
            "lineage_mapping_v1. Format: 'glean:app.ping' (e.g. "
            "'glean:fenix.baseline') or 'legacy:ping_name:<stable_urn>'. "
            "Use for ad-hoc verification against the agent's probe_fetcher.py."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the pings that would be fetched and exit without writes.",
    )
    args = parser.parse_args()
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be ISO (YYYY-MM-DD); got {args.date!r}")
    return args


def main() -> None:
    """Fetch probe definitions for every distinct ping in lineage_mapping_v1."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    token = os.environ.get("DATAHUB_GMS_TOKEN", "")
    # Token is only required for the legacy-telemetry path; warn but don't
    # fail at startup. Per-ping fetches will skip legacy entries if missing.
    if not token:
        logger.warning(
            "DATAHUB_GMS_TOKEN is not set; legacy_telemetry probes will be skipped."
        )

    client = bigquery.Client(project=args.destination_project)

    if args.pings:
        pings: list[dict[str, Any]] = []
        for entry in args.pings:
            parts = entry.split(":", 2)
            if len(parts) < 2 or parts[0] not in {"glean", "legacy"}:
                raise SystemExit(
                    f"--pings entries must be 'glean:app.ping' or "
                    f"'legacy:ping:<stable_urn>'; got {entry!r}"
                )
            platform = "legacy_telemetry" if parts[0] == "legacy" else "glean"
            stable_urn = (
                parts[2] if platform == "legacy_telemetry" and len(parts) == 3 else None
            )
            if platform == "legacy_telemetry" and not stable_urn:
                raise SystemExit(f"--pings legacy entry needs stable_urn: {entry!r}")
            pings.append(
                {
                    "ping_platform": platform,
                    "source_ping": parts[1],
                    "stable_urn": stable_urn,
                }
            )
        logger.info(f"Fetching {len(pings)} ping(s) from --pings")
    else:
        source_fq = f"{args.source_project}.{args.source_dataset}.{args.source_table}"
        pings = list_pings(client, source_fq, args.date)
        logger.info(f"Found {len(pings)} distinct pings in {source_fq}")

    if args.dry_run:
        for ping in pings:
            logger.info(f"  would fetch {ping['ping_platform']}/{ping['source_ping']}")
        return

    # Fill the probe-info caches one app at a time before fanning out. lru_cache
    # does not coalesce in-flight calls, so concurrent workers on one app can race
    # a transient failure against a success, giving two pings different
    # sensitivity for the same probe and caching whichever finished last. Doing it
    # here also fails a catalog outage before anything is written.
    glean_apps = {
        ping["source_ping"].split(".", 1)[0]
        for ping in pings
        if ping["ping_platform"] == "glean" and "." in ping["source_ping"]
    }
    for app in sorted(glean_apps):
        fetch_probe_info(app)

    rows: list[dict[str, Any]] = []
    hard_failures = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(fetch_for_ping, p, token, args.date): p for p in pings
        }
        for future in as_completed(futures):
            ping = futures[future]
            try:
                fetched = future.result()
                rows.extend(fetched)
                logger.info(
                    f"  {ping['ping_platform']}/{ping['source_ping']}: "
                    f"{len(fetched)} probes"
                )
            except Exception as e:
                logger.error(
                    f"Worker failed for {ping['ping_platform']}/"
                    f"{ping['source_ping']}: {e}"
                )
                hard_failures += 1

    logger.info(
        f"Fetched probes for {len(pings) - hard_failures} ping(s); "
        f"{hard_failures} hard failure(s); {len(rows)} total probe rows."
    )

    # Distinguish "every fetch hard-failed" (catastrophic — DataHub/Glean
    # outage, token issue) from "every fetch legitimately empty" (benign —
    # Glean Dictionary has no entries for the input pings). Fail loudly only
    # in the catastrophic case, mirroring column_profiles_v1's pattern.
    if not rows and hard_failures > 0:
        raise RuntimeError(
            f"All probe fetches failed ({hard_failures} hard failure(s), "
            f"0 probes fetched) — failing the task. Likely DataHub or Glean "
            f"Dictionary outage."
        )

    if not rows:
        logger.warning("No probe rows produced; nothing to write.")
        return

    save_probes(
        client,
        rows,
        args.date,
        args.destination_project,
        args.destination_dataset,
        args.destination_table,
    )
    logger.info(
        f"Wrote {len(rows)} rows to "
        f"{args.destination_project}.{args.destination_dataset}."
        f"{args.destination_table}${args.date.replace('-', '')}"
    )


if __name__ == "__main__":
    main()
