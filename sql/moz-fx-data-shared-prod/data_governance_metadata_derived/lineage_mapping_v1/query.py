#!/usr/bin/env python3

"""Resolve upstream DataHub lineage for tables profiled in column_profiles_v1.

Reads the distinct (project, dataset, table) tuples from the previous
column_profiles_v1 partition and, for each, walks upstream lineage in DataHub
to find the source telemetry ping. Writes one row per table to
``lineage_mapping_v1``, overwriting the run's ``resolved_at`` date partition.

The lineage-walk logic is vendored from
``data-shared-llm-agents/agents/schema_enricher/src/schema_enricher/tools/probe_fetcher.py``
so this script depends only on ``google.cloud.bigquery`` + ``requests``.

Behavioral parity with the agent's probe_fetcher:

* ``_select_ping_from_lineage`` is preserved including the multi-app
  aggregation branch (cross-app Glean tables write a NULL source_ping row,
  matching the original ``write_lineage_mapping(..., None)`` behavior).
* "No ping found" and "multi-app" are cached as NULL rows (semantic). DataHub
  transport errors are NOT cached — the worker skips the row so the next run
  retries, mirroring the original ``_resolve_lineage`` which doesn't call
  ``write_lineage_mapping`` on HTTP errors. Total skipped count is logged.
* Per-call column-name filtering and the 500-probe cap that the agent applies
  in ``fetch_probe_definitions`` are intentionally **not** replicated here.
  This is the bulk cache layer — it materializes the full set; the agent
  filters at read time when it consumes ``probe_definitions_v1``.
* The 7-day TTL in the original agent cache is replaced by weekly partition
  overwrites; older partitions live for 90 days per metadata.yaml.

Requires DATAHUB_GMS_TOKEN in the environment.
"""
import argparse
import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_DATAHUB_URL = "https://mozilla.acryl.io/api/graphql"

_LINEAGE_QUERY = """
query GetUpstreamLineage($urn: String!) {
  searchAcrossLineage(input: {
    urn: $urn
    direction: UPSTREAM
    count: 250
  }) {
    searchResults {
      degree
      entity {
        urn
        type
        ... on Dataset {
          name
          platform { urn }
          subTypes { typeNames }
        }
      }
    }
  }
}
"""

_LINEAGE_MAPPING_SCHEMA = [
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_ping", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ping_platform", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stable_urn", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("resolved_at", "DATE", mode="REQUIRED"),
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


def _table_to_urn(project: str, dataset: str, table: str) -> str:
    """Build a DataHub dataset URN for a BigQuery table."""
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:bigquery,"
        f"{project}.{dataset}.{table},PROD)"
    )


def _glean_app_from_urn(urn: str) -> str | None:
    """Extract the Glean app slug from a Glean ping URN.

    e.g. "urn:li:dataset:(urn:li:dataPlatform:Glean,fenix.baseline,PROD)" -> "fenix"
    """
    parts = urn.split(",")
    if len(parts) >= 2:
        app_ping = parts[1]
        dot_idx = app_ping.find(".")
        if dot_idx != -1:
            return app_ping[:dot_idx]
    return None


def _select_ping_from_lineage(results: list[dict]) -> dict | None:
    """Pick the closest upstream ping from a DataHub lineage result list.

    Sorts by degree ascending (closest to the derived table first), and returns
    the first Glean or LegacyTelemetry Ping node. For Glean pings, returns
    source_ping as "app.ping" so the probe-definitions key is unique per app.
    For Legacy pings, also returns the URN of the closest upstream _stable BQ
    table (used downstream to fetch the probe field schema).

    Special cases:

    * Returns ``None`` when no ping node is present in the lineage.
    * Returns ``{"is_multi_app": True, "apps": [...]}`` when Glean pings from
      multiple distinct apps appear at the same minimum degree — i.e. the
      derived table is a cross-app aggregation and no single source ping can be
      assigned. Mirrors the behavior of
      ``data-shared-llm-agents/.../probe_fetcher.py``.
    """
    sorted_results = sorted(results, key=lambda r: r["degree"])

    ping_node = None
    ping_platform = None
    glean_app = None
    for result in sorted_results:
        entity = result["entity"]
        platform_urn = entity.get("platform", {}).get("urn", "")
        sub_types = entity.get("subTypes", {}).get("typeNames", [])
        if "LegacyTelemetry" in platform_urn and "Ping" in sub_types:
            ping_node = entity
            ping_platform = "legacy_telemetry"
            break
        if "Glean" in platform_urn and "Ping" in sub_types:
            ping_node = entity
            ping_platform = "glean"
            glean_app = _glean_app_from_urn(entity["urn"])
            break

    if not ping_node:
        return None

    if ping_platform == "glean":
        min_degree = next(
            r["degree"]
            for r in sorted_results
            if r["entity"]["urn"] == ping_node["urn"]
        )
        other_apps = {
            _glean_app_from_urn(r["entity"]["urn"])
            for r in sorted_results
            if (
                r["degree"] == min_degree
                and "Glean" in r["entity"].get("platform", {}).get("urn", "")
                and "Ping" in r["entity"].get("subTypes", {}).get("typeNames", [])
                and r["entity"]["urn"] != ping_node["urn"]
            )
        } - {None, glean_app}
        if other_apps:
            return {
                "is_multi_app": True,
                "apps": sorted({glean_app} | other_apps),
            }

    source_ping = (
        f"{glean_app}.{ping_node['name']}"
        if ping_platform == "glean" and glean_app
        else ping_node["name"]
    )

    stable_urn = None
    if ping_platform == "legacy_telemetry":
        ping_degree = next(
            r["degree"]
            for r in sorted_results
            if r["entity"]["urn"] == ping_node["urn"]
        )
        stable_candidates = [
            r
            for r in sorted_results
            if (
                r["degree"] < ping_degree
                and "bigquery" in r["entity"].get("platform", {}).get("urn", "")
                and "_stable." in r["entity"].get("urn", "")
            )
        ]
        if stable_candidates:
            stable_urn = max(stable_candidates, key=lambda r: r["degree"])["entity"][
                "urn"
            ]

    return {
        "ping_platform": ping_platform,
        "source_ping": source_ping,
        "stable_urn": stable_urn,
    }


def resolve_ping(project: str, dataset: str, table: str, token: str) -> dict | None:
    """Walk upstream DataHub lineage and return ping metadata, or None."""
    urn = _table_to_urn(project, dataset, table)
    data = _datahub_graphql(_LINEAGE_QUERY, {"urn": urn}, token)
    results = data["searchAcrossLineage"]["searchResults"]
    return _select_ping_from_lineage(results)


def list_profiled_tables(
    client: bigquery.Client,
    source_table_fq: str,
    profiled_at: str,
) -> list[tuple[str, str, str]]:
    """Return distinct (project, dataset, table) tuples from column_profiles_v1.

    Reads the partition for ``profiled_at`` so the lineage job operates on the
    same table set the profiler just wrote. Falls back to the most recent
    profiled_at partition within the last 14 days if the run-date partition is
    empty (the column_profiles job runs weekly; lineage may run on a date where
    no profile run happened).
    """
    query = f"""
        WITH latest AS (
          SELECT MAX(profiled_at) AS d
          FROM `{source_table_fq}`
          WHERE profiled_at BETWEEN
            DATE_SUB(@target, INTERVAL 14 DAY) AND @target
        )
        SELECT DISTINCT source_project, source_dataset, source_table
        FROM `{source_table_fq}`
        WHERE profiled_at = (SELECT d FROM latest)
        ORDER BY source_dataset, source_table
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("target", "DATE", profiled_at)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return [(r.source_project, r.source_dataset, r.source_table) for r in rows]


def resolve_for_table(
    table: tuple[str, str, str], token: str, resolved_at: str
) -> dict[str, Any] | None:
    """Resolve lineage for a single table and return a destination-row dict.

    Returns:
      * A row dict with populated ping fields when DataHub returns a ping.
      * A row dict with NULL ping fields when DataHub *successfully* reports
        no upstream ping, or detects a multi-app aggregation. This is the
        semantic "we asked and the answer is none" cache entry — matching the
        original probe_fetcher behavior of ``write_lineage_mapping(..., None)``.
      * ``None`` when DataHub itself is unreachable / errors. In that case the
        caller skips the row entirely so the next run can retry, rather than
        poisoning the cache with a NULL that looks like "no ping found."
    """
    project, dataset, table_name = table
    try:
        ping_info = resolve_ping(project, dataset, table_name, token)
    except Exception as e:
        logger.warning(
            f"Lineage failed for {project}.{dataset}.{table_name}: {e}. "
            f"Skipping row — not caching as NULL."
        )
        return None

    if ping_info and ping_info.get("is_multi_app"):
        logger.info(
            f"Multi-app aggregation at {project}.{dataset}.{table_name}: "
            f"{', '.join(ping_info['apps'])}. Writing NULL source_ping."
        )
        ping_info = None

    return {
        "source_project": project,
        "source_dataset": dataset,
        "source_table": table_name,
        "source_ping": ping_info["source_ping"] if ping_info else None,
        "ping_platform": ping_info["ping_platform"] if ping_info else None,
        "stable_urn": ping_info["stable_urn"] if ping_info else None,
        "resolved_at": resolved_at,
    }


def save_lineage(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    date: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
) -> None:
    """Overwrite the run's date partition with all resolved lineage rows."""
    job_config = bigquery.LoadJobConfig()
    job_config.schema = _LINEAGE_MAPPING_SCHEMA
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="resolved_at"
    )
    job_config.clustering_fields = ["source_dataset", "source_table"]

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
    parser.add_argument("--source-table", default="column_profiles_v1")
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument("--destination-table", default="lineage_mapping_v1")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument(
        "--tables",
        nargs="+",
        help=(
            "Optional list of fully-qualified BQ tables (project.dataset.table) "
            "to resolve instead of reading from column_profiles_v1. Use for "
            "ad-hoc verification against the agent's probe_fetcher.py. "
            "Combine with --dry-run to skip writes."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the tables that would be resolved and exit without writes.",
    )
    args = parser.parse_args()
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be ISO (YYYY-MM-DD); got {args.date!r}")
    return args


def main() -> None:
    """Resolve DataHub lineage for every table in the latest column_profiles partition."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    token = os.environ.get("DATAHUB_GMS_TOKEN", "")
    if not token:
        raise SystemExit(
            "DATAHUB_GMS_TOKEN is not set. Configure the secret in Airflow."
        )

    client = bigquery.Client(project=args.destination_project)

    if args.tables:
        tables: list[tuple[str, str, str]] = []
        for fqn in args.tables:
            parts = fqn.split(".")
            if len(parts) != 3:
                raise SystemExit(
                    f"--tables entries must be project.dataset.table; got {fqn!r}"
                )
            tables.append((parts[0], parts[1], parts[2]))
        logger.info(f"Resolving {len(tables)} table(s) from --tables")
    else:
        source_fq = f"{args.source_project}.{args.source_dataset}.{args.source_table}"
        tables = list_profiled_tables(client, source_fq, args.date)
        logger.info(f"Found {len(tables)} tables in {source_fq} for {args.date}")

    if args.dry_run:
        for project, dataset, table in tables[:20]:
            logger.info(f"  would resolve {project}.{dataset}.{table}")
        if len(tables) > 20:
            logger.info(f"  ... and {len(tables) - 20} more")
        return

    rows: list[dict[str, Any]] = []
    skipped = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(resolve_for_table, t, token, args.date): t for t in tables
        }
        for future in as_completed(futures):
            table = futures[future]
            try:
                row = future.result()
            except Exception as e:
                logger.error(f"Worker failed for {table}: {e}")
                skipped += 1
                continue
            if row is None:
                skipped += 1
                continue
            rows.append(row)

    logger.info(
        f"Resolved {len(rows)} tables; skipped {skipped} due to transient errors."
    )

    # A DataHub-wide outage (token expiration, network outage) would otherwise
    # log warnings and exit 0, hiding the failure from triage. Fail loudly when
    # nothing resolved AND at least one worker hard-failed.
    if not rows and skipped > 0:
        raise RuntimeError(
            f"All {skipped} lineage resolutions failed (0 tables resolved) — "
            f"failing the task. Likely DataHub outage or token issue."
        )

    if not rows:
        logger.warning("No lineage rows produced; nothing to write.")
        return

    save_lineage(
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
