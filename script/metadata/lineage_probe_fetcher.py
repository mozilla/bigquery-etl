"""Resolve upstream lineage for profiled tables and fetch probe definitions from source pings.

Two output tables:
  akomar_metadata_phase2_table_pings_v1  — lineage mapping: one row per derived table
  akomar_metadata_phase2_ping_probes_v1  — probe definitions: one row per probe in a ping (fetched once per ping)
"""

import json
import logging
import os
from argparse import ArgumentParser
from datetime import datetime, timezone

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

DATAHUB_URL = "https://mozilla.acryl.io/api/graphql"
GLEAN_DICT_URL = "https://dictionary.telemetry.mozilla.org/data/{app}/pings/{ping}.json"
PHASE1_TABLE = "mozdata-nonprod.analysis.akomar_data_profiling_v1"
MAPPING_TABLE = "mozdata-nonprod.analysis.akomar_metadata_phase2_table_pings_v1"
PROBE_TABLE = "mozdata-nonprod.analysis.akomar_metadata_phase2_ping_probes_v1"
DEST_PROJECT = "mozdata-nonprod"

MAPPING_SCHEMA = [
    bigquery.SchemaField(
        "source_project",
        "STRING",
        mode="REQUIRED",
        description="GCP project of the derived BQ table.",
    ),
    bigquery.SchemaField(
        "source_dataset",
        "STRING",
        mode="REQUIRED",
        description="BQ dataset of the derived table.",
    ),
    bigquery.SchemaField(
        "source_table",
        "STRING",
        mode="REQUIRED",
        description="BQ table name of the derived table.",
    ),
    bigquery.SchemaField(
        "source_ping",
        "STRING",
        mode="NULLABLE",
        description="Name of the source telemetry ping resolved via DataHub lineage (e.g. first-shutdown, baseline). Null if no ping node found in lineage.",
    ),
    bigquery.SchemaField(
        "ping_platform",
        "STRING",
        mode="NULLABLE",
        description="Telemetry platform of the source ping: 'glean' or 'legacy_telemetry'. Null if no ping found.",
    ),
    bigquery.SchemaField(
        "stable_urn",
        "STRING",
        mode="NULLABLE",
        description="DataHub URN of the closest _stable.* BQ table in the lineage chain. Legacy telemetry only; used to fetch probe schema fields.",
    ),
    bigquery.SchemaField(
        "processed_at",
        "TIMESTAMP",
        mode="REQUIRED",
        description="Timestamp when lineage was resolved for this table.",
    ),
]

PROBE_SCHEMA = [
    bigquery.SchemaField(
        "ping_platform",
        "STRING",
        mode="REQUIRED",
        description="Telemetry platform: 'glean' or 'legacy_telemetry'.",
    ),
    bigquery.SchemaField(
        "source_ping",
        "STRING",
        mode="REQUIRED",
        description="Name of the telemetry ping (e.g. first-shutdown, baseline).",
    ),
    bigquery.SchemaField(
        "probe_name",
        "STRING",
        mode="NULLABLE",
        description=(
            "Name of the probe/metric as defined in the source."
            " Legacy probes use dot notation (e.g. environment.system.os); Glean probes use snake_case."
        ),
    ),
    bigquery.SchemaField(
        "probe_description",
        "STRING",
        mode="NULLABLE",
        description="Description from the source artifact: Glean Dictionary for Glean pings, stable table schema fields for Legacy Telemetry.",
    ),
    bigquery.SchemaField(
        "probe_type",
        "STRING",
        mode="NULLABLE",
        description="Data type or kind of the probe (e.g. string, counter, labeled_counter, uint32).",
    ),
    bigquery.SchemaField(
        "probe_raw",
        "STRING",
        mode="NULLABLE",
        description="Full probe definition as JSON from the source artifact.",
    ),
    bigquery.SchemaField(
        "data_sensitivity",
        "STRING",
        mode="REPEATED",
        description="Glean data_sensitivity labels (e.g. technical, interaction, web_activity, highly_sensitive). Empty for legacy telemetry.",
    ),
    bigquery.SchemaField(
        "send_in_pings",
        "STRING",
        mode="REPEATED",
        description="Glean send_in_pings list — which pings the metric is sent in. Empty for legacy telemetry.",
    ),
    bigquery.SchemaField(
        "tags",
        "STRING",
        mode="REPEATED",
        description="Glean metric tags from metadata.tags. Empty for legacy telemetry.",
    ),
    bigquery.SchemaField(
        "processed_at",
        "TIMESTAMP",
        mode="REQUIRED",
        description="Timestamp when probe definitions were fetched for this ping.",
    ),
]


# --- DataHub GraphQL client ---


def datahub_graphql(query, variables):
    """Execute a DataHub GraphQL query and return the response data dict."""
    token = os.environ["DATAHUB_GMS_TOKEN"]
    response = requests.post(
        DATAHUB_URL,
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


LINEAGE_QUERY = """
query GetUpstreamLineage($urn: String!) {
  searchAcrossLineage(input: {
    urn: $urn
    direction: UPSTREAM
    count: 100
  }) {
    total
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

SCHEMA_FIELDS_QUERY = """
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


def get_lineage(table_urn):
    """Return upstream lineage search results for a dataset URN."""
    data = datahub_graphql(LINEAGE_QUERY, {"urn": table_urn})
    return data["searchAcrossLineage"]["searchResults"]


def get_schema_fields(dataset_urn):
    """Return schema fields for a dataset URN as a list of dicts."""
    data = datahub_graphql(SCHEMA_FIELDS_QUERY, {"urn": dataset_urn})
    schema = data.get("dataset", {}).get("schemaMetadata")
    if not schema:
        return []
    return schema["fields"]


# --- Lineage resolver ---


def table_to_urn(project, dataset, table):
    """Build a DataHub dataset URN for a BigQuery table."""
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{project}.{dataset}.{table},PROD)"


def resolve_ping(project, dataset, table):
    """Walk upstream lineage and return ping metadata for a BQ table.

    Returns a dict with keys:
      ping_platform  - 'glean' or 'legacy_telemetry'
      source_ping    - ping name, e.g. 'new-profile'
      stable_urn     - DataHub URN of the closest stable BQ table (Legacy only, else None)
    Returns None if no ping node is found in the lineage.
    """
    urn = table_to_urn(project, dataset, table)
    logging.info(f"Fetching lineage for {project}.{dataset}.{table}")
    results = get_lineage(urn)
    logging.info(f"Found {len(results)} upstream entities")

    ping_node = None
    ping_platform = None
    stable_urn = None

    sorted_results = sorted(results, key=lambda r: r["degree"], reverse=True)

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
            break

    if not ping_node:
        logging.warning(
            f"No ping node found in lineage for {project}.{dataset}.{table}"
        )
        return None

    ping_name = ping_node["name"]
    logging.info(f"Found ping: {ping_name} (platform: {ping_platform})")

    # For legacy telemetry, find the closest stable BQ table in the chain
    # Chain: derived → ... → stable → live → ping
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
            best = max(stable_candidates, key=lambda r: r["degree"])
            stable_urn = best["entity"]["urn"]
            logging.info(f"Found stable table: {best['entity']['name']}")

    return {
        "ping_platform": ping_platform,
        "source_ping": ping_name,
        "stable_urn": stable_urn,
    }


# --- Probe definition fetchers ---


def infer_glean_app(dataset):
    """Infer Glean Dictionary app name from a BQ dataset name.

    Strips known suffixes to get the app slug used in the Glean Dictionary,
    e.g. firefox_desktop_derived → firefox_desktop.
    """
    for suffix in ("_derived", "_stable", "_live"):
        if dataset.endswith(suffix):
            return dataset[: -len(suffix)]
    return dataset


def fetch_glean_probes(
    _ping_platform, source_ping, stable_urn=None, dataset=None
):  # noqa: stable_urn unused (shared signature)
    """Fetch probe definitions for a Glean ping from the Glean Dictionary JSON API."""
    app = infer_glean_app(dataset or "")
    url = GLEAN_DICT_URL.format(app=app, ping=source_ping)
    logging.info(f"Fetching Glean probes from {url}")
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        logging.warning(f"Glean Dictionary entry not found: {url}")
        return []
    response.raise_for_status()
    data = response.json()
    probes = []
    for metric in data.get("metrics", []):
        probes.append(
            {
                "probe_name": metric.get("name"),
                "probe_description": metric.get("description"),
                "probe_type": metric.get("type"),
                "probe_raw": json.dumps(metric),
                "data_sensitivity": metric.get("data_sensitivity") or [],
                "send_in_pings": metric.get("send_in_pings") or [],
                "tags": (metric.get("metadata") or {}).get("tags") or [],
            }
        )
    logging.info(f"Found {len(probes)} Glean probes for {app}/{source_ping}")
    return probes


def fetch_legacy_probes(
    _ping_platform, source_ping, stable_urn, dataset=None
):  # noqa: dataset unused (shared signature)
    """Fetch probe definitions from the schema of a Legacy Telemetry stable table."""
    logging.info(f"Fetching schema fields from {stable_urn}")
    fields = get_schema_fields(stable_urn)
    probes = []
    for field in fields:
        probes.append(
            {
                "probe_name": field.get("fieldPath"),
                "probe_description": field.get("description"),
                "probe_type": field.get("nativeDataType"),
                "probe_raw": json.dumps(field),
                "data_sensitivity": [],
                "send_in_pings": [],
                "tags": [],
            }
        )
    logging.info(f"Found {len(probes)} legacy probe fields for {source_ping}")
    return probes


PROBE_FETCHERS = {
    "glean": fetch_glean_probes,
    "legacy_telemetry": fetch_legacy_probes,
}


# --- BQ helpers ---


def bq_delete(bq_client, table, where_clause):
    """Delete rows from a BQ table matching a WHERE clause, ignoring NotFound."""
    try:
        bq_client.query(f"DELETE FROM `{table}` WHERE {where_clause}").result()
    except NotFound:
        pass  # Table doesn't exist yet on first run


def bq_insert(bq_client, records, dest_table, schema):
    """Append records to a BQ table, creating it if needed."""
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq_client.load_table_from_json(records, dest_table, job_config=job_config)
    job.result()


# --- Step 1: Lineage resolution → MAPPING_TABLE ---


def get_already_mapped_tables(bq_client):
    """Return set of (project, dataset, table) already in the mapping table."""
    try:
        query = f"SELECT source_project, source_dataset, source_table FROM `{MAPPING_TABLE}`"
        return {
            (r.source_project, r.source_dataset, r.source_table)
            for r in bq_client.query(query).result()
        }
    except NotFound:
        return set()


def resolve_lineage_for_tables(bq_client, tables):
    """Resolve lineage for tables not yet in the mapping table and write results."""
    already_mapped = get_already_mapped_tables(bq_client)
    pending = [(p, d, t) for p, d, t in tables if (p, d, t) not in already_mapped]
    logging.info(
        f"{len(pending)} tables need lineage resolution (skipping {len(tables) - len(pending)} already mapped)"
    )

    now = datetime.now(timezone.utc).isoformat()
    for project, dataset, table in pending:
        logging.info(f"  Resolving {dataset}.{table}")
        try:
            ping_info = resolve_ping(project, dataset, table)
        except Exception as e:
            logging.error(f"  Lineage failed for {dataset}.{table}: {e}")
            ping_info = None

        record = {
            "source_project": project,
            "source_dataset": dataset,
            "source_table": table,
            "source_ping": ping_info["source_ping"] if ping_info else None,
            "ping_platform": ping_info["ping_platform"] if ping_info else None,
            "stable_urn": ping_info["stable_urn"] if ping_info else None,
            "processed_at": now,
        }
        bq_insert(bq_client, [record], MAPPING_TABLE, MAPPING_SCHEMA)


# --- Step 2: Probe fetch → PROBE_TABLE ---


def get_unique_pings(bq_client):
    """Return unique (ping_platform, source_ping, stable_urn, sample_dataset) from mapping table."""
    query = f"""
        SELECT
            ping_platform,
            source_ping,
            ANY_VALUE(stable_urn) AS stable_urn,
            ANY_VALUE(source_dataset) AS sample_dataset
        FROM `{MAPPING_TABLE}`
        WHERE source_ping IS NOT NULL
        GROUP BY ping_platform, source_ping
    """
    return [
        {
            "ping_platform": r.ping_platform,
            "source_ping": r.source_ping,
            "stable_urn": r.stable_urn,
            "sample_dataset": r.sample_dataset,
        }
        for r in bq_client.query(query).result()
    ]


def get_already_fetched_pings(bq_client):
    """Return set of (ping_platform, source_ping) already in the probe table."""
    try:
        query = f"SELECT DISTINCT ping_platform, source_ping FROM `{PROBE_TABLE}`"
        return {
            (r.ping_platform, r.source_ping) for r in bq_client.query(query).result()
        }
    except NotFound:
        return set()


def fetch_probes_for_pings(bq_client):
    """Fetch probe definitions for unique pings not yet in the probe table."""
    unique_pings = get_unique_pings(bq_client)
    already_fetched = get_already_fetched_pings(bq_client)
    pending = [
        p
        for p in unique_pings
        if (p["ping_platform"], p["source_ping"]) not in already_fetched
    ]
    logging.info(
        f"{len(pending)} pings need probe fetch (skipping {len(unique_pings) - len(pending)} already fetched)"
    )

    now = datetime.now(timezone.utc).isoformat()
    for ping in pending:
        platform = ping["ping_platform"]
        source_ping = ping["source_ping"]
        stable_urn = ping["stable_urn"]
        dataset = ping["sample_dataset"]

        logging.info(f"  Fetching probes for {platform}/{source_ping}")
        try:
            fetcher = PROBE_FETCHERS[platform]
            probes = fetcher(
                platform, source_ping, stable_urn=stable_urn, dataset=dataset
            )
        except Exception as e:
            logging.error(f"  Probe fetch failed for {platform}/{source_ping}: {e}")
            continue

        if not probes:
            logging.warning(f"  No probes found for {platform}/{source_ping}")
            continue

        records = [
            {
                "ping_platform": platform,
                "source_ping": source_ping,
                "probe_name": probe["probe_name"],
                "probe_description": probe["probe_description"],
                "probe_type": probe["probe_type"],
                "probe_raw": probe["probe_raw"],
                "data_sensitivity": probe["data_sensitivity"],
                "send_in_pings": probe["send_in_pings"],
                "tags": probe["tags"],
                "processed_at": now,
            }
            for probe in probes
        ]
        bq_insert(bq_client, records, PROBE_TABLE, PROBE_SCHEMA)
        logging.info(f"  Saved {len(records)} probes for {platform}/{source_ping}")


# --- Main ---


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(
        description="Resolve lineage and fetch probe definitions for Phase 1 profiled tables."
    )
    parser.add_argument(
        "--table",
        help="Fully qualified BQ table (project.dataset.table). If omitted, processes all Phase 1 tables.",
    )
    return parser.parse_args()


def get_phase1_tables(bq_client):
    """Return distinct (project, dataset, table) tuples from the Phase 1 profiling table."""
    query = f"""
        SELECT DISTINCT source_project, source_dataset, source_table
        FROM `{PHASE1_TABLE}`
        ORDER BY source_dataset, source_table
    """
    return [
        (row.source_project, row.source_dataset, row.source_table)
        for row in bq_client.query(query).result()
    ]


def main():
    """Resolve lineage and fetch probe definitions for all Phase 1 profiled tables."""
    args = parse_args()
    bq_client = bigquery.Client(project=DEST_PROJECT)

    if args.table:
        project, dataset, table = args.table.split(".")
        tables = [(project, dataset, table)]
    else:
        tables = get_phase1_tables(bq_client)
        logging.info(f"Found {len(tables)} Phase 1 tables")

    logging.info("=== Step 1: Lineage resolution ===")
    resolve_lineage_for_tables(bq_client, tables)

    logging.info("=== Step 2: Probe fetch ===")
    fetch_probes_for_pings(bq_client)


if __name__ == "__main__":
    main()
