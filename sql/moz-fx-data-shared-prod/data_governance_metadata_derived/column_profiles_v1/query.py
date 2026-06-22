#!/usr/bin/env python3

"""Profile columns of base tables across one or more source datasets.

Writes one row per profiled column to a single destination table
(``column_profiles_v1``) in ``data_governance_metadata_derived``, tagged with
``source_dataset``/``source_table``. Profiles every base table in each
``--source-datasets`` entry (default ``telemetry_derived,telemetry``), or a
``--tables`` subset. A single run accumulates all rows and overwrites its own
``profiled_at`` date partition in one load, so the job is idempotent.

The profiling logic is vendored verbatim from
``data-shared-llm-agents/scripts/bq_profiler.py`` so this script depends only on
``google.cloud.bigquery`` plus the Python standard library — the bigquery-etl
runner image installs dependencies with ``--no-deps``.
"""

import argparse
import datetime
import json
import logging
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_BQ_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")

_HIGH_CARDINALITY_THRESHOLD = 50
_SKIP_DISTINCT_TYPES = {"STRUCT", "JSON", "RECORD", "ARRAY", "INTERVAL"}
_MAX_PROFILABLE_COLUMNS = 500
_TIER3_COLUMNS = {"metrics"}

_SKIP_TABLE_TYPES = {"VIEW", "MATERIALIZED VIEW"}

# Profile errors that are expected/benign (the table just isn't profilable) vs.
# hard failures (permissions, quota, unexpected). If every table hard-fails the
# task should fail rather than silently writing nothing.
_BENIGN_ERROR_MARKERS = ("is empty", "not found", "exceeds column limit")

# Intentionally narrow: only leaf names that are unambiguously PII regardless of
# table context. Suffix patterns like _id are excluded — they match too many
# non-PII columns (campaign_id, etc).
_PII_LEAF_NAMES = {
    "account",
    "account_id",
    "email",
    "first_name",
    "last_name",
    "full_name",
    "fxa",
    "fxa_id",
    "phone",
    "phone_number",
    "address",
    "ip",
    "ip_address",
    "password",
    "date_of_birth",
    "dob",
    "birthdate",
}
_PII_LEAF_SUFFIXES = ("_email",)

# Explicit destination schema, matching schema.yaml. profiled_at is a DATE here
# (the partition key) rather than the TIMESTAMP used by the original tool.
_COLUMN_PROFILES_SCHEMA = [
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("null_rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("distinct_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("is_high_cardinality", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("example_value", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "values",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("frequency", "INTEGER", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("column_tier", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("profiled_at", "DATE", mode="REQUIRED"),
]


def _validate_bq_identifier(value: str, name: str) -> str:
    """Validate a BigQuery resource name before embedding it in a FROM clause.

    Raises ValueError if the value could inject SQL via backtick-quoted
    identifiers. Valid characters: letters, digits, underscores, hyphens (no
    spaces or quotes).
    """
    if not _BQ_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid BigQuery identifier for {name}: {value!r}. "
            "Must match [A-Za-z0-9][A-Za-z0-9_-]*."
        )
    return value


def _json_default(obj: Any) -> str:
    if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
        return obj.isoformat()
    return str(obj)


def is_pii_column(field_path: str) -> bool:
    """Return True if the leaf segment matches a known PII name or suffix."""
    leaf = field_path.split(".")[-1].lower()
    return leaf in _PII_LEAF_NAMES or leaf.endswith(_PII_LEAF_SUFFIXES)


def get_columns(client: bigquery.Client, table: str) -> list[tuple[str, str, str]]:
    """Return (field_path, data_type, tier) tuples for all columns in the table.

    Tiers:
      scalar        — top-level non-STRUCT column
      leaf          — leaf field inside a simple (non-repeated) STRUCT
      nested_leaf   — leaf field inside a REPEATED STRUCT (profiled via UNNEST)
      scalar_array  — simple ARRAY<scalar> column (described from context)
      undocumented  — known wide columns (e.g. metrics) or nested ARRAY-in-ARRAY
      pii_suppressed — column name matches a known PII pattern; never scanned
    """
    project, dataset, table_name = table.split(".")
    _validate_bq_identifier(project, "project")
    _validate_bq_identifier(dataset, "dataset")
    query = f"""
        SELECT
            fp.field_path,
            fp.data_type,
            fp.column_name,
            c.data_type AS top_level_data_type
        FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS AS fp
        JOIN `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS AS c
            ON fp.table_name = c.table_name AND fp.column_name = c.column_name
        WHERE fp.table_name = @table_name
        ORDER BY c.ordinal_position, fp.field_path
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    array_paths = {
        row.field_path for row in rows if row.data_type.upper().startswith("ARRAY")
    }

    results = []
    for row in rows:
        field_path = row.field_path
        data_type = row.data_type
        column_name = row.column_name
        top_level_type = row.top_level_data_type

        if column_name in _TIER3_COLUMNS:
            if field_path == column_name:
                results.append((field_path, top_level_type, "undocumented"))
            continue

        is_repeated_parent = top_level_type.upper().startswith("ARRAY")

        if is_repeated_parent:
            if field_path == column_name:
                if "STRUCT" not in top_level_type.upper():
                    if is_pii_column(field_path):
                        results.append((field_path, top_level_type, "pii_suppressed"))
                    else:
                        results.append((field_path, top_level_type, "scalar_array"))
                else:
                    results.append((field_path, top_level_type, "undocumented"))
                continue

            has_nested_array_ancestor = False
            if "." in field_path:
                parts = field_path.split(".")
                ancestors = [".".join(parts[:i]) for i in range(2, len(parts))]
                has_nested_array_ancestor = any(a in array_paths for a in ancestors)

            if has_nested_array_ancestor:
                continue

            if data_type.upper().startswith("STRUCT") or data_type.upper().startswith(
                "ARRAY"
            ):
                continue

            if is_pii_column(field_path):
                results.append((field_path, data_type, "pii_suppressed"))
                continue

            results.append((field_path, data_type, "nested_leaf"))
            continue

        if "." in field_path:
            parts = field_path.split(".")
            ancestors = [".".join(parts[:i]) for i in range(1, len(parts))]
            if any(a in array_paths for a in ancestors):
                continue

        if data_type.upper().startswith("STRUCT"):
            continue

        if is_pii_column(field_path):
            results.append((field_path, data_type, "pii_suppressed"))
            continue

        tier = "leaf" if "." in field_path else "scalar"
        results.append((field_path, data_type, tier))

    return results


def field_path_to_sql(field_path: str) -> str:
    """Convert dot-notation field path to backtick-quoted SQL (e.g. `a`.`b`)."""
    return ".".join(f"`{part}`" for part in field_path.split("."))


def field_path_to_alias(field_path: str) -> str:
    """Convert dot-notation field path to a flat SQL alias (dots → __)."""
    return field_path.replace(".", "__")


def _sample_bucket(run_date: str, table: str) -> int:
    """Deterministic 1% sample bucket (1-99) for a (run_date, table) pair.

    Seeding by run_date makes a given --date reproducible; including the table
    name spreads buckets across tables instead of always sampling the same one.
    """
    return random.Random(f"{run_date}:{table}").randint(1, 99)


def build_profile_query(
    table: str,
    columns: list[tuple[str, str, str]],
    partition_filter: str | None = None,
    sample_id: int | None = None,
    tablesample: bool = False,
) -> str:
    """Build a single query that profiles every profilable column in one pass.

    Profiles scalar and leaf tiers only. nested_leaf fields are profiled
    separately via build_nested_profile_queries (UNNEST-based).

    Sampling is decided by the caller: a non-None sample_id adds a
    ``sample_id = <bucket>`` filter (deterministic per run date), while
    tablesample adds TABLESAMPLE SYSTEM (1 PERCENT) for tables whose sample_id
    column is unpopulated. The two are mutually exclusive.
    """
    excluded_tiers = {"undocumented", "pii_suppressed", "nested_leaf", "scalar_array"}
    profilable = [(fp, dt) for fp, dt, tier in columns if tier not in excluded_tiers]

    col_stats = []
    for field_path, data_type in profilable:
        alias = field_path_to_alias(field_path)
        sql_ref = field_path_to_sql(field_path)
        col_stats.append(f"COUNTIF({sql_ref} IS NULL) AS `{alias}__nulls`")
        if any(t in data_type.upper() for t in _SKIP_DISTINCT_TYPES):
            col_stats.append(f"NULL AS `{alias}__distinct`")
        else:
            col_stats.append(f"APPROX_COUNT_DISTINCT({sql_ref}) AS `{alias}__distinct`")
            col_stats.append(
                f"APPROX_TOP_COUNT({sql_ref}, 50) AS `{alias}__top_values`"
            )
        col_stats.append(f"ANY_VALUE({sql_ref}) AS `{alias}__example`")

    select = ",\n        ".join(["COUNT(*) AS total_rows"] + col_stats)

    from_clause = f"`{table}`"
    if tablesample:
        from_clause += " TABLESAMPLE SYSTEM (1 PERCENT)"

    where_clauses = []
    if partition_filter:
        where_clauses.append(partition_filter)
    if sample_id is not None:
        where_clauses.append(f"`sample_id` = {sample_id}")

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return f"SELECT {select} FROM {from_clause} {where}"


def build_nested_profile_queries(
    table: str,
    columns: list[tuple[str, str, str]],
    partition_filter: str | None = None,
    sample_id: int | None = None,
    tablesample: bool = False,
) -> list[tuple[str, str, list[tuple[str, str]]]]:
    """Build one UNNEST-based profiling query per REPEATED parent column.

    Each query profiles all nested_leaf fields under a single REPEATED parent.
    Returns a list of (parent_column, query_sql, nested_fields) tuples.
    nested_fields is [(field_path, data_type), ...] for result parsing.

    Sampling mirrors build_profile_query: caller-supplied sample_id bucket, or
    TABLESAMPLE when the table's sample_id column is unpopulated.
    """
    nested_leaves = [(fp, dt) for fp, dt, tier in columns if tier == "nested_leaf"]
    if not nested_leaves:
        return []

    by_parent: dict[str, list[tuple[str, str]]] = {}
    for field_path, data_type in nested_leaves:
        parent = field_path.split(".")[0]
        by_parent.setdefault(parent, []).append((field_path, data_type))

    results = []
    for parent, fields in by_parent.items():
        unnest_alias = f"_{parent}_elem"

        col_stats = []
        for field_path, data_type in fields:
            suffix = field_path[len(parent) + 1 :]
            sql_ref = ".".join(f"`{p}`" for p in [unnest_alias] + suffix.split("."))
            alias = field_path_to_alias(field_path)

            col_stats.append(f"COUNTIF({sql_ref} IS NULL) AS `{alias}__nulls`")
            if any(t in data_type.upper() for t in _SKIP_DISTINCT_TYPES):
                col_stats.append(f"NULL AS `{alias}__distinct`")
            else:
                col_stats.append(
                    f"APPROX_COUNT_DISTINCT({sql_ref}) AS `{alias}__distinct`"
                )
                col_stats.append(
                    f"APPROX_TOP_COUNT({sql_ref}, 50) AS `{alias}__top_values`"
                )
            col_stats.append(f"ANY_VALUE({sql_ref}) AS `{alias}__example`")

        select = ",\n        ".join(["COUNT(*) AS total_rows"] + col_stats)

        from_clause = f"`{table}`"
        if tablesample:
            from_clause += " TABLESAMPLE SYSTEM (1 PERCENT)"

        where_clauses = []
        if partition_filter:
            where_clauses.append(partition_filter)
        if sample_id is not None:
            where_clauses.append(f"`sample_id` = {sample_id}")

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = (
            f"SELECT {select} "
            f"FROM {from_clause}, UNNEST(`{parent}`) AS `{unnest_alias}` "
            f"{where}"
        )
        results.append((parent, query, fields))

    return results


def get_partition_filter(
    client: bigquery.Client, table: str, run_date: str
) -> str | None:
    """Return a WHERE clause scoped to the partition 7 days before run_date.

    Anchored to run_date (the --date / profiled_at partition being written), NOT
    CURRENT_DATE, so the scanned source data corresponds to the partition
    produced and reruns/backfills of a given date are reproducible. run_date
    must be a validated ISO date (YYYY-MM-DD). Returns None for unpartitioned
    tables.
    """
    table_ref = client.get_table(table)
    tp = table_ref.time_partitioning
    if tp is None:
        return None
    field = tp.field
    if field:
        return f"DATE(`{field}`) = DATE_SUB(DATE('{run_date}'), INTERVAL 7 DAY)"
    return f"_PARTITIONDATE = DATE_SUB(DATE('{run_date}'), INTERVAL 7 DAY)"


def sample_id_is_populated(
    client: bigquery.Client, table: str, partition_filter: str | None
) -> bool:
    """Return True if the table's sample_id column holds any non-NULL value.

    Tables sourced from server-side Glean pings carry a sample_id column that is
    never populated (these pings carry no client_id, which sample_id is derived
    from), so it is entirely NULL. A ``sample_id = bucket`` sample would then
    match no rows and the table would profile to zero columns, so the caller
    uses this to decide whether to fall back to TABLESAMPLE. Checks the same
    slice the profiler scans (the partition_filter), short-circuiting via
    LIMIT 1 so the common populated case stays cheap.
    """
    where_clauses = ["`sample_id` IS NOT NULL"]
    if partition_filter:
        where_clauses.append(partition_filter)
    query = (
        f"SELECT EXISTS(SELECT 1 FROM `{table}` "
        f"WHERE {' AND '.join(where_clauses)} LIMIT 1) AS populated"
    )
    return bool(list(client.query(query).result())[0].populated)


def _dataset_region(client: bigquery.Client, project: str, dataset: str) -> str:
    """Return the INFORMATION_SCHEMA region qualifier (e.g. 'region-us') for a dataset.

    TABLE_STORAGE is only queryable at region scope, so the region must match the
    dataset's actual location rather than being hardcoded to US.
    """
    location = client.get_dataset(f"{project}.{dataset}").location
    return f"region-{location.lower()}"


def check_table_has_rows(
    client: bigquery.Client, table: str, region: str
) -> tuple[int, float]:
    """Return (total_rows, total_gb). (-1, 0) if not found, (0, 0) if empty."""
    project, dataset, table_name = table.split(".")
    _validate_bq_identifier(project, "project")
    _validate_bq_identifier(dataset, "dataset")
    query = f"""
        SELECT total_rows, total_logical_bytes
        FROM `{project}.{region}`.INFORMATION_SCHEMA.TABLE_STORAGE
        WHERE project_id = @project
        AND table_schema = @dataset
        AND table_name = @table_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project", "STRING", project),
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return -1, 0.0
    return rows[0].total_rows, rows[0].total_logical_bytes / 1e9


def _parse_profile_row(
    row: Any,
    total_rows: int,
    field_path: str,
    data_type: str,
    tier: str,
) -> dict[str, Any]:
    """Extract stats for one field from a profiling query result row."""
    alias = field_path_to_alias(field_path)
    null_count = getattr(row, f"{alias}__nulls")
    distinct_count = getattr(row, f"{alias}__distinct", None)
    example = getattr(row, f"{alias}__example", None)
    null_rate = round(null_count / total_rows * 100, 1) if total_rows > 0 else 0.0

    top_values_raw = getattr(row, f"{alias}__top_values", []) or []
    values = [[item["value"], item["count"]] for item in top_values_raw]

    is_high_cardinality = (
        distinct_count is None or distinct_count > _HIGH_CARDINALITY_THRESHOLD
    )

    entry: dict[str, Any] = {
        "data_type": data_type,
        "tier": tier,
        "null_rate": null_rate,
        "distinct_count": distinct_count,
        "is_high_cardinality": is_high_cardinality,
    }
    if is_high_cardinality:
        entry["example"] = str(example) if example is not None else None
        entry["values"] = values[:5]
    else:
        entry["values"] = values

    return entry


def _run_profile_query(
    client: bigquery.Client,
    table: str,
    columns: list[tuple[str, str, str]],
    run_date: str,
) -> tuple[dict[str, Any], float]:
    """Execute the profiling query and return (column_stats_dict, gb_scanned)."""
    partition_filter = get_partition_filter(client, table, run_date)

    # Decide how to sample. Prefer the deterministic sample_id bucket, but only
    # when that column is actually populated; tables sourced from server-side
    # Glean pings carry an all-NULL sample_id, where `sample_id = bucket` would
    # scan zero rows. Fall back to TABLESAMPLE in that case.
    sample_id: int | None = None
    tablesample = False
    if any(fp == "sample_id" for fp, _, _ in columns):
        if sample_id_is_populated(client, table, partition_filter):
            sample_id = _sample_bucket(run_date, table)
        else:
            tablesample = True
            logger.info(
                "sample_id present but unpopulated for %s; "
                "falling back to TABLESAMPLE SYSTEM (1 PERCENT).",
                table,
            )
    query = build_profile_query(
        table, columns, partition_filter, sample_id, tablesample
    )

    job = client.query(query)
    rows = list(job.result())
    gb_scanned = job.total_bytes_processed / 1e9
    total_rows = rows[0].total_rows

    # TABLESAMPLE SYSTEM samples whole storage blocks, so on a small table a 1%
    # sample can resolve to zero rows even when the partition has data. Retry once
    # with a full (unsampled) partition scan: a small partition is cheap to scan
    # in full and yields exact stats, while a genuinely empty partition still
    # returns zero. Clearing tablesample also drops sampling from the nested
    # queries built below.
    if total_rows == 0 and tablesample:
        tablesample = False
        query = build_profile_query(
            table, columns, partition_filter, sample_id, tablesample
        )
        job = client.query(query)
        rows = list(job.result())
        gb_scanned += job.total_bytes_processed / 1e9
        total_rows = rows[0].total_rows

    row = rows[0]
    results: dict[str, Any] = {}

    # The scanned slice (recent partition + 1% sample) can be empty even when the
    # table has data. Emit no column stats rather than meaningless all-zero rows.
    if total_rows == 0:
        logger.info("Scanned slice empty for %s; no column stats emitted.", table)
        return {}, gb_scanned

    for field_path, data_type, tier in columns:
        if tier == "undocumented":
            results[field_path] = {"data_type": data_type, "tier": "undocumented"}
            continue
        if tier == "pii_suppressed":
            results[field_path] = {"data_type": data_type, "tier": "pii_suppressed"}
            continue
        if tier == "scalar_array":
            results[field_path] = {"data_type": data_type, "tier": "scalar_array"}
            continue
        if tier == "nested_leaf":
            continue

        results[field_path] = _parse_profile_row(
            row, total_rows, field_path, data_type, tier
        )

    nested_queries = build_nested_profile_queries(
        table, columns, partition_filter, sample_id, tablesample
    )
    for _parent, nested_sql, nested_fields in nested_queries:
        try:
            nested_job = client.query(nested_sql)
            nested_rows = list(nested_job.result())
            gb_scanned += nested_job.total_bytes_processed / 1e9
            nested_row = nested_rows[0]
            nested_total = nested_row.total_rows
            if nested_total == 0:
                # Empty UNNEST slice — record the fields without misleading
                # all-zero stats, same as the top-level empty-scan guard.
                logger.info(
                    "Nested slice empty for %s.%s; no stats emitted.", table, _parent
                )
                for field_path, data_type in nested_fields:
                    results[field_path] = {
                        "data_type": data_type,
                        "tier": "nested_leaf",
                    }
                continue
            for field_path, data_type in nested_fields:
                results[field_path] = _parse_profile_row(
                    nested_row, nested_total, field_path, data_type, "nested_leaf"
                )
        except Exception as e:
            logger.warning(f"Nested profiling failed for {_parent}: {e}")
            for field_path, data_type in nested_fields:
                results[field_path] = {"data_type": data_type, "tier": "nested_leaf"}

    return results, gb_scanned


def list_bq_tables(project: str, dataset: str) -> str:
    """List all tables in a BigQuery dataset with their table types.

    Returns JSON: {"tables": [{"table": "...", "table_type": "..."}, ...]}
    or {"error": "..."} if the dataset is inaccessible.
    """
    try:
        _validate_bq_identifier(project, "project")
        _validate_bq_identifier(dataset, "dataset")
        query = f"""
            SELECT table_name, table_type
            FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLES
            ORDER BY table_name
        """
        client = bigquery.Client(project=project)
        rows = list(client.query(query).result())
        return json.dumps(
            {
                "tables": [
                    {"table": row.table_name, "table_type": row.table_type}
                    for row in rows
                ]
            }
        )
    except Exception as e:
        logger.error(f"Failed to list tables in {project}.{dataset}: {e}")
        return json.dumps({"error": str(e)})


def profile_bq_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    run_date: str,
    region: str,
) -> str:
    """Profile every column in a single BigQuery table; return stats as JSON.

    Samples 1% via the sample_id bucket when that column is present and
    populated, falling back to TABLESAMPLE when sample_id is all-NULL.
    Applies a partition filter scoped to 7 days before run_date when a date
    partition column is detected, so the scanned data matches the profiled_at
    partition. Fields inside REPEATED structs are profiled via UNNEST with the
    same sampling. Simple ARRAY<scalar> columns are listed as tier
    "scalar_array" with no stats.
    Fields under _TIER3_COLUMNS (e.g. metrics) remain tier "undocumented".

    Returns a JSON object with column stats, or {"error": "..."} if the table
    is empty, not found, or inaccessible.
    """
    fully_qualified = f"{project}.{dataset}.{table}"
    try:
        total_rows, _ = check_table_has_rows(client, fully_qualified, region)
        if total_rows == -1:
            return json.dumps(
                {"error": f"Table {fully_qualified} not found in INFORMATION_SCHEMA."}
            )
        if total_rows == 0:
            return json.dumps({"error": f"Table {fully_qualified} is empty."})

        columns = get_columns(client, fully_qualified)
        if len(columns) > _MAX_PROFILABLE_COLUMNS:
            return json.dumps(
                {
                    "error": (
                        f"Table {fully_qualified} has {len(columns)} columns, "
                        f"exceeds column limit of {_MAX_PROFILABLE_COLUMNS}."
                    ),
                }
            )
        column_stats, gb_scanned = _run_profile_query(
            client, fully_qualified, columns, run_date
        )

        return json.dumps(
            {
                "table": fully_qualified,
                "total_rows": total_rows,
                "gb_scanned": round(gb_scanned, 4),
                "columns": column_stats,
            },
            default=_json_default,
        )
    except Exception as e:
        logger.error(f"Failed to profile {fully_qualified}: {e}")
        return json.dumps({"error": str(e)})


def build_rows(profile: dict[str, Any], profiled_at: str) -> list[dict[str, Any]]:
    """Turn one profile_bq_table result into per-column destination rows.

    Equivalent to the schema_enricher bq_writer.write_column_profiles loop,
    except profiled_at is the run DATE (the partition key) rather than the
    current timestamp.
    """
    table_fq = profile.get("table", "")
    parts = table_fq.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Cannot parse project.dataset.table from profile: {table_fq!r}"
        )
    source_project, source_dataset, source_table = parts

    records: list[dict[str, Any]] = []
    for column_name, stats in profile.get("columns", {}).items():
        values_raw = stats.get("values", [])
        values_bq = [
            {"value": str(v) if v is not None else None, "frequency": c}
            for v, c in values_raw
        ]
        records.append(
            {
                "source_project": source_project,
                "source_dataset": source_dataset,
                "source_table": source_table,
                "column_name": column_name,
                "data_type": stats.get("data_type", ""),
                "null_rate": stats.get("null_rate"),
                "distinct_count": stats.get("distinct_count"),
                "is_high_cardinality": stats.get("is_high_cardinality"),
                "example_value": stats.get("example"),
                "values": values_bq,
                "column_tier": stats.get("tier", "scalar"),
                "profiled_at": profiled_at,
            }
        )
    return records


def save_profiles(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    date: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
) -> None:
    """Overwrite the run's date partition with all profiled column rows."""
    job_config = bigquery.LoadJobConfig()
    job_config.schema = _COLUMN_PROFILES_SCHEMA
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="profiled_at"
    )
    # Must match the destination table's clustering, otherwise loads into a
    # clustered table fail with "Incompatible table partitioning specification".
    job_config.clustering_fields = ["source_dataset", "source_table", "column_name"]

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
        "--date", default="", help="Run date (YYYY-MM-DD); the partition key."
    )
    parser.add_argument("--source-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--source-datasets",
        default="telemetry_derived,telemetry",
        help=(
            "Source datasets to profile, comma-separated as a single value "
            "(default: 'telemetry_derived,telemetry'). Passed as one string so it "
            "can be driven by an Airflow dag_run.conf override."
        ),
    )
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument("--destination-table", default="column_profiles_v1")
    parser.add_argument(
        "--tables",
        nargs="*",
        help=(
            "Optional subset of source table names to profile. Requires exactly "
            "one --source-datasets entry; default profiles all base tables in "
            "each dataset."
        ),
    )
    parser.add_argument("--max-workers", type=int, default=3)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Discover and report the tables that would be profiled, then exit "
            "without running profiling queries or writing to BigQuery."
        ),
    )
    args = parser.parse_args()
    if not args.date:
        parser.error(
            "--date is required (YYYY-MM-DD). Pass it directly on the command "
            'line, or — in an Airflow manual run — via the trigger config ("date"); '
            "scheduled/backfill runs derive it from the run's interval-end date "
            "automatically."
        )
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be an ISO date (YYYY-MM-DD); got {args.date!r}")
    # A single comma-separated string (so it can be driven by one CLI value or an
    # Airflow dag_run.conf override); normalize to a list. Commas are the only
    # separator — a space-separated value is rejected as an invalid dataset name.
    args.source_datasets = [
        d.strip() for d in args.source_datasets.split(",") if d.strip()
    ]
    if not args.source_datasets:
        parser.error(
            "--source-datasets must list at least one dataset (comma-separated). "
            "Pass it directly on the command line, or — in an Airflow manual run — "
            'via the trigger config ("source_datasets"); scheduled/backfill runs '
            "use the default set automatically."
        )
    invalid = [d for d in args.source_datasets if not _BQ_IDENTIFIER_RE.match(d)]
    if invalid:
        parser.error(
            "--source-datasets must be comma-separated dataset names; "
            f"invalid entr{'y' if len(invalid) == 1 else 'ies'}: {invalid}"
        )
    if args.tables and len(args.source_datasets) > 1:
        parser.error(
            "--tables requires exactly one --source-datasets entry "
            "(table names are ambiguous across datasets); got "
            f"{len(args.source_datasets)}: {args.source_datasets}"
        )
    return args


def _discover_base_tables(
    project: str, dataset: str, table_filter: set[str] | None
) -> list[str]:
    """List profilable base tables in a dataset (skipping views), honoring filter."""
    tables_result = json.loads(list_bq_tables(project, dataset))
    if "error" in tables_result:
        logger.warning(
            "Skipping dataset %s.%s — table discovery failed: %s",
            project,
            dataset,
            tables_result["error"],
        )
        return []
    base = [
        t["table"]
        for t in tables_result["tables"]
        if t["table_type"] not in _SKIP_TABLE_TYPES
    ]
    if table_filter:
        base = [t for t in base if t in table_filter]
    return base


def main() -> None:
    """Entrypoint for the column profiler job."""
    args = parse_args()

    client = bigquery.Client(project=args.source_project)
    table_filter = set(args.tables) if args.tables else None

    # Build (dataset, table) work items across every requested dataset, and
    # resolve each dataset's region (TABLE_STORAGE is queryable only per region).
    work: list[tuple[str, str]] = []
    dataset_regions: dict[str, str] = {}
    for dataset in args.source_datasets:
        try:
            dataset_regions[dataset] = _dataset_region(
                client, args.source_project, dataset
            )
        except Exception as e:
            logger.warning(
                "Skipping dataset %s.%s — could not resolve region: %s",
                args.source_project,
                dataset,
                e,
            )
            continue
        tables = _discover_base_tables(args.source_project, dataset, table_filter)
        logger.info(
            "Discovered %d base table(s) to profile in %s.%s",
            len(tables),
            args.source_project,
            dataset,
        )
        work.extend((dataset, t) for t in tables)

    if table_filter:
        missing = sorted(table_filter - {t for _, t in work})
        if missing:
            logger.warning(
                "Requested --tables not found in %s: %s",
                args.source_datasets,
                missing,
            )

    if not work:
        logger.warning(
            "No tables to profile across datasets %s; partition left unchanged.",
            args.source_datasets,
        )
        return

    if args.dry_run:
        logger.info(
            "DRY RUN — would profile %d table(s) across %d dataset(s) and "
            "overwrite %s.%s.%s$%s. No profiling queries or writes performed.",
            len(work),
            len(args.source_datasets),
            args.destination_project,
            args.destination_dataset,
            args.destination_table,
            args.date.replace("-", ""),
        )
        for dataset, table_name in work:
            logger.info("  would profile %s.%s", dataset, table_name)
        return

    def _profile_one(item: tuple[str, str]) -> tuple[tuple[str, str], str]:
        dataset, table_name = item
        return item, profile_bq_table(
            client,
            args.source_project,
            dataset,
            table_name,
            args.date,
            dataset_regions[dataset],
        )

    rows: list[dict[str, Any]] = []
    profiled = 0
    hard_failures = 0
    total_gb_scanned = 0.0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(_profile_one, item): item for item in work}
        for future in as_completed(futures):
            dataset, table_name = futures[future]
            try:
                _, profile_json = future.result()
            except Exception as e:
                logger.warning("Profiling raised for %s.%s: %s", dataset, table_name, e)
                hard_failures += 1
                continue

            profile = json.loads(profile_json)
            if "error" in profile:
                msg = profile["error"]
                if any(marker in msg for marker in _BENIGN_ERROR_MARKERS):
                    logger.info("Skipping %s.%s: %s", dataset, table_name, msg)
                else:
                    logger.warning(
                        "Profiling failed for %s.%s: %s", dataset, table_name, msg
                    )
                    hard_failures += 1
                continue

            profiled += 1
            total_gb_scanned += profile.get("gb_scanned", 0.0)
            table_rows = build_rows(profile, args.date)
            logger.info("%s.%s → %d column rows", dataset, table_name, len(table_rows))
            rows.extend(table_rows)

    # A dataset-wide outage (permissions/quota) would otherwise log warnings and
    # exit 0, hiding the failure from triage. Fail loudly when nothing profiled
    # AND at least one table hard-failed (an all-empty/benign dataset is fine).
    if profiled == 0 and hard_failures > 0:
        raise RuntimeError(
            f"All profiling attempts failed across datasets {args.source_datasets} "
            f"({hard_failures} hard failure(s), 0 tables profiled) — failing the task."
        )

    if not rows:
        logger.warning(
            "No rows to write for %s on %s (%d table(s) profiled, %.2f GB scanned); "
            "partition left unchanged.",
            args.source_datasets,
            args.date,
            profiled,
            total_gb_scanned,
        )
        return

    save_profiles(
        client,
        rows,
        args.date,
        args.destination_project,
        args.destination_dataset,
        args.destination_table,
    )
    logger.info(
        "Wrote %d rows from %d table(s) across %d dataset(s) to %s.%s.%s$%s "
        "(%.2f GB scanned)",
        len(rows),
        profiled,
        len(args.source_datasets),
        args.destination_project,
        args.destination_dataset,
        args.destination_table,
        args.date.replace("-", ""),
        total_gb_scanned,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
