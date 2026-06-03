#!/usr/bin/env python3

"""Profile columns of every base table in a source dataset.

Writes one row per profiled column to a per-dataset destination table in
``data_governance_metadata_derived``. Each weekly run overwrites its own date
partition (``profiled_at``) so the job is idempotent.

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


def build_profile_query(
    table: str,
    columns: list[tuple[str, str, str]],
    partition_filter: str | None = None,
) -> str:
    """Build a single query that profiles every profilable column in one pass.

    Profiles scalar and leaf tiers only. nested_leaf fields are profiled
    separately via build_nested_profile_queries (UNNEST-based).
    """
    excluded_tiers = {"undocumented", "pii_suppressed", "nested_leaf", "scalar_array"}
    profilable = [(fp, dt) for fp, dt, tier in columns if tier not in excluded_tiers]
    has_sample_id = any(fp == "sample_id" for fp, _ in profilable)

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

    where_clauses = []
    if partition_filter:
        where_clauses.append(partition_filter)
    if has_sample_id:
        sample_id = random.randint(1, 99)
        where_clauses.append(f"`sample_id` = {sample_id}")

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return f"SELECT {select} FROM `{table}` {where}"


def build_nested_profile_queries(
    table: str,
    columns: list[tuple[str, str, str]],
    partition_filter: str | None = None,
) -> list[tuple[str, str, list[tuple[str, str]]]]:
    """Build one UNNEST-based profiling query per REPEATED parent column.

    Each query profiles all nested_leaf fields under a single REPEATED parent.
    Returns a list of (parent_column, query_sql, nested_fields) tuples.
    nested_fields is [(field_path, data_type), ...] for result parsing.
    """
    nested_leaves = [(fp, dt) for fp, dt, tier in columns if tier == "nested_leaf"]
    if not nested_leaves:
        return []

    by_parent: dict[str, list[tuple[str, str]]] = {}
    for field_path, data_type in nested_leaves:
        parent = field_path.split(".")[0]
        by_parent.setdefault(parent, []).append((field_path, data_type))

    has_sample_id = any(
        fp == "sample_id" for fp, _, tier in columns if tier == "scalar"
    )

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

        where_clauses = []
        if partition_filter:
            where_clauses.append(partition_filter)
        if has_sample_id:
            sample_id = random.randint(1, 99)
            where_clauses.append(f"`sample_id` = {sample_id}")

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = (
            f"SELECT {select} "
            f"FROM `{table}`, UNNEST(`{parent}`) AS `{unnest_alias}` "
            f"{where}"
        )
        results.append((parent, query, fields))

    return results


def get_partition_filter(client: bigquery.Client, table: str) -> str | None:
    """Return a WHERE clause scoped to a recent partition, or None."""
    table_ref = client.get_table(table)
    tp = table_ref.time_partitioning
    if tp is None:
        return None
    field = tp.field
    if field:
        return f"DATE(`{field}`) = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    return "_PARTITIONDATE = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"


def check_table_has_rows(client: bigquery.Client, table: str) -> tuple[int, float]:
    """Return (total_rows, total_gb). (-1, 0) if not found, (0, 0) if empty."""
    project, dataset, table_name = table.split(".")
    _validate_bq_identifier(project, "project")
    _validate_bq_identifier(dataset, "dataset")
    query = f"""
        SELECT total_rows, total_logical_bytes
        FROM `{project}.region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
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
) -> tuple[dict[str, Any], float]:
    """Execute the profiling query and return (column_stats_dict, gb_scanned)."""
    partition_filter = get_partition_filter(client, table)
    query = build_profile_query(table, columns, partition_filter)

    job = client.query(query)
    rows = list(job.result())
    gb_scanned = job.total_bytes_processed / 1e9

    row = rows[0]
    total_rows = row.total_rows
    results: dict[str, Any] = {}

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

    nested_queries = build_nested_profile_queries(table, columns, partition_filter)
    for _parent, nested_sql, nested_fields in nested_queries:
        try:
            nested_job = client.query(nested_sql)
            nested_rows = list(nested_job.result())
            gb_scanned += nested_job.total_bytes_processed / 1e9
            nested_row = nested_rows[0]
            nested_total = nested_row.total_rows
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
    project: str,
    dataset: str,
    table: str,
    columns_filter: list[str] | None = None,
) -> str:
    """Profile every column in a single BigQuery table; return stats as JSON.

    Samples 1% when a sample_id column is present. Applies a 7-day partition
    filter when a date partition column is detected. Fields inside REPEATED
    structs are profiled via UNNEST with the same sampling. Simple
    ARRAY<scalar> columns are listed as tier "scalar_array" with no stats.
    Fields under _TIER3_COLUMNS (e.g. metrics) remain tier "undocumented".

    Returns a JSON object with column stats, or {"error": "..."} if the table
    is empty, not found, or inaccessible.
    """
    fully_qualified = f"{project}.{dataset}.{table}"
    try:
        client = bigquery.Client(project=project)

        total_rows, total_gb = check_table_has_rows(client, fully_qualified)
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
        column_stats, gb_scanned = _run_profile_query(client, fully_qualified, columns)

        if columns_filter:
            filter_set = set(columns_filter)
            column_stats = {
                fp: stats
                for fp, stats in column_stats.items()
                if fp.split(".")[0] in filter_set
            }

        return json.dumps(
            {
                "table": fully_qualified,
                "total_rows": total_rows,
                "gb_scanned": round(gb_scanned, 4),
                "profiled_at": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y-%m-%d"
                ),
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
    job_config.clustering_fields = ["source_table", "column_name"]

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
    parser.add_argument("--source-dataset", required=True)
    parser.add_argument("--destination-project", default="moz-fx-data-shared-prod")
    parser.add_argument(
        "--destination-dataset", default="data_governance_metadata_derived"
    )
    parser.add_argument(
        "--destination-table", default="column_profiles_telemetry_derived_v1"
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset of source table names to profile.",
    )
    parser.add_argument("--max-workers", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    """Entrypoint for the column profiler job."""
    args = parse_args()

    client = bigquery.Client(project=args.source_project)

    tables_result = json.loads(list_bq_tables(args.source_project, args.source_dataset))
    if "error" in tables_result:
        raise RuntimeError(
            f"Failed to list tables in {args.source_project}."
            f"{args.source_dataset}: {tables_result['error']}"
        )

    base_tables = [
        t["table"]
        for t in tables_result["tables"]
        if t["table_type"] not in _SKIP_TABLE_TYPES
    ]
    if args.tables:
        subset = set(args.tables)
        base_tables = [t for t in base_tables if t in subset]

    logging.info(
        "Profiling %d base table(s) in %s.%s",
        len(base_tables),
        args.source_project,
        args.source_dataset,
    )

    def _profile_one(table_name: str) -> tuple[str, str]:
        return table_name, profile_bq_table(
            args.source_project, args.source_dataset, table_name
        )

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(_profile_one, t): t for t in base_tables}
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                _, profile_json = future.result()
            except Exception as e:
                logging.warning("Profiling raised for %s: %s", table_name, e)
                continue

            profile = json.loads(profile_json)
            if "error" in profile:
                logging.warning("Skipping %s: %s", table_name, profile["error"])
                continue

            table_rows = build_rows(profile, args.date)
            logging.info("%s → %d column rows", table_name, len(table_rows))
            rows.extend(table_rows)

    if not rows:
        logging.warning(
            "No rows to write for %s.%s on %s; partition left unchanged.",
            args.source_project,
            args.source_dataset,
            args.date,
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
    logging.info(
        "Wrote %d rows to %s.%s.%s$%s",
        len(rows),
        args.destination_project,
        args.destination_dataset,
        args.destination_table,
        args.date.replace("-", ""),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
