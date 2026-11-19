"""Read the classifier's inputs out of BigQuery.

Every read the pipeline does lives here: the columns to classify and the evidence
attached to them, the table-to-ping mapping, the probe definitions those pings
carry, and the columns an earlier run already finished.

The three metadata tables accumulate one snapshot per pipeline run, and a run only
refreshes the datasets it was asked for, so the newest snapshot per key is not
the newest partition. Every reader of those tables therefore picks the latest row
per key with QUALIFY ROW_NUMBER(): filtering to the maximum timestamp instead
would silently drop every key last refreshed by an earlier run. The destination
table needs none of this, holding one current-state row per column.
"""

import logging
from typing import Any

from google.cloud import bigquery

from .config import ClassificationConfig, validate_bq_identifier

TableKey = tuple[str, str, str]  # (source_project, source_dataset, source_table)

ColumnKey = tuple[str, str, str, str]  # TableKey plus column_name

PingKey = tuple[str, str]  # (ping_platform, source_ping)

# The source table's declared schema drives the work list and the profile joins
# onto it, so a column the profiler skipped or has not reached yet is still
# classified from its name, type and description, and a column dropped from the
# source table stops being classified even while its profile row is inside the
# retention window.
_COLUMNS_QUERY = """
WITH profiles AS (
  SELECT
    source_table, column_name, column_tier, null_rate, distinct_count,
    is_high_cardinality, example_value, `values`
  FROM `{profiles_table}`
  WHERE source_project = @project AND source_dataset = @dataset{profiles_predicate}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_table, column_name ORDER BY profiled_at DESC
  ) = 1
),
lineage AS (
  SELECT source_table, ping_platform
  FROM `{lineage_table}`
  WHERE source_project = @project AND source_dataset = @dataset
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_table ORDER BY resolved_at DESC
  ) = 1
),
candidates AS (
  SELECT
    c.table_name, c.field_path, c.data_type,
    NULLIF(c.description, '') AS description,
    -- A `metrics.` path is a Glean metric only when lineage resolved the table
    -- to ping_platform = 'glean'. Derived tables can carry a `metrics` STRUCT of
    -- their own whose fields are ordinary columns. A NULL platform means no
    -- lineage row or no ping found, and counts as glean so a ping table's metric
    -- leaves behave the same before and after it resolves.
    (STARTS_WITH(c.field_path, 'metrics.')
      AND COALESCE(l.ping_platform, 'glean') = 'glean') AS is_glean_metric
  -- The same project and dataset as the parameters above, interpolated rather
  -- than bound because an identifier cannot be a query parameter.
  -- validate_bq_identifier guards these two before they reach the FROM clause.
  FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS AS c
  LEFT JOIN lineage l ON l.source_table = c.table_name
)
SELECT
  c.table_name, c.field_path, c.data_type, c.description, c.is_glean_metric,
  p.column_tier, p.null_rate, p.distinct_count, p.is_high_cardinality,
  p.example_value, p.`values`
FROM candidates c
LEFT JOIN profiles p
  ON p.source_table = c.table_name AND p.column_name = c.field_path
WHERE (
    -- A Glean metric is metrics.<type>.<name>, whatever BigQuery shape its type
    -- takes (a labeled_counter is an ARRAY<STRUCT<key, value>>, a
    -- timing_distribution a STRUCT), so the container filter must not reach it.
    -- Deeper paths are Glean-internal structure rather than product metrics: the
    -- .key / .value of a labeled map, the .sum / .values of a distribution.
    (c.is_glean_metric AND ARRAY_LENGTH(SPLIT(c.field_path, '.')) = 3)
    -- Everything else, including a non-Glean `metrics` STRUCT: containers are
    -- excluded, since a container carries no value of its own to classify.
    -- ARRAY<scalar> is deliberately kept, since it does.
    OR (NOT c.is_glean_metric
      AND c.data_type NOT LIKE 'STRUCT<%'
      AND c.data_type NOT LIKE 'ARRAY<STRUCT<%')
  )
  -- Scope is the tables this pipeline profiled: one the profiler produced no rows
  -- for is not classified, whether it was empty, missing, or past its column
  -- limit. Widening that coverage is an upstream ask. The subquery also keeps out
  -- the views COLUMN_FIELD_PATHS lists alongside tables.
  AND c.table_name IN (SELECT DISTINCT source_table FROM profiles){schema_predicate}
ORDER BY c.table_name, c.field_path
"""

_PING_MAPPING_QUERY = """
SELECT source_project, source_dataset, source_table, source_ping, ping_platform
FROM `{lineage_table}`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY source_project, source_dataset, source_table
  ORDER BY resolved_at DESC
) = 1
"""

_PROBES_QUERY = """
SELECT ping_platform, source_ping, probe_name, probe_description, probe_type,
       data_sensitivity, tags
FROM `{probes_table}`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ping_platform, source_ping, probe_name ORDER BY fetched_at DESC
) = 1
"""

# Filtered on model and not on taxonomy_version: a model switch should redo the
# work (the destination is current-state, so the new rows replace the old ones),
# while a taxonomy bump is a decision to re-classify deliberately rather than
# something a resume should trigger.
_ALREADY_CLASSIFIED_QUERY = """
SELECT source_project, source_dataset, source_table, column_name
FROM `{destination_table}`
WHERE source_project = @project AND source_dataset = @dataset
  AND model = @model{table_predicate}
"""


def _table_predicate(column: str, table: str | None) -> str:
    """Return the single-table filter on `column`, empty when no table was given."""
    return f" AND {column} = @table" if table is not None else ""


def _job_config(**scalars: str | None) -> bigquery.QueryJobConfig:
    """Bind the named STRING parameters, skipping the ones left unset.

    Keeps the optional predicates and the parameters that fill them from drifting
    apart: a parameter passed as None is one whose predicate is absent too.
    """
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(name, "STRING", value)
            for name, value in scalars.items()
            if value is not None
        ]
    )


def client_for(config: ClassificationConfig) -> bigquery.Client:
    """Build the BigQuery client for a run.

    Which project bills the reads is a config decision, so it is settled here
    rather than at the call site.
    """
    return bigquery.Client(project=config.project)


def load_columns(
    client: bigquery.Client,
    config: ClassificationConfig,
    project: str,
    dataset: str,
    table: str | None = None,
) -> dict[TableKey, list[dict[str, Any]]]:
    """Load the columns to classify for one dataset, grouped by table.

    Each column dict carries the schema's own name, type and description plus the
    latest profile of that column (whose fields are all None for a column the
    profiler has no row for), and is_glean_metric (which needs the table's resolved
    ping platform rather than its field path alone).
    """
    validate_bq_identifier(project, "project")
    validate_bq_identifier(dataset, "dataset")
    query = _COLUMNS_QUERY.format(
        profiles_table=config.profiles_table,
        lineage_table=config.lineage_table,
        project=project,
        dataset=dataset,
        profiles_predicate=_table_predicate("source_table", table),
        schema_predicate=_table_predicate("c.table_name", table),
    )
    job_config = _job_config(project=project, dataset=dataset, table=table)

    columns_by_table: dict[TableKey, list[dict[str, Any]]] = {}
    total = 0
    for row in client.query(query, job_config=job_config).result():
        values = [(v["value"], v["frequency"]) for v in (row["values"] or [])]
        columns_by_table.setdefault((project, dataset, row.table_name), []).append(
            {
                "column_name": row.field_path,
                "data_type": row.data_type,
                "bq_description": row.description,
                "is_glean_metric": row.is_glean_metric,
                "column_tier": row.column_tier,
                "null_rate": row.null_rate,
                "distinct_count": row.distinct_count,
                "is_high_cardinality": row.is_high_cardinality,
                "example_value": row.example_value,
                "values": values,
            }
        )
        total += 1
    logging.info(
        "Loaded %s columns across %s tables in %s.%s",
        total,
        len(columns_by_table),
        project,
        dataset,
    )
    return columns_by_table


def load_ping_mapping(
    client: bigquery.Client, config: ClassificationConfig
) -> dict[TableKey, dict[str, Any]]:
    """Load each profiled table's latest ping resolution, keyed by table.

    A table whose lineage found no ping node is present with both fields None:
    that is its latest resolution, and skipping it in SQL would resurrect an
    earlier snapshot's ping instead, since the filter would run before the dedup.
    Callers therefore check source_ping before looking a ping up.

    Loaded once per run and not filtered by dataset: one lineage row per profiled
    table is small enough to hold, and the probes it leads to are keyed by ping,
    which serves many datasets.
    """
    query = _PING_MAPPING_QUERY.format(lineage_table=config.lineage_table)
    mapping = {
        (row.source_project, row.source_dataset, row.source_table): {
            "source_ping": row.source_ping,
            "ping_platform": row.ping_platform,
        }
        for row in client.query(query).result()
    }
    logging.info("Loaded ping mapping for %s tables", len(mapping))
    return mapping


def load_probes_by_ping(
    client: bigquery.Client, config: ClassificationConfig
) -> dict[PingKey, list[dict[str, Any]]]:
    """Load the probe definitions of every resolved ping, keyed by ping.

    source_ping is already app-qualified (`fenix.baseline`), so the ping is the
    whole key: there is no separate app dimension to break ties on.
    """
    query = _PROBES_QUERY.format(probes_table=config.probes_table)
    probes_by_ping: dict[PingKey, list[dict[str, Any]]] = {}
    total = 0
    for row in client.query(query).result():
        probes_by_ping.setdefault((row.ping_platform, row.source_ping), []).append(
            {
                "probe_name": row.probe_name,
                "probe_description": row.probe_description,
                "probe_type": row.probe_type,
                "data_sensitivity": list(row.data_sensitivity or []),
                "tags": list(row.tags or []),
            }
        )
        total += 1
    logging.info("Loaded %s probes across %s pings", total, len(probes_by_ping))
    return probes_by_ping


def load_already_classified(
    client: bigquery.Client,
    config: ClassificationConfig,
    project: str,
    dataset: str,
    table: str | None = None,
) -> set[ColumnKey]:
    """Return the (project, dataset, table, column) keys already classified.

    Scoped to one dataset and to this run's model. The destination table is
    current-state over the whole warehouse, so it is not small enough to pull
    into a set whole.
    """
    query = _ALREADY_CLASSIFIED_QUERY.format(
        destination_table=config.destination_table,
        table_predicate=_table_predicate("source_table", table),
    )
    job_config = _job_config(
        project=project, dataset=dataset, model=config.model, table=table
    )

    done = {
        (row.source_project, row.source_dataset, row.source_table, row.column_name)
        for row in client.query(query, job_config=job_config).result()
    }
    logging.info(
        "%s columns already classified in %s.%s with %s",
        len(done),
        project,
        dataset,
        config.model,
    )
    return done
