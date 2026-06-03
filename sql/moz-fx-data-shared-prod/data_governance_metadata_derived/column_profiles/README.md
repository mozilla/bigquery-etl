# Column Profiles

Per-column profiling statistics (null rate, approximate distinct count,
cardinality, example values, value/frequency distributions) for base tables
across Mozilla source datasets. Produced weekly and consumed by schema
enrichment tooling to generate accurate column descriptions.

## Architecture

Profiling is split into **one job (and table) per source dataset**, with a
single **union view** as the read interface:

```
                          ┌─────────────────────────────────────────┐
  weekly bqetl job  ─────▶│ column_profiles_telemetry_derived_v1     │┐
  (one per source         │   partitioned by profiled_at (DATE)      ││
   dataset)               │   clustered by source_table, column_name ││  per-dataset
                          └─────────────────────────────────────────┘│  tables
                           ┌─────────────────────────────────────────┐│
                           │ column_profiles_<other_dataset>_v1  ...  │┘
                           └─────────────────────────────────────────┘
                                              │ UNION ALL
                                              ▼
                                  ┌───────────────────────┐
                                  │   column_profiles      │   ← query this
                                  │        (view)          │
                                  └───────────────────────┘
                                              │
                                              ▼
                                   schema_enricher agent
                                   (read_column_profiles)
```

Why per-dataset tables instead of one shared table:

- **Idempotent reruns/backfills** — each weekly run overwrites only its own
  `profiled_at` date partition (`WRITE_TRUNCATE` on the partition decorator).
  A single shared table would force `DELETE`+`INSERT` per dataset and risk
  concurrent-DML contention.
- **Failure isolation** — a problem profiling one dataset doesn't fail the
  others.
- **Per-dataset cost attribution** — each job is its own Airflow task.

The union `view` gives consumers a single query surface regardless of how many
per-dataset tables exist.

## Querying the view (`column_profiles`)

The view exposes the same columns as every underlying table. **Always filter on
`profiled_at`** — it is the partition key, and a filter lets BigQuery prune
partitions in each underlying table (pruning propagates through `UNION ALL`).

Latest profile for one table's columns:

```sql
SELECT column_name, data_type, null_rate, distinct_count, example_value
FROM `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles`
WHERE source_dataset = 'telemetry_derived'
  AND source_table   = 'feature_usage_v2'
  AND profiled_at >= CURRENT_DATE() - INTERVAL 14 DAY   -- prunes to ~1-2 partitions
QUALIFY ROW_NUMBER() OVER (PARTITION BY column_name ORDER BY profiled_at DESC) = 1;
```

Low-cardinality value distribution for a column:

```sql
SELECT column_name, v.value, v.frequency
FROM `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles`,
     UNNEST(values) AS v
WHERE source_dataset = 'telemetry_derived'
  AND source_table   = 'feature_usage_v2'
  AND column_name    = 'normalized_channel'
  AND profiled_at >= CURRENT_DATE() - INTERVAL 14 DAY
ORDER BY v.frequency DESC;
```

Coverage snapshot for a dataset (how many columns profiled, by tier):

```sql
SELECT source_table, column_tier, COUNT(*) AS n_columns
FROM `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles`
WHERE source_dataset = 'telemetry_derived'
  AND profiled_at >= CURRENT_DATE() - INTERVAL 14 DAY
GROUP BY source_table, column_tier
ORDER BY source_table, column_tier;
```

> **Pruning note:** filtering only on `source_dataset` / `source_table` does
> *not* prune partitions (those are clustering keys, not the partition column),
> and the view still fans out to every underlying table. Including a
> `profiled_at` lower bound is what bounds the scan. `DATE(profiled_at) >= …`
> also works and prunes if you need a form that is valid against both `DATE`
> and `TIMESTAMP` profiled_at columns.

## Schema

| Column | Type | Mode | Description |
|---|---|---|---|
| `source_project` | STRING | REQUIRED | GCP project of the profiled table. |
| `source_dataset` | STRING | REQUIRED | Dataset of the profiled table. |
| `source_table` | STRING | REQUIRED | Table name (no project/dataset). |
| `column_name` | STRING | REQUIRED | Field path, dot notation for nested fields. |
| `data_type` | STRING | REQUIRED | BigQuery data type from INFORMATION_SCHEMA. |
| `null_rate` | FLOAT | NULLABLE | Percentage of rows where the column is NULL. |
| `distinct_count` | INTEGER | NULLABLE | Approximate distinct non-null values. |
| `is_high_cardinality` | BOOLEAN | NULLABLE | True if `distinct_count` > 50. |
| `example_value` | STRING | NULLABLE | Representative value (high-cardinality columns only). |
| `values` | RECORD (REPEATED) | | `value`/`frequency` pairs (low-cardinality columns, distinct_count ≤ 50). |
| `column_tier` | STRING | NULLABLE | `scalar`, `leaf`, `nested_leaf`, `scalar_array`, `undocumented`, or `pii_suppressed`. |
| `profiled_at` | DATE | REQUIRED | Run date of the profiling job; the partition key. |

PII-named columns are never scanned — they appear with `column_tier =
'pii_suppressed'` and no statistics. Tables with more than 500 columns are
skipped (too wide to profile in one pass) and simply produce no rows.

## Partitioning & retention

Each per-dataset table is:

- **Partitioned** by `profiled_at` (DATE, daily granularity).
- **Clustered** by `source_table`, `column_name`.
- **Retained** for **90 days** (`expiration_days: 90`) — roughly the last ~13
  weekly snapshots; older partitions are auto-deleted by BigQuery. Consumers
  read the latest snapshot, so history is kept only for drift analysis.

## Scheduling

All per-dataset jobs run on the **`bqetl_data_governance_metadata`** DAG,
weekly (Mondays 04:00 UTC). Each run profiles its source dataset's base tables
(1% `sample_id` sampling and a 7-day partition filter bound the scan) and
overwrites that run's `profiled_at` partition.

## Adding a new source dataset

To start profiling another dataset (e.g. `firefox_desktop`):

1. **Clone the job directory** for the new dataset:

   ```bash
   cp -r \
     sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_telemetry_derived_v1 \
     sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_firefox_desktop_v1
   ```

   `query.py` is generic — it takes the source dataset and destination table as
   arguments, so it does **not** need editing.

2. **Update `metadata.yaml`** in the new directory:
   - `friendly_name` / `description` → reference the new dataset.
   - `scheduling.arguments` → point at the new dataset and destination table:
     ```yaml
     arguments: [
       "--date", "{{ ds }}",
       "--source-dataset", "firefox_desktop",
       "--destination-table", "column_profiles_firefox_desktop_v1"
     ]
     ```
   - Keep `dag_name: bqetl_data_governance_metadata`, the `time_partitioning`
     (`profiled_at`, `expiration_days: 90`), and clustering unchanged.

3. **`schema.yaml`** is identical for every dataset — copy as-is.

4. **Deploy the new table's schema** (the partition-decorator load won't create
   a partitioned/clustered table on its own):

   ```bash
   ./bqetl query schema deploy \
     moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles_firefox_desktop_v1
   ```

5. **Add the table to the union view** — append a `UNION ALL` block in
   [`view.sql`](./view.sql):

   ```sql
   UNION ALL
   SELECT *
   FROM `moz-fx-data-shared-prod.data_governance_metadata_derived.column_profiles_firefox_desktop_v1`
   ```

6. **Regenerate the DAG** so the new task is scheduled:

   ```bash
   ./bqetl dag generate bqetl_data_governance_metadata
   ```

7. **Confirm IAM** — the Airflow workload-identity SA
   (`default-workloads@moz-fx-data-airflow-gke-prod`) needs read on the new
   source dataset. Standard `derived`/`derived_restricted` datasets already
   grant the pipeline service accounts via their base ACL; verify with
   `bq show <project>:<source_dataset>` if unsure.

## Consumers

The `schema_enricher` agent (in the `data-shared-llm-agents` repo) reads the
view via its `read_column_profiles` tool, which selects the most recent
snapshot per column using the 14-day partition-pruning window shown above.
