# Statcounter CSV Ingestion Pipeline

Fetches CSVs from Statcounter public download URLs (one per geography/device combination per day), enriches them into one row per date/geography/device/browser, and loads them into BigQuery via GCS.

Examples include:

- [Desktop Browser Market Share Europe for 1 Jan 2026](https://gs.statcounter.com/browser-market-share/desktop/europe/#daily-20260101-20260101-bar)
- [Mobile Browser Market Share Worldwide for 1 Jan 2026](https://gs.statcounter.com/browser-market-share/mobile/worldwide/#daily-20260101-20260101-bar)

This is a source ingestion pipeline following an EtLT pattern — the pipeline extracts, lightly transforms (column renames and dimension stamping), and loads data into BigQuery as-is. Any aggregation or business logic happens in downstream queries. The schema reflects Statcounter's data model, not an internal one, and is intentionally fixed.

## Table of Contents

- [Statcounter CSV Ingestion Pipeline](#statcounter-csv-ingestion-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Structure](#structure)
  - [Tables](#tables)
  - [Running Locally](#running-locally)
  - [Data](#data)
    - [Schema](#schema)
    - [Transformation](#transformation)
    - [Primary Key](#primary-key)
  - [Pipeline](#pipeline)
    - [Pipeline Steps](#pipeline-steps)
    - [Idempotency](#idempotency)
    - [Response Validation](#response-validation)
    - [Data Quality](#data-quality)
    - [Errors](#errors)
      - [CSV Deletion](#csv-deletion)
  - [Implementation](#implementation)
    - [Observability](#observability)
    - [Configuration](#configuration)
    - [Code Quality](#code-quality)

## Structure

Shared pipeline logic lives in `pipeline.py` at the `statcounter_derived` level. Each table has its own subdirectory with a thin `query.py` that declares a `GEOGRAPHIES` list and calls `make_app()` to build a Typer CLI.

``` text
statcounter_derived/
  pipeline.py                  ← shared logic
  table_v1/
    query.py                   ← thin wrapper
    metadata.yaml
    schema.yaml
```

To add a new pipeline, create a new subdirectory with a `query.py`, `metadata.yaml`, and `schema.yaml`. In `query.py`, declare a `GEOGRAPHIES` list of `(name, slug, code)` tuples and call `make_app()` with `geographies`, `gcs_blob_prefix`, `bq_table`, and `clustering_fields`.

Each table subdirectory must include a `README.md` with a Schema table (columns, types, descriptions) and a Source section pointing to `../pipeline.py`. Keep table `README`s focused on what the table contains — implementation detail lives in the dataset-level `README`.

## Tables

| Table | Geography | DAG | Clustering |
| --- | --- | --- | --- |
| `browser_market_share_worldwide_v1` | Worldwide (one source per device) | `bqetl_statcounter_browser_market_share` | `device`, `browser` |
| `browser_market_share_regions_v1` | Africa, Asia, Europe, North America, Oceania, South America (one source per region/device combination) | `bqetl_statcounter_browser_market_share` | `geography`, `device`, `browser` |

## Running Locally

In production, Airflow invokes each `query.py` directly on the scheduled date. The steps below are for local development and backfill only.

Requires Python 3.11. To set up a local environment:

``` text
cd sql/moz-fx-data-shared-prod/statcounter_derived
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`pipeline.py` creates the dataset, table, and primary key constraints on first run if they do not already exist.

To run a pipeline:

``` text
# Default: yesterday
python <table_v1>/query.py

# Single specific date
python <table_v1>/query.py --date-from 2026-05-12

# Date range
python <table_v1>/query.py --date-from 2026-05-01 --date-to 2026-05-12
```

Each date is a separate fetch/load cycle, so large ranges take proportionally longer — worldwide makes 2 requests per day (one per device), regions makes 12 (one per region/device combination).

## Data

### Schema

The output schema after transformation has six columns:

| Column | Type | Description |
| --- | --- | --- |
| `sk` | STRING | Surrogate key; MD5 hash of date, geography, device, and browser |
| `date` | DATE | The date of the observation |
| `geography` | STRING | The geographic region |
| `device` | STRING | The device type (Desktop or Mobile) |
| `browser` | STRING | The browser name |
| `percent` | FLOAT | The browser's market share percentage |

`pipeline.py` defines the schema in `SCHEMA` and each table's `schema.yaml` documents it. The pipeline passes it explicitly to the BigQuery load job rather than using autodetect.

All fields are `NULLABLE` in `schema.yaml`. The pipeline enforces non-null constraints on PK columns before loading (see [Data Quality](#data-quality)) rather than relying on the schema, so that unexpected data from Statcounter produces a catchable validation error rather than a hard load failure.

The schema omits a `loaded_at` column. With `WRITE_TRUNCATE`, it would only reflect the most recent load per partition, not a full audit trail. Find load history in `INFORMATION_SCHEMA.PARTITIONS` or Airflow task logs.

The pipeline hardcodes the schema rather than deriving it dynamically from `PipelineConfig` because Statcounter's public CSV export only provides data at the device, region, and date grain — no finer breakdown is available from the source. The grain cannot change without a change to the upstream data product itself. A dynamic schema (deriving columns and PK from `PipelineConfig` at runtime) would add flexibility but introduces complexity — losing type safety, requiring dimension key consistency validation across sources at config time, with no payoff when the source grain is fixed.

### Transformation

The raw CSV from Statcounter has two columns: `Browser` and a dynamic percent column whose name includes the date. Statcounter alternates between abbreviated and full month names (e.g. `Market Share Perc. (DD MMM YYYY)` or `Market Share Perc. (DD MMMM YYYY)`), so the pipeline tries both formats when parsing. After parsing, the pipeline renames these to `browser` and `percent`, then stamps each row with the partition date, geography, and device. The result has one row per date/geography/device/browser combination.

| Before | After |
| --- | --- |
| `Browser` | `browser` |
| `Market Share Perc. (DD MMM YYYY)` or `(DD MMMM YYYY)` | `percent` |
| _(no date column)_ | `date` — added from partition date |
| _(no geography column)_ | `geography` — added from source definition |
| _(no device column)_ | `device` — added from source definition |

### Primary Key

Each table is partitioned by `date`. Within a partition, the PK is the set of columns that uniquely identify a row. The pipeline asserts PK integrity (no nulls, no duplicates) before loading. See each table's README for its specific PK.

## Pipeline

> [!NOTE]
> The pipeline processes each date independently. Within a single date, the pipeline fails fast: errors in fetching or validation stop processing before any data reaches GCS or BigQuery, and a count mismatch after load leaves the GCS file in place for inspection. Across a multi-date range (backfills), if a date fails, the pipeline logs the traceback and continues with the next date. At the end of the run, the pipeline emits a summary listing succeeded and failed dates, then exits with an error if any date failed so Airflow marks the task red. Single-date runs (the daily Airflow case) raise on first failure as before. See [Errors](#errors) for a full breakdown.

### Pipeline Steps

Once per run:

1. Create the dataset if it does not exist
2. Create the table with schema, partitioning, and clustering if it does not exist
3. Set primary key constraints on the table if not already set

Each date in the requested range runs inside its own try/except — a failure on one date does not stop later dates. For each date, for each source (one per geography/device combination):

1. Fetch CSV from the download URL for that single date
2. Parse CSV into a DataFrame
3. Rename columns to match schema (`Browser` → `browser`, dynamic percent column → `percent`)
4. Add date, geography, and device columns from the partition date and source definition
5. Add surrogate key (`sk`) as MD5 hash of the natural key columns
6. Reorder columns to match schema field order

Then across all sources combined:

1. Concatenate all DataFrames into one
2. Validate at least one row is present
3. Validate no nulls or duplicates on the PK columns (`date`, `geography`, `device`, `browser`)
4. Validate numeric columns within expected bounds
5. Upload the transformed CSV to GCS
6. Load the CSV into the target BigQuery partition, replacing any existing data for that date
7. Count records in BigQuery for that date
8. Compare DataFrame and BigQuery record counts, raise if mismatched
9. Delete the CSV from GCS

### Idempotency

The pipeline processes date ranges one day at a time, mapping each to a single BigQuery partition overwrite — re-running any date replaces its partition cleanly with no duplicate rows. Because each date is self-contained, you can safely resume a backfill by narrowing the date range to only the failed dates listed in the end-of-run summary.

### Response Validation

After fetching, the pipeline checks the response for an empty body and for the presence of at least one data row. It raises an error immediately if either check fails, before writing any data to GCS or BigQuery.

### Data Quality

Before loading, the pipeline validates the transformed DataFrame against the following checks:

- **Row count** — the concatenated DataFrame must contain at least one row
- **PK** — (`date`, `geography`, `device`, `browser`) must not be null, empty, or duplicated
- **Numeric bounds** — numeric columns defined in `NUMERIC_COLUMNS` must fall within expected bounds

After loading, the pipeline compares the DataFrame record count against the BigQuery record count for the same date. If they do not match, the pipeline raises an error and does not delete the GCS file.

Each table also has a `checks.sql` that runs as a separate Airflow task after the load. It warns if the row count for a given date is more than 10% above or below the 7-day average, which catches anomalies that pre-load validation cannot — such as Statcounter returning significantly fewer browsers than usual.

### Errors

> [!WARNING]
>
> | Stage | Condition | Behavior |
> | --- | --- | --- |
> | Fetch | Empty body or no data rows | Raises before writing to GCS or BigQuery |
> | Validation | Min row count, PK, or bounds check fails | Raises before uploading to GCS |
> | Load | BigQuery load job fails | Raises; pipeline attempts GCS cleanup (logged if it fails) before re-raising the load error |
> | Post-load | DataFrame and BigQuery counts do not match | Raises; pipeline leaves the GCS file in place for inspection |
> | Cleanup | GCS delete fails after a successful load and count check | Logs the error; the date stays marked succeeded because the data is in BigQuery |
>
> In a multi-date run, the outer loop catches any raise inside a date. The pipeline records the failed date with its full traceback and proceeds to the next date. Single-date runs raise immediately. In both cases, the task exits non-zero if any date failed.

#### CSV Deletion

> [!NOTE]
> The pipeline deletes the CSV after a successful load (no archiving). The BigQuery table is the authoritative record — the pipeline only deletes after the post-load count check confirms the data landed correctly. Check `INFORMATION_SCHEMA.PARTITIONS` or Airflow task logs for partition history.

## Implementation

### Observability

Python's standard `logging` module emits logs at each step, which route to Airflow task logs or the local CLI depending on the execution environment. Airflow handles scheduling.

### Configuration

Shared constants live at the top of `pipeline.py`:

| Constant | Description |
| --- | --- |
| `GCS_BUCKET` | GCS bucket name for staging |
| `BQ_PROJECT` | BigQuery project ID |
| `BQ_DATASET` | BigQuery dataset name |
| `DATE_COLUMN` | Date column used for partitioning |
| `SCHEMA` | List of column definitions matching `schema.yaml` |
| `PK_COLUMNS` | List of primary key columns used for null/duplicate validation and table constraints |
| `NUMERIC_COLUMNS` | Dict mapping column names to `(lower, upper)` bounds tuples |

Each table's `query.py` sets per-pipeline values via `make_app()`:

| Field | Description |
| --- | --- |
| `geographies` | List of `(name, slug, code)` tuples — one entry per geography. `build_sources()` expands this across Desktop and Mobile to produce the full source list |
| `gcs_blob_prefix` | GCS path prefix for the staged CSV file — date and run ID are appended at runtime (e.g. `statcounter_data/browser_market_share_worldwide_YYYYMMDD_<run_id>.csv`) |
| `bq_table` | BigQuery table name |
| `clustering_fields` | BigQuery clustering fields for the table — must match the `clustering` config in `metadata.yaml` |

### Code Quality

- Each pipeline step is an atomic function with a single responsibility
- All functions have Google-style docstrings with types
- `black` formats all Python files
- `pipeline.py` holds shared constants, logic, and `make_app()`; each `query.py` supplies per-pipeline values
- BigQuery queries use parameterized values rather than string interpolation
- Airflow handles I/O retries (`retries: 2, retry_delay: 30m` in `dags.yaml`)
- Inline comments explain non-obvious logic only — not what the code does, but why
- Annotate a local only when its right-hand side does not pin the type — empty collections like `dfs: list[pd.DataFrame] = []` need an annotation; string literals and typed function returns do not

| Task type | Examples | Purity | Deterministic | Notes |
| --- | --- | --- | --- | --- |
| Transformation | `rename_columns`, `add_metadata`, `add_surrogate_key`, `reorder_columns`, `concatenate_dataframes` | Pure | Yes | Return new DataFrames via `df.assign()` / `df.rename()`; inputs never mutated |
| Scheduling | `date.today()` in `main` | Pure | No | No side effects, but the return value varies by call time |
| Validation | `validate_min_row_count`, `validate_no_nulls`, `validate_unique_pk`, `validate_numeric_bounds` | Impure | Yes | Raises and logs are intentional controlled side effects |
| I/O | `fetch_csv`, `upload_to_gcs`, `load_into_bigquery`, `count_bq_records`, `delete_from_gcs`, `ensure_dataset`, `ensure_table`, `ensure_primary_key` | Impure | No | Side effects and external state dependence are unavoidable for network and storage operations |
