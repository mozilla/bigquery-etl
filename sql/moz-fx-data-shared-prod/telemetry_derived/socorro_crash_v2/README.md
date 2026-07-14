# socorro_crash_v2

Daily import of Socorro crash reports from GCS newline JSON into
`moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`, partitioned by `crash_date`.

`query.py` loads one day of JSON from
`gs://moz-fx-socorro-prod-prod-telemetry/v1/crash_report/<yyyymmdd>/` and writes
the corresponding `crash_date` partition. This replaces the Spark/Dataproc job
that used to do this (`https://github.com/mozilla/telemetry-airflow/blob/main/jobs/socorro_import_crash_data.py`).

## How it works

1. Load the day's JSON into a temporary table using a schema derived
   from `schema.yaml` at runtime
2. Run `transform.sql` to reshape the temp table into the destination table's
   layout and write the `crash_date` partition with `WRITE_TRUNCATE`

## Usage

```sh
python3 query.py --date 2026-01-01
```

Options: `--date` (required), `--project`, `--source-bucket`, `--source-prefix`,
`--destination-dataset`, `--destination-table`, `--dry-run`. `--dry-run` prints
the planned load (source URI, destination partition) without touching BigQuery.

## Migration notes

The original Spark job read GCS JSON, coerced it into a Spark `StructType`
derived from the JSON (`telemetry_socorro_crash.json`), and wrote
partitioned parquet to `gs://moz-fx-data-prod-socorro-data/socorro_crash/v2/`. A
separate `parquet2bigquery` GKE pod then loaded that parquet into this table.

Moving the job here removes the Spark cluster, the intermediate parquet, and the
separate load job. The load is a single BigQuery load plus one transform query.

### Schema

`schema.yaml` is a copy of the current production table schema, so the
migrated job targets the existing table shape without redefining it.  The schemas is based on
Schema is based on https://github.com/mozilla-services/socorro/blob/main/socorro/schemas/telemetry_socorro_crash.json.

Differences from the upstream schema:
  - `crash_date` (DATE) is the partition column. It is not present in
    the crash report JSON. The old pipeline derived it from the GCS folder name
    (`crash_date=<yyyymmdd>`); here it comes from `--date`.
  - Arrays are stored as `RECORD<list: ARRAY<RECORD<element>>>` rather than
    plain `REPEATED` fields. This is the encoding Spark's parquet writer
    produced, and downstream consumers (for example the symbolication jobs that
    read `json_dump.modules.list`) depend on it. `transform.sql` rewraps the
    following arrays to match:
    - `additional_minidumps` (element: STRING)
    - `addons` (element: STRING)
    - `json_dump.crashing_thread.frames`
    - `json_dump.modules`
    - `json_dump.threads` (and the nested `frames` inside each thread)
    - `memory_report.reports`

The upstream schema is unlikely to change so it's static instead of dynamically built.
