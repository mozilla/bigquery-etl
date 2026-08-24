"""Shared loader for Plausible's daily raw parquet exports.

Plausible writes two files per day for firefox.com into the bucket provisioned
by DENG-11379:

    gs://moz-fx-data-prod-external-plausible-data/firefox-com/<YYYY-MM-DD>/events.parquet
    gs://moz-fx-data-prod-external-plausible-data/firefox-com/<YYYY-MM-DD>/sessions.parquet

Each file maps 1:1 onto one day partition of its destination table -- events by
event `timestamp`, sessions by session `start` -- so a run replaces exactly one
partition and reruns are idempotent. README.md in this directory documents the
export semantics this relies on, and the midnight window-split caveat.

Both tables load the same way: stage the parquet in a temp table, assert the
file covers only the requested date, then select explicitly-typed columns into
the destination partition. Staging first keeps the type mapping under our
control instead of BigQuery's parquet autodetection.

Staging is not a plain load, because BigQuery cannot read these files as
delivered: Plausible stores `session_id` as an unsigned 64-bit hash, and
BigQuery reads a parquet UINT_64 column into a signed INT64 and fails the whole
load once a value exceeds the INT64 maximum. No load-job setting retargets it --
only DECIMAL logical types can be redirected to NUMERIC -- so the file is
rewritten with its unsigned 64-bit columns cast to string before it is uploaded.
Each table's typed SELECT then converts them, which is where every other type
decision already lives. See _reencode_unsigned_64bit.
"""

import tempfile
import uuid
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage  # type: ignore[attr-defined]

GCS_BUCKET = "moz-fx-data-prod-external-plausible-data"
GCS_PREFIX = "firefox-com"

DEFAULT_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_DATASET = "plausible_external"
TMP_DATASET = "tmp"

# Plausible only exports the firefox.com property into this bucket. Another
# site_id appearing would mean the export covers a property we haven't
# reviewed, so it's surfaced rather than silently mixed into the table.
EXPECTED_SITE_ID = 86997652


def parse_args(description, argv=None):
    """Parse the CLI arguments shared by both tables' entrypoints."""
    parser = ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        required=True,
        help="Plausible export date (YYYY-MM-DD), i.e. Airflow's {{ds}}.",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument(
        "--tmp-dataset",
        default=TMP_DATASET,
        help="Dataset to stage the raw parquet in before typing it.",
    )
    parser.add_argument(
        "--destination-table",
        default=None,
        help=(
            "Fully qualified project.dataset.table to write instead of the "
            "table this script normally owns. `bqetl backfill` passes this to "
            "redirect a run into backfills_staging_derived, and it is also the "
            "way to test a load into a scratch dataset."
        ),
    )
    parser.add_argument("--source-bucket", default=GCS_BUCKET)
    parser.add_argument("--source-prefix", default=GCS_PREFIX)
    args = parser.parse_args(argv)
    args.date = datetime.strptime(args.date, "%Y-%m-%d").date()
    return args


def _require_source_blob(args, source_file):
    """Return the source blob's GCS URI, failing if the export hasn't landed.

    Plausible pushes each day's files on its own schedule, so a missing file is
    the expected failure when an export is late or has stopped. Checking up
    front turns that into an actionable message instead of BigQuery's generic
    "no files matched URI".
    """
    blob_path = f"{args.source_prefix}/{args.date.isoformat()}/{source_file}"
    blob = storage.Client().bucket(args.source_bucket).get_blob(blob_path)
    if blob is None:
        raise FileNotFoundError(
            f"gs://{args.source_bucket}/{blob_path} does not exist -- Plausible "
            f"has not delivered the {args.date.isoformat()} export yet. Rerun "
            "this task once the file lands; no partition has been modified."
        )
    print(
        f"Found gs://{args.source_bucket}/{blob_path} "
        f"({blob.size} bytes, updated {blob.updated})"
    )
    return blob


def _reencode_unsigned_64bit(source_path, destination_path):
    """Rewrite a parquet file with its unsigned 64-bit columns cast to string.

    BigQuery reads a parquet UINT_64 column into a signed INT64 and fails the
    load as soon as a value exceeds the INT64 maximum, which Plausible's
    `session_id` hash routinely does. Nothing in the load job overrides that, so
    the column has to be made readable before upload. String is lossless and
    needs no precision negotiation, and each table's typed SELECT already casts
    these columns to their final types.

    Row groups are streamed one at a time, so peak memory tracks the largest row
    group rather than the whole file.

    Returns the names of the columns that were cast, empty if the file had none
    and can be uploaded untouched.
    """
    reader = pq.ParquetFile(source_path)
    unsigned = [
        field.name for field in reader.schema_arrow if pa.types.is_uint64(field.type)
    ]
    if not unsigned:
        return []

    target_schema = pa.schema(
        [
            field.with_type(pa.string()) if field.name in unsigned else field
            for field in reader.schema_arrow
        ]
    )
    with pq.ParquetWriter(destination_path, target_schema) as writer:
        for batch in reader.iter_batches():
            writer.write_table(pa.Table.from_batches([batch]).cast(target_schema))
    return unsigned


def _stage_source(bq_client, blob, tmp_table):
    """Download the export, make it readable, and load it into a temp table."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with tempfile.TemporaryDirectory() as workdir:
        delivered = Path(workdir) / "delivered.parquet"
        blob.download_to_filename(delivered)

        reencoded = Path(workdir) / "reencoded.parquet"
        cast_columns = _reencode_unsigned_64bit(delivered, reencoded)
        if cast_columns:
            print(f"Cast unsigned 64-bit column(s) to string: {cast_columns}")
            upload = reencoded
        else:
            upload = delivered

        with open(upload, "rb") as source:
            load_job = bq_client.load_table_from_file(
                source, tmp_table, job_config=job_config
            ).result()
    print(f"Staged {load_job.output_rows} rows into {tmp_table}")


def _check_staged(bq_client, tmp_table, expected_date, partition_field, duplicate_key):
    """Validate the staged file before it replaces a destination partition.

    Raises when the file is empty, spans a date other than `expected_date`, or
    covers a site other than firefox.com. The first two would write rows into
    the wrong partition or blank a good one; the third would quietly mix another
    property's traffic into tables documented as firefox.com-only. Duplicate
    keys that the vendor is supposed to have removed are reported rather than
    raised, because the typed SELECT already resolves them.
    """
    duplicates = f"COUNT(*) - COUNT(DISTINCT {duplicate_key})" if duplicate_key else "0"
    checks = f"""
        SELECT
          COUNT(*) AS total_rows,
          COUNTIF(DATE(`{partition_field}`) <> DATE '{expected_date}')
            AS out_of_range_rows,
          MIN(`{partition_field}`) AS min_value,
          MAX(`{partition_field}`) AS max_value,
          -- SAFE_CAST because site_id is staged as a string: it is one of the
          -- unsigned 64-bit columns re-encoded to make the file loadable.
          -- IS DISTINCT FROM, not <>, because COUNTIF only counts TRUE: a NULL
          -- or non-numeric site_id casts to NULL and would slip through the
          -- comparison silently. Those are exactly the vendor changes this
          -- check exists to catch.
          COUNTIF(SAFE_CAST(site_id AS INT64) IS DISTINCT FROM {EXPECTED_SITE_ID})
            AS unexpected_site_rows,
          {duplicates} AS duplicate_rows
        FROM
          `{tmp_table}`
    """
    row = next(iter(bq_client.query(checks).result()))

    if row.total_rows == 0:
        raise ValueError(
            f"{tmp_table} is empty. firefox.com always records traffic, so an "
            f"empty {expected_date} export means the vendor job failed upstream. "
            f"Refusing to replace the {expected_date} partition with 0 rows."
        )

    print(
        f"Staged {row.total_rows} rows, `{partition_field}` in "
        f"[{row.min_value}, {row.max_value}]"
    )

    if row.out_of_range_rows:
        raise ValueError(
            f"{row.out_of_range_rows} of {row.total_rows} staged rows have "
            f"`{partition_field}` outside {expected_date} (range "
            f"[{row.min_value}, {row.max_value}]). Each export file is expected "
            f"to cover exactly one day, and this load replaces only the "
            f"{expected_date} partition, so those rows would be dropped. "
            "Plausible may have changed how it windows the export."
        )

    if row.unexpected_site_rows:
        raise ValueError(
            f"{row.unexpected_site_rows} of {row.total_rows} staged rows have a "
            f"site_id that is not {EXPECTED_SITE_ID} (firefox.com), or that is "
            "null or non-numeric. These tables, "
            "and the schema.yaml description of every site_id column, are scoped "
            "to firefox.com, so loading them would inflate every downstream "
            "count. Plausible has likely added a property to this export: decide "
            "whether it belongs here, in its own table, or filtered out, rather "
            "than letting it land unnoticed."
        )

    if row.duplicate_rows:
        print(
            f"WARNING: {row.duplicate_rows} duplicate {duplicate_key} rows in the "
            "source file. Plausible is expected to deduplicate these; the load "
            "keeps the most recently updated row per key."
        )


def _load_partition(bq_client, tmp_table, destination, select_sql):
    """Replace a single day partition with the typed source rows."""
    job = bq_client.query(
        select_sql.format(tmp_table=tmp_table),
        job_config=bigquery.QueryJobConfig(
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()
    print(f"Loaded {job.total_rows} rows into {destination}")


def resolve_destination(args, table):
    """Return the partition to write, as `project.dataset.table$YYYYMMDD`.

    `--destination-table` wins when set, which is how `bqetl backfill` points a
    run at a table in backfills_staging_derived instead of the production one.
    The date decorator is appended either way, so a backfill fills the staging
    table one partition at a time and stays as rerunnable as a scheduled run.
    """
    target = args.destination_table or f"{args.project}.{args.dataset}.{table}"
    if target.count(".") != 2:
        raise ValueError(
            f"--destination-table must be a fully qualified "
            f"project.dataset.table, got {target!r}."
        )
    return f"{target}${args.date.strftime('%Y%m%d')}"


def run(args, *, table, source_file, partition_field, select_sql, duplicate_key=None):
    """Load one day of a Plausible export into one partition of `table`.

    `select_sql` is formatted with `tmp_table` and must project the columns of
    `table`'s schema.yaml, in order, with explicit types.
    """
    destination = resolve_destination(args, table)
    blob = _require_source_blob(args, source_file)

    bq_client = bigquery.Client(args.project)
    tmp_table = (
        f"{args.project}.{args.tmp_dataset}"
        f".plausible_{table}_{uuid.uuid4().hex[:8]}"
    )

    try:
        _stage_source(bq_client, blob, tmp_table)
        _check_staged(bq_client, tmp_table, args.date, partition_field, duplicate_key)
        _load_partition(bq_client, tmp_table, destination, select_sql)
    finally:
        bq_client.delete_table(tmp_table, not_found_ok=True)
