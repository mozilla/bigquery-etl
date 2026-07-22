#!/usr/bin/env python3

"""Load a day of Socorro crash reports from GCS JSON into socorro_crash_v2."""

import os
import uuid
from pathlib import Path

import click
from google.cloud import bigquery, storage

from bigquery_etl.schema import Schema

THIS_DIR = os.path.dirname(__file__)
DEFAULT_SCHEMA = os.path.join(THIS_DIR, "schema.yaml")
TRANSFORM_SQL = os.path.join(THIS_DIR, "transform.sql")


def load_source_schema():
    """Build the schema to load the raw JSON with.

    This is the table schema minus crash_date, with the wrapped arrays flattened
    back to their natural JSON shape. It is derived from schema.yaml at runtime
    so the load schema and the destination schema never drift apart.
    """
    # Use the repo's schema loader so !include-field-description and the other
    # include tags in schema.yaml are resolved
    fields = Schema.from_schema_file(Path(DEFAULT_SCHEMA)).schema["fields"]
    return [
        bigquery.SchemaField.from_api_repr(_unwrap(field))
        for field in fields
        if field["name"] != "crash_date"
    ]


def _unwrap(field):
    """Flatten a wrapped array back to the plain array the JSON contains.

    A field stored as RECORD<list: ARRAY<RECORD<element>>> becomes the plain
    REPEATED field the source JSON actually has, recursively.
    """
    is_wrapped = field["type"] == "RECORD" and [
        f["name"] for f in field.get("fields", [])
    ] == ["list"]
    if is_wrapped:
        element = field["fields"][0]["fields"][0]
        if element["type"] == "RECORD":
            inner = [_unwrap(f) for f in element.get("fields", [])]
            return {
                "name": field["name"],
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": inner,
            }
        return {"name": field["name"], "type": element["type"], "mode": "REPEATED"}
    if field.get("fields"):
        return {**field, "fields": [_unwrap(f) for f in field["fields"]]}
    return field


@click.command(help=__doc__)
@click.option(
    "--date",
    "date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Partition date to load, e.g. 2019-08-01.",
)
@click.option("--project", default="moz-fx-data-shared-prod")
@click.option("--source-bucket", default="moz-fx-socorro-prod-prod-telemetry")
@click.option(
    "--source-prefix",
    default="v1/crash_report",
    help="Source prefix without the date folder.",
)
@click.option("--destination-dataset", default="telemetry_derived")
@click.option("--destination-table", default="socorro_crash_v2")
@click.option(
    "--dry-run",
    is_flag=True,
    help=(
        "Smoke test the load: build the load schema, confirm source objects "
        "exist in GCS, load the temp table, and validate the transform against "
        "it without writing the destination partition."
    ),
)
def main(
    date,
    project,
    source_bucket,
    source_prefix,
    destination_dataset,
    destination_table,
    dry_run,
):
    """Load one crash_date partition of GCS JSON into BigQuery."""
    date = date.date()

    date_nodash = date.strftime("%Y%m%d")
    source_prefix_with_date = f"{source_prefix}/{date_nodash}/"
    source_uri = f"gs://{source_bucket}/{source_prefix_with_date}*"
    partition = ".".join(
        [
            project,
            destination_dataset,
            f"{destination_table}${date_nodash}",
        ]
    )
    with open(TRANSFORM_SQL) as f:
        transform = f.read()

    client = bigquery.Client(project)

    if dry_run:
        # Build the load schema locally
        schema = load_source_schema()
        print(f"Load schema built: {len(schema)} top-level fields")

        # Confirm the date folder actually holds objects before loading
        storage_client = storage.Client(project)
        blobs = storage_client.list_blobs(
            source_bucket, prefix=source_prefix_with_date, max_results=1
        )
        if next(iter(blobs), None) is None:
            raise click.ClickException(f"No source objects found under {source_uri}")
        print(f"Source objects present under {source_uri}")
    else:
        schema = load_source_schema()

    # Load the raw JSON into a temp table using the natural (unwrapped) schema.
    tmp_table = f"tmp.socorro_crash_{uuid.uuid4().hex[:8]}"
    load_result = client.load_table_from_uri(
        source_uri,
        tmp_table,
        location="US",
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # Ignore fields present in the JSON but absent from the schema.
            ignore_unknown_values=True,
        ),
    ).result()
    print(f"Loaded {load_result.output_rows} rows into {tmp_table}")

    try:
        query = transform.format(source_table=tmp_table, crash_date=date.isoformat())

        if dry_run:
            # Dry run the transform against the real temp table without writing
            validate_result = client.query(
                query,
                job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
            )
            print(
                f"Transform validates; would scan "
                f"{validate_result.total_bytes_processed} bytes. "
                f"Skipping write to {partition}."
            )
            return

        # Transform into the destination shape and write the partition.
        query_result = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                destination=partition,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()
        print(f"Wrote {query_result.total_rows} rows into {partition}")
    finally:
        client.delete_table(tmp_table, not_found_ok=True)


if __name__ == "__main__":
    main()
