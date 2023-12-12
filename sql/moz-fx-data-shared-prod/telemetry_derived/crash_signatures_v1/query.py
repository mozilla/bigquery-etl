"""Generate signatures for symbolicated crash pings via siggen."""
from pathlib import Path

import click
import yaml
from google.cloud import bigquery, bigquery_storage
from siggen.generator import SignatureGenerator

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
OUTPUT_SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields


@click.command()
@click.option(
    "--submission-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
)
@click.option(
    "--source-table",
    default="moz-fx-data-shared-prod.telemetry_derived.crash_symbolicated_v1",
)
@click.option(
    "--destination-table",
    default="moz-fx-data-shared-prod.telemetry_derived.crash_signatures_v1",
)
def main(
    submission_date,
    source_table,
    destination_table,
):
    sig_generator = SignatureGenerator()
    bq = bigquery.Client()
    bq_read = bigquery_storage.BigQueryReadClient()
    source = bq.get_table(source_table)
    session = bq_read.create_read_session(
        parent=f"projects/{source.project}",
        read_session=bigquery_storage.ReadSession(
            table=source.path.lstrip("/"),
            data_format=bigquery_storage.DataFormat.AVRO,
        ),
        max_stream_count=1,
    )
    reader = bq_read.read_rows(session.streams[0].name)
    json_rows = [
        {
            "submission_timestamp": crash["submission_timestamp"].isoformat(),
            "document_id": crash["document_id"],
            "signature": sig_generator.generate(crash).signature or None,
        }
        for crash in reader.rows(session)
    ]

    job = bq.load_table_from_json(
        json_rows=json_rows,
        destination=f'{destination_table}${submission_date.replace("-", "")}',
        job_config=bigquery.LoadJobConfig(
            ignore_unknown_values=False,
            schema=OUTPUT_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()


if __name__ == "__main__":
    main()
