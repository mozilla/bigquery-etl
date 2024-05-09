"""Import Braze Currents from GCS into BigQuery."""

import rich_click as click
from google.cloud import bigquery


@click.command()
@click.option(
    "--destination-project",
    required=True,
    help="Google Cloud Project the table is saved to.",
)
@click.option(
    "--destination-dataset",
    required=True,
    help="Dataset in BigQuery the table is saved to.",
)
@click.option(
    "--destination-table", required=True, help="Name of the table in BigQuery."
)
@click.option("--source-bucket", required=True, help="Google Cloud Storage Bucket ")
@click.option("--source-prefix", required=True, help="Prefix of the path in GSC.")
@click.option("--event-type", required=True, help="Eventtype for the table.")
def import_braze_current_from_bucket(
    destination_project,
    destination_dataset,
    destination_table,
    source_bucket,
    source_prefix,
    event_type,
):
    """Use bigquery client to store AVRO files from bucket in BigQuery."""
    client = bigquery.Client(destination_project)
    uri = f"gs://{source_bucket}/{source_prefix}/event_type={event_type}/*"
    client.load_table_from_uri(
        uri,
        destination=f"{destination_project}.{destination_dataset}.{destination_table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.AVRO,
        ),
    ).result()
