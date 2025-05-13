"""Import historical newtab telemetry from GCS into BigQuery."""

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
@click.option("--source-file", required=True, help="Name of file to be ingested")
def import_file_from_bucket(
    destination_project,
    destination_dataset,
    destination_table,
    source_bucket,
    source_prefix,
    source_file,
):
    """Use bigquery client to ingest PARQUET files from bucket in BigQuery."""
    client = bigquery.Client(destination_project)
    uri = f"gs://{source_bucket}/{source_prefix}/{source_file}.PARQUET"
    client.load_table_from_uri(
        uri,
        destination=f"{destination_project}.{destination_dataset}.{destination_table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.PARQUET,
        ),
    ).result()


if __name__ == "__main__":
    import_file_from_bucket()
