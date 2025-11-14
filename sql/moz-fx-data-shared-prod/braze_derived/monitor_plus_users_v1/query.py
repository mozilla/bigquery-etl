"""Import Add-ons Developers Users user data from GCS into BigQuery."""

import rich_click as click
from google.cloud import bigquery

from bigquery_etl.cli.query import schema


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
@click.option(
    "--source-files",
    required=True,
    help="Comma-separated list of files to be ingested (without .csv extension)",
)
def import_file_from_bucket(
    destination_project,
    destination_dataset,
    destination_table,
    source_bucket,
    source_prefix,
    source_files,
):
    """Use bigquery client to ingest CSV files from bucket in BigQuery."""
    client = bigquery.Client(destination_project)
    file_list = [f.strip() for f in source_files.split(",")]
    uris = [f"gs://{source_bucket}/{source_prefix}/{file}.csv" for file in file_list]
    client.load_table_from_uri(
        uris,
        destination=f"{destination_project}.{destination_dataset}.{destination_table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.CSV,
            schema=schema,
            skip_leading_rows=1,
        ),
    ).result()


if __name__ == "__main__":
    import_file_from_bucket()
