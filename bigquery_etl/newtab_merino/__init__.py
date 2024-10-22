"""Extract query results and write the combined JSON to a single file."""

import json
import logging
from datetime import datetime, timedelta, timezone

import rich_click as click
from google.cloud import storage  # type: ignore
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command()
@click.option(
    "--source-project",
    required=True,
    help="Google Cloud Project where the source table is located.",
)
@click.option(
    "--source-dataset",
    required=True,
    help="Dataset in BigQuery where the source table is located.",
)
@click.option(
    "--source-table", required=True, help="Name of the source table in BigQuery."
)
@click.option(
    "--destination-bucket",
    required=True,
    help="Destination Google Cloud Storage Bucket.",
)
@click.option(
    "--destination-prefix", required=True, help="Prefix of the bucket path in GCS."
)
@click.option(
    "--destination-prefix", required=True, help="Prefix of the bucket path in GCS."
)
@click.option(
    "--deletion-days-old",
    required=True,
    type=int,
    help="Number of days after which files in GCS should be deleted.",
)
def export_newtab_merino_table_to_gcs(
    source_project: str,
    source_dataset: str,
    source_table: str,
    destination_bucket: str,
    destination_prefix: str,
    deletion_days_old: int,
):
    """Use bigquery client to export data from BigQuery to GCS."""
    client = bigquery.Client(source_project)
    error_counter = 0
    threshold = 1

    try:
        # Generate the current timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")

        # BigQuery does not export the proper JSON format, so we use a temp file and reformat
        temp_file = "temp.ndjson"

        job_config = bigquery.job.ExtractJobConfig(
            destination_format=bigquery.job.DestinationFormat.NEWLINE_DELIMITED_JSON
        )

        destination_uri = f"gs://{destination_bucket}/{destination_prefix}/{temp_file}"

        extract_job = client.extract_table(
            source=f"{source_project}.{source_dataset}.{source_table}",
            destination_uris=[destination_uri],
            job_config=job_config,
        )

        extract_job.result()  # Waits for the job to complete.

        # Verify that job was successful
        if extract_job.state != "DONE":
            log.error("Export failed with errors:", extract_job.errors)
            error_counter += 1

        # Initialize the storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(destination_bucket)
        blob = bucket.blob(f"{destination_prefix}/{temp_file}")

        # Read the temporary JSON file from GCS
        temp_file_content = blob.download_as_text()

        # Convert the content to a JSON array
        json_array = [json.loads(line) for line in temp_file_content.splitlines()]
        json_data = json.dumps(json_array, indent=1)

        # Write the JSON array to the final destination files in GCS:
        # 1. latest.json is a single file, that's easy to reference from Merino.
        # 2. {timestamp}.json keeps a historical record for debugging purposes.
        for suffix in ["latest", timestamp]:
            final_destination_uri = f"{destination_prefix}/{suffix}.json"
            final_blob = bucket.blob(final_destination_uri)
            final_blob.upload_from_string(json_data, content_type="application/json")

        # Delete the temporary file from GCS
        blob.delete()

        # Delete files older than 3 days
        delete_old_files(bucket, destination_prefix, deletion_days_old)

        log.info("Export successful and temporary file deleted")

    except Exception as err:
        error_counter += 1
        log.error(f"An error occurred: {err}")

        if error_counter > threshold:
            raise Exception(
                f"More than the accepted threshold of {threshold} operations failed."
            )


def delete_old_files(bucket, prefix, days_old):
    """Delete files older than `days_old` days from the bucket with the given prefix."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.updated < cutoff_date:
            blob.delete()
            log.info(f"Deleted {blob.name}")
