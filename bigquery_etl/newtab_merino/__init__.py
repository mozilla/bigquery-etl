"""Export data from BigQuery to GCS."""

import rich_click as click
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta, timezone
import json


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
def export_newtab_merino_extract_to_gcs(
    source_project,
    source_dataset,
    source_table,
    destination_bucket,
    destination_prefix,
):
    """Use bigquery client to export data from BigQuery to GCS."""
    client = bigquery.Client(source_project)

    # Generate the current timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")

    # BigQuery does not export the proper JSON format, so we use a temp file and reformat
    temp_file = "temp.json"

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
        print("Export failed with errors:", extract_job.errors)
        return

    # Initialize the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(destination_bucket)
    blob = bucket.blob(f"{destination_prefix}/{temp_file}")

    # Read the temporary JSON file from GCS
    temp_file_content = blob.download_as_text()

    # Convert the content to a JSON array
    json_array = [json.loads(line) for line in temp_file_content.splitlines()]

    # Write the JSON array to the final destination file in GCS
    final_destination_uri = f"{destination_prefix}/{timestamp}.json"
    final_blob = bucket.blob(final_destination_uri)
    final_blob.upload_from_string(
        json.dumps(json_array, indent=1), content_type="application/json"
    )

    # Delete the temporary file from GCS
    blob.delete()

    # Delete files older than 3 days
    delete_old_files(bucket, destination_prefix, 3)

    print("Export successful and temporary file deleted")


def delete_old_files(bucket, prefix, days_old):
    """Delete files older than `days_old` days from the bucket with the given prefix."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.time_created < cutoff_date:
            blob.delete()
            print(f"Deleted {blob.name}")


if __name__ == "__main__":
    export_newtab_merino_extract_to_gcs()
