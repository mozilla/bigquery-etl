"""Extract query results and write the combined JSON to a single file."""

import json
import logging
from datetime import datetime, timedelta, timezone

import rich_click as click
from google.cloud import storage  # type: ignore
from google.cloud import bigquery

from bigquery_etl.newtab_merino.ctrpred.config import GB_CTRPRED_CONFIG
from bigquery_etl.newtab_merino.ctrpred.infer_ctr import predict_log_odds
from bigquery_etl.newtab_merino.ctrpred.pseudo_counts import log_odds_to_pseudo_counts
from bigquery_etl.newtab_merino.ctrpred.timeline import query_timeline_data
from bigquery_etl.newtab_merino.ctrpred.utils import (
    build_model_input,
    replace_ctrpred_treatment_rows,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CTR_PRED_TREATMENT_REGION = GB_CTRPRED_CONFIG.artifact_region
CTR_PRED_SOURCE_TABLE = "newtab_merino_extract_v3"


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
    multiple=True,
    help="Destination Google Cloud Storage Bucket. Can be specified multiple times.",
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
    destination_bucket: tuple,
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

        # Use the first bucket for the initial export
        primary_bucket = destination_bucket[0]
        destination_uri = f"gs://{primary_bucket}/{destination_prefix}/{temp_file}"

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

        # Read the temporary file from the primary bucket
        primary_bucket_obj = storage_client.bucket(primary_bucket)
        blob = primary_bucket_obj.blob(f"{destination_prefix}/{temp_file}")
        temp_file_content = blob.download_as_text()

        # Convert the content to a JSON array
        json_array = [json.loads(line) for line in temp_file_content.splitlines()]

        if source_table == CTR_PRED_SOURCE_TABLE:
            end_time = floor_to_ten_minutes(datetime.now(timezone.utc))
            json_array = apply_ctrpred_postprocessing_safely(
                json_array, client, end_time
            )

        json_data = json.dumps(json_array, indent=1)

        # Write to all destination buckets
        for bucket_name in destination_bucket:
            bucket = storage_client.bucket(bucket_name)

            # Write the JSON array to the final destination files in GCS:
            # 1. latest.json is a single file, that's easy to reference from Merino.
            # 2. {timestamp}.json keeps a historical record for debugging purposes.
            for suffix in ["latest", timestamp]:
                final_destination_uri = f"{destination_prefix}/{suffix}.json"
                final_blob = bucket.blob(final_destination_uri)
                final_blob.upload_from_string(
                    json_data, content_type="application/json"
                )

            # Delete files older than specified days
            delete_old_files(bucket, destination_prefix, deletion_days_old)

            log.info(f"Export successful to bucket {bucket_name}")

        # Delete the temporary file from the primary bucket
        blob.delete()

        log.info("Temporary file deleted")

    except Exception as err:
        error_counter += 1
        log.error(f"An error occurred: {err}")

        if error_counter > threshold:
            raise Exception(
                f"More than the accepted threshold of {threshold} operations failed."
            )


def floor_to_ten_minutes(timestamp):
    """Floor a timestamp to the start of its ten-minute interval."""
    return timestamp.replace(
        minute=timestamp.minute // 10 * 10, second=0, microsecond=0
    )


def apply_ctrpred_postprocessing(json_array, client, end_time):
    """Replace CTR prediction treatment rows in a Merino artifact."""
    replacement_rows = [
        row for row in json_array if row["region"] == CTR_PRED_TREATMENT_REGION
    ]
    if not replacement_rows:
        return json_array

    replacement_item_ids = [row["corpus_item_id"] for row in replacement_rows]

    timeline_df = query_timeline_data(
        client,
        replacement_item_ids,
        end_time,
        region=GB_CTRPRED_CONFIG.region,
        experiment_slug=GB_CTRPRED_CONFIG.experiment_slug,
        experiment_branch=GB_CTRPRED_CONFIG.experiment_branch,
    )
    replacement_item_ids, replacement_timelines = build_model_input(timeline_df)
    replacement_keys = [
        (corpus_item_id, CTR_PRED_TREATMENT_REGION)
        for corpus_item_id in replacement_item_ids
    ]
    forecast_slot = end_time.hour * 6 + end_time.minute // 10
    log_odds = predict_log_odds(
        replacement_timelines,
        forecast_slot,
        GB_CTRPRED_CONFIG.actr,
    )
    pseudo_counts = log_odds_to_pseudo_counts(
        log_odds.mean,
        log_odds.variance,
        GB_CTRPRED_CONFIG.pseudo_counts,
    )
    return replace_ctrpred_treatment_rows(json_array, replacement_keys, pseudo_counts)


def apply_ctrpred_postprocessing_safely(json_array, client, end_time):
    """Apply CTR prediction, falling back to the original artifact on failure."""
    try:
        return apply_ctrpred_postprocessing(json_array, client, end_time)
    except Exception as err:
        log.exception("CTR prediction postprocessing failed: %s", err)
        return json_array


def delete_old_files(bucket, prefix, days_old):
    """Delete files older than `days_old` days from the bucket with the given prefix."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.updated < cutoff_date:
            blob.delete()
            log.info(f"Deleted {blob.name}")
