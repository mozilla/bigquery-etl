"""Export referral install totals from BigQuery to a CSV file in GCS.

Supports the Firefox Referral Program (DENG-11237). The Website team syncs the
CSV into the Postgres DB behind the referral hub page on firefox.com.

Output format matches the Website team's request: `invite_code,total_installs`
(no header by default), one file per run date named `referral_data-<date>.csv`.
The run date (not wall-clock time) is used so an Airflow retry overwrites the
same object rather than accumulating a second file for the same logical date.
"""

import logging

import rich_click as click
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
    "--source-table",
    required=True,
    help="Name of the source table in BigQuery.",
)
@click.option(
    "--destination-bucket",
    required=True,
    help="Destination Google Cloud Storage bucket (name only, no gs:// prefix).",
)
@click.option(
    "--destination-prefix",
    required=False,
    default="",
    help="Optional prefix (subfolder) within the bucket. Omit to write to the "
    "bucket root.",
)
@click.option(
    "--date",
    required=True,
    help="Run date (YYYY-MM-DD), used in the output filename. Pass Airflow's "
    "{{ds}} so retries overwrite the same object instead of accumulating files.",
)
@click.option(
    "--include-header/--no-include-header",
    default=False,
    help="Whether to write a CSV header row. Defaults to no header per the "
    "Website team's requested format.",
)
def export_referral_totals_to_gcs(
    source_project: str,
    source_dataset: str,
    source_table: str,
    destination_bucket: str,
    destination_prefix: str,
    date: str,
    include_header: bool,
):
    """Extract the referral totals table to a CSV file in GCS, named by run date."""
    client = bigquery.Client(source_project)

    filename = f"referral_data-{date}.csv"
    object_path = (
        f"{destination_prefix.rstrip('/')}/{filename}"
        if destination_prefix
        else filename
    )
    destination_uri = f"gs://{destination_bucket}/{object_path}"

    job_config = bigquery.job.ExtractJobConfig(
        destination_format=bigquery.job.DestinationFormat.CSV,
        print_header=include_header,
    )

    extract_job = client.extract_table(
        source=f"{source_project}.{source_dataset}.{source_table}",
        destination_uris=[destination_uri],
        job_config=job_config,
    )
    try:
        extract_job.result()  # Waits for the job to complete.
    except Exception as e:
        raise click.ClickException(
            f"Export to {destination_uri} failed: {extract_job.errors}"
        ) from e

    log.info(f"Export successful: {destination_uri}")
