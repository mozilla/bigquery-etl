"""Export referral install totals from BigQuery to a CSV file in GCS.

Supports the Firefox Referral Program (DENG-11237). The Website team syncs the
CSV into the Postgres DB behind the referral hub page on firefox.com.

Output format matches the Website team's request: `invite_code,total_installs`
(no header by default), one timestamped file per run named
`referral_data-YYYY-MM-DDZHH:MM:SS.csv`.
"""

import logging
from datetime import datetime, timezone

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
    required=True,
    help="Prefix of the bucket path in GCS.",
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
    include_header: bool,
):
    """Extract the referral totals table to a timestamped CSV file in GCS."""
    client = bigquery.Client(source_project)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dZ%H:%M:%S")
    filename = f"referral_data-{timestamp}.csv"
    destination_uri = f"gs://{destination_bucket}/{destination_prefix}/{filename}"

    job_config = bigquery.job.ExtractJobConfig(
        destination_format=bigquery.job.DestinationFormat.CSV,
        print_header=include_header,
    )

    extract_job = client.extract_table(
        source=f"{source_project}.{source_dataset}.{source_table}",
        destination_uris=[destination_uri],
        job_config=job_config,
    )
    extract_job.result()  # Waits for the job to complete.

    if extract_job.state != "DONE":
        raise Exception(f"Export failed with errors: {extract_job.errors}")

    log.info(f"Export successful: {destination_uri}")
