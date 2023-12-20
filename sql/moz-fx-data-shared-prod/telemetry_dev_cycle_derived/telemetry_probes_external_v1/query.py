"""Telemetry probes data - download from API, clean and upload to BigQuery."""

import logging

import click
import requests
from google.cloud import bigquery

API_BASE_URL = "https://probeinfo.telemetry.mozilla.org"
SCHEMA = [
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("probe", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("release_version", "INT64"),
    bigquery.SchemaField("last_version", "INT64"),
    bigquery.SchemaField("expiry_version", "STRING"),
    bigquery.SchemaField("first_added_date", "DATE"),
]
DEFAULT_PROJECT_ID = "moz-fx-data-shared-prod"
DEFAULT_DATASET_ID = "telemetry_dev_cycle_derived"
DEFAULT_TABLE_NAME = "telemetry_probes_external_v1"
CHANNELS = ["release", "beta", "nightly"]


def get_api_response(url):
    """Get json of response if the requests to the API was successful."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def store_data_in_bigquery(data, schema, destination_project, destination_table_id):
    """Upload data to Bigquery in a single, non partitioned table."""
    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_json(
        data, destination_table_id, location="US", job_config=job_config
    )
    load_job.result()
    stored_table = client.get_table(destination_table_id)
    logging.info(f"Loaded {stored_table.num_rows} rows into {destination_table_id}.")


def download_telemetry_probes(url: str):
    """Download probes for telemetry and parse the data."""
    firefox_metrics = []
    for channel in CHANNELS:
        probes = get_api_response(f"{url}/firefox/{channel}/main/all_probes")
        for probe in probes.values():
            release_version = int(probe["history"][channel][-1]["versions"]["first"])
            last_version = int(probe["history"][channel][0]["versions"]["last"])
            expiry_version = probe["history"][channel][0]["expiry_version"]
            first_added_date = probe["first_added"][channel][:10]

            firefox_metrics.append(
                {
                    "channel": channel,
                    "probe": probe["name"],
                    "type": probe["type"],
                    "release_version": release_version,
                    "last_version": last_version,
                    "expiry_version": expiry_version,
                    "first_added_date": first_added_date,
                }
            )
    return firefox_metrics


@click.command
@click.option(
    "--bq_project_id",
    default=DEFAULT_PROJECT_ID,
    show_default=True,
    help="BigQuery project the data is written to.",
)
@click.option(
    "--bq_dataset_id",
    default=DEFAULT_DATASET_ID,
    show_default=True,
    help="BigQuery dataset the data is written to.",
)
@click.option(
    "--bq_table_name",
    default=DEFAULT_TABLE_NAME,
    show_default=True,
    help="Bigquery table the data is written to.",
)
def run_telemetry_probes(bq_project_id, bq_dataset_id, bq_table_name):
    """Download the data for telemetry probes from the API ad store it in BigQuery."""
    telemetry_probes = download_telemetry_probes(API_BASE_URL)
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    store_data_in_bigquery(
        data=telemetry_probes,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_telemetry_probes()
