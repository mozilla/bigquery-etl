"""Glean metric data - download from API, clean and upload to BigQuery."""

import logging
from pathlib import Path

import click
import requests
import yaml
from google.cloud import bigquery

API_BASE_URL = "https://probeinfo.telemetry.mozilla.org"

DEFAULT_PROJECT_ID = Path(__file__).parent.parent.parent.name
DEFAULT_DATASET_ID = Path(__file__).parent.parent.name
DEFAULT_TABLE_NAME = Path(__file__).parent.name
DEFAULT_BAD_REQUEST_THRESHOLD = 5

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields


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


def download_glean_metrics(url, threshold):
    """Download metrics for glean products and parse the data."""
    # get a list of all glean apps
    glean_apps_response = get_api_response(f"{url}/glean/repositories")
    glean_apps = [glean_app["name"] for glean_app in glean_apps_response]

    glean_metrics = []
    error_counter = 0
    for glean_app in glean_apps:
        try:
            metrics = get_api_response(f"{url}/glean/{glean_app}/metrics")
            for name, metric in metrics.items():
                first_seen = metric["history"][0]["dates"]["first"][:10]
                last_seen = metric["history"][-1]["dates"]["last"][:10]
                expires = metric["history"][-1]["expires"]
                glean_metrics.append(
                    {
                        "glean_app": glean_app,
                        "metric": name,
                        "type": metric["history"][-1]["type"],
                        "first_seen_date": first_seen,
                        "last_seen_date": last_seen,
                        "expires": expires,
                    }
                )
        except requests.exceptions.HTTPError as err:
            error_counter += 1
            logging.error(err)
        if error_counter > threshold:
            raise Exception(
                f"More then the accepted threshold of {threshold} requests failed."
            )
    return glean_metrics


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
@click.option(
    "--threshold",
    default=DEFAULT_BAD_REQUEST_THRESHOLD,
    show_default=True,
    help="Number of bad Requests to get metrics for glean apps before the job fails.",
)
def run_glean_metrics(bq_project_id, bq_dataset_id, bq_table_name, threshold):
    """Download the data from the API and store it in BigQuery."""
    glean_metrics = download_glean_metrics(API_BASE_URL, threshold)

    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    store_data_in_bigquery(
        data=glean_metrics,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_glean_metrics()
