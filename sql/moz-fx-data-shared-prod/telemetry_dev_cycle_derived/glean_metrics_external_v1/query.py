"""Glean metric data - download from API, clean and upload to BigQuery."""

import logging

import click
import requests
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

API_BASE_URL = "https://probeinfo.telemetry.mozilla.org"
SCHEMA = [
    bigquery.SchemaField("glean_app", "STRING"),
    bigquery.SchemaField("metric", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("first_seen_date", "DATE"),
    bigquery.SchemaField("last_seen_date", "DATE"),
    bigquery.SchemaField("expires", "STRING"),
]
PROJECT_ID = "moz-fx-data-shared-prod"
DATASET_ID = "telemetry_dev_cycle_derived"
TABLE_NAME = "glean_metrics_external_v1"


def get_api_response(url):
    """Get json of response if the requests to the API was successful."""
    response = requests.get(url)

    if response.status_code != 200:
        logging.error(
            f"Failed to download data from {url}. \nResponse status code {response.status_code}."
        )
        return

    return response.json()


def download_glean_metrics(url: str):
    """Download metrics for glean products and parse the data."""
    # get a list of all glean apps
    glean_apps_response = get_api_response(f"{url}/glean/repositories")
    glean_apps = [glean_app["name"] for glean_app in glean_apps_response]

    glean_metrics = []
    for glean_app in glean_apps:
        if metrics := get_api_response(f"{url}/glean/{glean_app}/metrics"):
            for name, metric in metrics.items():
                first_seen = metric["history"][0]["dates"]["first"][:10]
                last_seen = metric["history"][-1]["dates"]["last"][:10]
                expires = metric["history"][0]["expires"]
                glean_metrics.append(
                    {
                        "glean_app": glean_app,
                        "metric": name,
                        "type": metric["history"][0]["type"],
                        "first_seen_date": first_seen,
                        "last_seen_date": last_seen,
                        "expires": expires,
                    }
                )
    return glean_metrics


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

    try:
        load_job.result()
    except BadRequest as ex:
        for e in load_job.errors:
            logging.error(f"Error: {e['message']}")
        raise ex
    stored_table = client.get_table(destination_table_id)
    logging.info(f"Loaded {stored_table.num_rows} rows into {destination_table_id}.")


@click.command
@click.option(
    "--bq_project_id",
    default=PROJECT_ID,
    show_default=True,
    help="BigQuery project the data is written to.",
)
@click.option(
    "--bq_dataset_id",
    default=DATASET_ID,
    show_default=True,
    help="BigQuery dataset the data is written to.",
)
@click.option(
    "--bq_table_name",
    default=TABLE_NAME,
    show_default=True,
    help="Bigquery table the data is written to.",
)
def run_glean_metrics(bq_project_id, bq_dataset_id, bq_table_name):
    """Download the data from the API and store it in BigQuery."""
    glean_metrics = download_glean_metrics(API_BASE_URL)

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
