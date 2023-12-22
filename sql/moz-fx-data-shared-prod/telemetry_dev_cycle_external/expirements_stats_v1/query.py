"""Experiment data downloaded from APIs, clean and upload to BigQuery."""

import datetime
import logging
import click
import requests
from pathlib import Path
import yaml
from google.cloud import bigquery

API_BASE_URL_EXPERIMENTS = "https://experimenter.services.mozilla.com"
API_BASE_URL_METRIC_HUB = "https://github.com/mozilla/metric-hub/tree/main/jetstream"

DEFAULT_PROJECT_ID = Path(__file__).parent.parent.parent.name
DEFAULT_DATASET_ID = Path(__file__).parent.parent.name
DEFAULT_TABLE_NAME = Path(__file__).parent.name

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields


def parse_unix_datetime_to_string(unix_string):
    """Parse unix_string with milliseconds to date string."""
    if not unix_string:
        return None
    return datetime.datetime.fromtimestamp(int(unix_string) // 1000).strftime(
        "%Y-%m-%d"
    )


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


def download_experiments_v1(url):
    """Download experiment data from API v1 and parse it."""
    experiments_v1 = []
    experiments = get_api_response(f"{url}/api/v1/experiments")
    for experiment in experiments:
        if experiment["status"] == "Draft":
            continue
        experiments_v1.append(
            {
                "slug": experiment["slug"],
                "start_date": parse_unix_datetime_to_string(experiment["start_date"]),
                "enrollment_end_date": None,
                "end_date": parse_unix_datetime_to_string(experiment["end_date"]),
            }
        )
    logging.info(
        f"Downloaded {len(experiments_v1)} records from the experiments v1 API"
    )
    return experiments_v1


def download_experiments_v6(url):
    """Download experiment data from API v6 and parse it."""
    experiments_v6 = []
    experiments = get_api_response(f"{url}/api/v6/experiments")
    for experiment in experiments:
        experiments_v6.append(
            {
                "slug": experiment["slug"],
                "start_date": experiment["startDate"],
                "enrollment_end_date": experiment["enrollmentEndDate"],
                "end_date": experiment["endDate"],
            }
        )
    logging.info(
        f"Downloaded {len(experiments_v6)} records from the experiments v6 API"
    )
    return experiments_v6


def download_metric_hub_files(url):
    """Download metric hub files from github."""
    metric_files = {}
    files = get_api_response(url)
    for file in files["payload"]["tree"]["items"]:
        if file["contentType"] != "file":
            continue
        slug = file["name"].removesuffix(".toml")
        metric_files[slug] = True
    logging.info(f"Downloaded {len(metric_files)} records from the API {url}")
    return metric_files


def compare_experiments_with_metric_hub_configs():
    """Download experiments from v1 and v6 API and compare them with config files in metric_hub."""
    experiments_v1 = download_experiments_v1(API_BASE_URL_EXPERIMENTS)
    experiments_v6 = download_experiments_v6(API_BASE_URL_EXPERIMENTS)
    metric_files = download_metric_hub_files(API_BASE_URL_METRIC_HUB)

    experiments = [
        {**experiment, "has_config": True}
        if experiment["slug"] in metric_files
        else {**experiment, "has_config": False}
        for experiment in (experiments_v1 + experiments_v6)
    ]
    return experiments


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
def run_experiments_stats(bq_project_id, bq_dataset_id, bq_table_name):
    """Download experiments from the APIs and store it in BigQuery."""
    experiments = compare_experiments_with_metric_hub_configs()
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    store_data_in_bigquery(
        data=experiments,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_experiments_stats()
