"""Suppression List downloaded from CampaignMonitor API and uploaded to BigQuery."""

from pathlib import Path
from typing import Any, Dict, List

import requests
import rich_click as click
from google.cloud import bigquery
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from bigquery_etl.schema import Schema

DEFAULT_PROJECT_ID = Path(__file__).parent.parent.parent.name
DEFAULT_DATASET_ID = Path(__file__).parent.parent.name
DEFAULT_TABLE_NAME = Path(__file__).parent.name

BASE_URL = "https://api.createsend.com/api/v3.3/clients"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()


def get_api_response(url: str, api_key: str):
    """Get json of response if the API request was successful."""
    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(api_key, "")

    response = requests.get(url=url, headers=headers, auth=auth)
    try:
        response.raise_for_status()
    except HTTPError as error:
        raise Exception(f"API request {url} failed: {response.text}") from error
    return response.json()


def store_data_in_bigquery(data, schema, destination_project, destination_table_id):
    """Upload data to Bigquery in a single, non partitioned table."""
    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        data, destination_table_id, location="US", job_config=job_config
    )
    load_job.result()
    click.echo(f"Loaded {len(data)} rows into {destination_table_id}.")


def download_suppression_list(
    base_url: str,
    client_id: str,
    api_key: str,
    page_size: int = 1000,
    order_field: str = "date",
    order_direction: str = "asc",
) -> List[Dict[str, Any]]:
    """Download suppression list from Campaign Monitor."""
    page = 1
    suppressions_campaign_monitor_v1 = []

    while True:
        suppression_list_url = (
            f"{base_url}/{client_id}/suppressionlist.json"
            + f"?page={page}&pagesize={page_size}"
            + f"&orderfield={order_field}&orderdirection={order_direction}"
        )
        json_response = get_api_response(url=suppression_list_url, api_key=api_key)
        for result in json_response["Results"]:
            suppressions_campaign_monitor_v1.append(
                {
                    "suppression_reason": result["SuppressionReason"],
                    "email": result["EmailAddress"],
                    "update_timestamp": result["Date"],
                    "state": result["State"],
                }
            )

        if json_response["PageNumber"] >= json_response["NumberOfPages"]:
            return suppressions_campaign_monitor_v1
        page += 1


@click.command
@click.option(
    "--api_key",
    required=True,
    help="Campaign Monitor API key to use for authentication.",
)
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
    "--client_id",
    required=True,
    help="Client Id for Campaign Monitor.",
)
def campaign_monitor_suppression_list_import_to_bigquery(
    api_key: str,
    bq_project_id: str,
    bq_dataset_id: str,
    bq_table_name: str,
    client_id: str,
):
    """Import Suppression List from Campaign Monitor."""
    suppressions = download_suppression_list(
        base_url=BASE_URL, client_id=client_id, api_key=api_key
    )
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    store_data_in_bigquery(
        data=suppressions,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )


if __name__ == "__main__":
    campaign_monitor_suppression_list_import_to_bigquery()
