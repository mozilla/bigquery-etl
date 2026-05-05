"""Data review data downloaded from Bugzilla API, clean and upload to BigQuery."""

import logging
import re
from pathlib import Path

import click
import requests
import yaml
from google.cloud import bigquery

API_BASE_URL = "https://bugzilla.mozilla.org/rest/bug"
API_PARAMS = {
    "f1": "flagtypes.name",
    "o1": "substring",
    "v1": "data-review",
    "query_format": "advanced",
    "include_fields": ["id", "history"],
}

DEFAULT_PROJECT_ID = Path(__file__).parent.parent.parent.name
DEFAULT_DATASET_ID = Path(__file__).parent.parent.name
DEFAULT_TABLE_NAME = Path(__file__).parent.name

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields


def get_api_response(url, params):
    """Get json of response if the requests to the API was successful."""
    response = requests.get(url, params)
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


def download_data_review_stats(url, params):
    """Download bugs with data-review from Bugzilla API and parse the data."""
    bugzilla_response = get_api_response(url, params)
    bugs = bugzilla_response["bugs"]

    data_review_rows = []
    for bug in bugs:
        bug_id = bug["id"]
        update_counter = 0
        for changeset in bug["history"]:
            update_datetime = changeset["when"][:-1]
            who = changeset["who"]
            update_counter += 1
            for change in changeset["changes"]:
                if change["field_name"] != "flagtypes.name":
                    continue
                if "data-review+" in change["added"]:
                    data_review_rows.append(
                        {
                            "steward": who,
                            "action": "approval",
                            "bug_id": bug_id,
                            "update_datetime": update_datetime,
                            "attachment_id": change.get("attachment_id"),
                            "update_counter": update_counter,
                        }
                    )
                if "data-review+" in change["removed"]:
                    data_review_rows.append(
                        {
                            "steward": who,
                            "action": "revoke approval",
                            "bug_id": bug_id,
                            "update_datetime": update_datetime,
                            "attachment_id": change.get("attachment_id"),
                            "update_counter": update_counter,
                        }
                    )
                if "data-review-" in change["added"]:
                    data_review_rows.append(
                        {
                            "steward": who,
                            "action": "rejection",
                            "bug_id": bug_id,
                            "update_datetime": update_datetime,
                            "attachment_id": change.get("attachment_id"),
                            "update_counter": update_counter,
                        }
                    )
                if "data-review-" in change["removed"]:
                    data_review_rows.append(
                        {
                            "steward": who,
                            "action": "revoke rejection",
                            "bug_id": bug_id,
                            "update_datetime": update_datetime,
                            "attachment_id": change.get("attachment_id"),
                            "update_counter": update_counter,
                        }
                    )
                if "data-review?" in change["added"]:
                    stewards = re.findall(r"data-review\?\(([^)]+)\)", change["added"])
                    for steward in stewards:
                        data_review_rows.append(
                            {
                                "steward": steward,
                                "requestor": who,
                                "action": "request",
                                "bug_id": bug_id,
                                "update_datetime": update_datetime,
                                "attachment_id": change.get("attachment_id"),
                                "update_counter": update_counter,
                            }
                        )
                if "data-review?" in change["removed"]:
                    stewards = re.findall(
                        r"data-review\?\(([^)]+)\)", change["removed"]
                    )
                    for steward in stewards:
                        data_review_rows.append(
                            {
                                "steward": steward,
                                "requestor": who,
                                "action": "remove request",
                                "bug_id": bug_id,
                                "update_datetime": update_datetime,
                                "attachment_id": change.get("attachment_id"),
                                "update_counter": update_counter,
                            }
                        )
    return data_review_rows


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
def run_data_review_stats(bq_project_id, bq_dataset_id, bq_table_name):
    """Download experiments from the APIs and store it in BigQuery."""
    data_review_stats = download_data_review_stats(API_BASE_URL, API_PARAMS)
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    store_data_in_bigquery(
        data=data_review_stats,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_data_review_stats()
