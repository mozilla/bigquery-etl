"""Cinder API Addon Moderations Decisions - download decisions, clean and upload to BigQuery."""

import json
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta

import requests
from google.cloud import bigquery

"""Get the bearer token for Cinder from the environment"""
cinder_bearer_token = os.environ.get("CINDER_TOKEN")


def post_response(url, headers, data):
    """POST response function."""
    response = requests.post(url, headers=headers, data=data)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response


def get_response(url, headers, params):
    """GET response function."""
    response = requests.get(url, headers=headers, params=params)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response.json()


def read_json(filename: str) -> dict:
    """Read JSON file."""
    with open(filename, "r") as f:
        data = json.loads(f.read())
    return data


def cinder_addon_decisions_download(date, bearer_token):
    """Download data from Cinder - bearer_token is called here."""
    submission_date = datetime.strptime(date, "%Y-%m-%d")
    start_datetime = submission_date + timedelta(days=-1)
    start_date = start_datetime.strftime("%Y-%m-%d")
    end_datetime = submission_date + timedelta(days=1)
    end_date = end_datetime.strftime("%Y-%m-%d")
    url = "https://stage.cinder.nonprod.webservices.mozgcp.net/api/v1/decisions/"
    query_params = {
        "created_at__lt": f"{end_date}T00:00:00.000000Z",
        "created_at__gt": f"{start_date}T23:59:59.999999Z",
        "limit": 1000,
        "offset": 0,
    }
    headers = {"accept": "application/json", "authorization": f"Bearer {bearer_token}"}
    response = get_response(url, headers, query_params)
    return response


def add_date_to_json(query_export_contents, date):
    """Add a date to the entries so we can partition table by this date."""
    fields_list = []
    for item in query_export_contents:
        field_dict = {
            "date": date,
            "decision_type": str(item["decision_type"]),
            "user": str(item["user"]),
            "created_at": str(item["created_at"]),
            "entity_id": str(item["entity_id"]),
            "entity": str(item["entity"]),
            "uuid": str(item["uuid"]),
            "entity_slug": str(item["entity_slug"]),
            "job_id": str(item["job_id"]),
            "job_assigned_at": str(item["job_assigned_at"]),
            "queue_slug": str(item["queue_slug"]),
            "typed_metadata": str(item["typed_metadata"]),
            "applied_policies": str(item["applied_policies"]),
        }
        fields_list.append(field_dict)
    return fields_list


def upload_to_bigquery(data, project, dataset, table_name, date):
    """Upload the data to bigquery."""
    date = date
    partition = f"{date}".replace("-", "")
    client = bigquery.Client(project)
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("decision_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entity_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entity", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entity_slug", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("job_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("job_assigned_at", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("queue_slug", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("typed_metadata", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("applied_policies", "STRING", mode="NULLABLE"),
        ],
    )
    destination = f"{project}.{dataset}.{table_name}${partition}"
    job = client.load_table_from_json(data, destination, job_config=job_config)
    job.result()


def main():
    """Input data, call functions, get stuff done."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="addon_moderations_derived")

    args = parser.parse_args()

    project = args.project
    dataset = args.dataset
    table_name = "cinder_decisions_raw_v1"

    date = args.date
    bearer_token = cinder_bearer_token

    cinder_data = []

    query_export = cinder_addon_decisions_download(date, bearer_token)
    """Data returns as a dictionary with a key called 'items' and the value being a list of data"""
    query_export_contents = query_export["items"]
    """Add date to each element in query_export for partitioning"""
    cinder_data = add_date_to_json(query_export_contents, date)
    """Pull out[ the list from query_export["items"] and put that data into the cinder_data list"""
    upload_to_bigquery(cinder_data, project, dataset, table_name, date)


if __name__ == "__main__":
    main()
