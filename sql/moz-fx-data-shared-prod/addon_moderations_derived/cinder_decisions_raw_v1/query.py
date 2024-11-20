"""Cinder API Addon Moderations Decisions - download decisions, clean and upload to BigQuery."""

import csv
import json
import os
import tempfile
from argparse import ArgumentParser
from time import sleep

import requests
from google.cloud import bigquery

CSV_FIELDS = [
    "user",
    "queue_slug",
    "job_id",
    "uuid",
    "applied_policies",
    "entity",
    "entity_slug",
    "entity_id",
    "created_at",
    "decision_type",
    "job_assigned_at",
    "typed_metadata",
]

cinder_bearer_token = os.environ.get("CINDER_TOKEN")


def post_response(url, headers, data):
    """POST response function."""
    response = requests.post(url, headers=headers, data=data)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response


def get_response(url, headers):
    """GET response function."""
    response = requests.get(url, headers=headers)
    if (response.status_code == 401) or (response.status_code == 400):
        print(f"***Error: {response.status_code}***")
        print(response.text)
    return response


def read_json(filename: str) -> dict:
    """Read JSON file."""
    with open(filename, "r") as f:
        data = json.loads(f.read())
    return data


def write_dict_to_csv(json_data, filename):
    """Write a dictionary to a csv."""
    with open(filename, "w") as out_file:
        dict_writer = csv.DictWriter(out_file, CSV_FIELDS)
        dict_writer.writeheader()
        dict_writer.writerows(json_data)


def cinder_addon_decisions_download(date, bearer_token):
    """Download data from Cinder - bearer_token are called here."""
    # getting overview metrics for different kpis / Deliverables
    url = "https://stage.cinder.nonprod.webservices.mozgcp.net/api/v1/decisions/"
    headers = {"accept": "application/json", "authorization": f"Bearer {bearer_token}"}
    print(url)
    response = get_response(url, headers)
    return response


def check_json(cinder_addon_decisions_response_text):
    """Script will return an empty dictionary for apps on days when there is no data. Check for that here."""
    with tempfile.NamedTemporaryFile() as tmp_json:
        with open(tmp_json.name, "w") as f_json:
            f_json.write(cinder_addon_decisions_response_text)
            try:
                query_export = read_json(f_json.name)
            except (
                ValueError
            ):  # ex. json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
                return None
    return query_export


def clean_json(query_export, date):
    """Turn the json file into a list to be input into a CSV for bq upload."""
    fields_list = []
    for item in query_export["items"]:
        field_dict = {
            "user": item["user"],
            "queue_slug": item["queue_slug"],
            "job_id": item["job_id"],
            "uuid": item["uuid"],
            "applied_policies": item["applied_policies"],
            "entity": item["entity"],
            "entity_slug": item["entity_slug"],
            "entity_id": item["entity_id"],
            "created_at": item["created_at"],
            "decision_type": item["decision_type"],
            "job_assigned_at": item["job_assigned_at"],
            "typed_metadata": item["typed_metadata"],
        }
        fields_list.append(field_dict)
    return fields_list


def upload_to_bigquery(csv_data, project, dataset, table_name, date):
    """Upload the data to bigquery."""
    date = date
    print("writing json to csv")
    partition = f"{date}".replace("-", "")
    print(partition)
    with tempfile.NamedTemporaryFile() as tmp_csv:
        with open(tmp_csv.name, "w+b") as f_csv:
            write_dict_to_csv(csv_data, f_csv.name)
            client = bigquery.Client(project)
            job_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date",
                ),
                skip_leading_rows=1,
                schema=[
                    bigquery.SchemaField("date", "DATE"),
                    bigquery.SchemaField("user", "STRING"),
                    bigquery.SchemaField("queue_slug", "STRING"),
                    bigquery.SchemaField("job_id", "STRING"),
                    bigquery.SchemaField("uuid", "STRING"),
                    bigquery.SchemaField("applied_policies", "STRING"),
                    bigquery.SchemaField("entity", "STRING"),
                    bigquery.SchemaField("entity_slug", "STRING"),
                    bigquery.SchemaField("entity_id", "STRING"),
                    bigquery.SchemaField("created_at", "STRING"),
                    bigquery.SchemaField("decision_type", "STRING"),
                    bigquery.SchemaField("job_assigned_at", "STRING"),
                    bigquery.SchemaField("typed_metadata", "STRING"),
                ],
            )
            destination = f"{project}.{dataset}.{table_name}${partition}"
            job = client.load_table_from_file(f_csv, destination, job_config=job_config)
            print(
                f"Writing Decisions data to {destination}. BigQuery job ID: {job.job_id}"
            )
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

    data = []

    json_file = cinder_addon_decisions_download(date, bearer_token)
    query_export = check_json(json_file.text)

    if query_export is not None:
        # This section writes the tmp json data into a temp CSV file which will then be put into a BigQuery table
        cinder_addon_decisions_data = clean_json(query_export, date)
        data.extend(cinder_addon_decisions_data)
    else:
        print("no data for today")
    sleep(5)

    upload_to_bigquery(data, project, dataset, table_name, date)


if __name__ == "__main__":
    main()
