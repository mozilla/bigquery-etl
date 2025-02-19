"""Cinder API Addon Moderations Decisions - download decisions, clean and upload to BigQuery."""

import json
import os

# from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import requests
from google.cloud import bigquery

# from bigquery_etl.schema import SCHEMA_FILE, Schema

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


# def cinder_addon_decisions_download(date, bearer_token):
#     """Download data from Cinder - bearer_token is called here."""
#     submission_date = datetime.strptime(date, "%Y-%m-%d")
#     start_datetime = submission_date + timedelta(days=-1)
#     start_date = start_datetime.strftime("%Y-%m-%d")
#     end_datetime = submission_date + timedelta(days=1)
#     end_date = end_datetime.strftime("%Y-%m-%d")
#     url = "https://stage.cinder.nonprod.webservices.mozgcp.net/api/v1/decisions/"
#     query_params = {
#         "created_at__lt": f"{end_date}T00:00:00.000000Z",
#         "created_at__gt": f"{start_date}T23:59:59.999999Z",
#         "limit": 1000,
#         "offset": 0,
#     }
#     headers = {"accept": "application/json", "authorization": f"Bearer {bearer_token}"}
#     response = get_response(url, headers, query_params)
#     return response


def add_date_to_json(query_export_contents):
    """Add a date to the entries so we can partition table by this date."""
    fields_list = []
    # i = 0
    # related_entities = []
    for item in query_export_contents:
        # print(i)
        x = 0
        timestamp_str = item["attributes"]["created"]
        dt_object = datetime.fromisoformat(timestamp_str)
        date = dt_object.strftime("%Y-%m-%d")
        rels = item.get("__rels", {})
        if rels:
            n = len(rels["items"])
            for x in range(n):
                related_entity_type = rels["items"][x]["entity_type"]
                related_entity_attributes = rels["items"][x]["attributes"]
        else:
            related_entity_type = ""
            related_entity_attributes = ""
        field_dict = {
            "date": date,
            "entity_type": item["entity_type"],
            "attributes": item["attributes"],
            "related_entity_type": related_entity_type,
            "related_entity_attributes": related_entity_attributes,
        }
        fields_list.append(field_dict)
        # i+=1
    return fields_list


def upload_to_bigquery(data, project, dataset, table_name):
    """Upload the data to bigquery."""
    client = bigquery.Client(project)
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("entity_type", "STRING"),
            bigquery.SchemaField(
                "attributes",
                "RECORD",
                fields=[
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("reason", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("locale", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("message", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("created", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "considers_illegal", "BOOLEAN", mode="NULLABLE"
                    ),
                    bigquery.SchemaField("illegal_category", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "illegal_subcategory", "STRING", mode="NULLABLE"
                    ),
                ],
            ),
            bigquery.SchemaField("related_entity_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("related_entity_attributes", "JSON", mode="NULLABLE"),
        ],
    )
    destination = f"{project}.{dataset}.{table_name}"
    job = client.load_table_from_json(data, destination, job_config=job_config)
    job.result()


def main():
    """Input data, call functions, get stuff done."""
    # parser = ArgumentParser(description=__doc__)
    # parser.add_argument("--date", required=True)
    # parser.add_argument("--project", default="moz-fx-data-shared-prod")
    # parser.add_argument("--dataset", default="addon_moderations_derived")

    # args = parser.parse_args()

    project = "moz-fx-data-shared-prod"
    dataset = "analysis"
    table_name = "mh_cinder_reports_raw_v1c"

    # date = args.date
    # bearer_token = cinder_bearer_token

    report_path = Path("reports.json")

    reports_data = []

    report_text_raw = report_path.read_text()
    report_text_clean = report_text_raw.replace(
        "}\n{", "},{"
    )  # take out new lines and replace with commas
    reports_data = json.loads("[" + report_text_clean + "]")  # returns list

    cinder_reports_data = []

    cinder_reports_data = add_date_to_json(reports_data)
    # Pull out[ the list from query_export["items"] and put that data into the cinder_data list"""
    upload_to_bigquery(cinder_reports_data, project, dataset, table_name)


if __name__ == "__main__":
    main()
