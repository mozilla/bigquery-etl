"""Cinder API Addon Moderations Decisions - download decisions, clean and upload to BigQuery."""

import json
import os
import tempfile
from argparse import ArgumentParser

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


def cinder_addon_decisions_download(date, bearer_token):
    """Download data from Cinder - bearer_token is called here."""
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


def upload_to_bigquery(data, project, dataset, table_name, date):
    """Upload the data to bigquery."""
    date = date
    client = bigquery.Client(project)
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )
    destination = f"{project}.{dataset}.{table_name}"
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

    json_file = cinder_addon_decisions_download(date, bearer_token)
    """Data returns as a dictionary with a key called 'items' and the value being a list of data"""
    query_export = check_json(json_file.text)
    """Pull out the list from query_export["items"] and put that data into the cinder_data list"""
    cinder_data = query_export["items"]

    upload_to_bigquery(cinder_data, project, dataset, table_name, date)


if __name__ == "__main__":
    main()
