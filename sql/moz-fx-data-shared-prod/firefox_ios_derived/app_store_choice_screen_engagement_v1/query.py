"""Fetch Analytics Report for Choice Screen Engagement in the App store via App Store Connect."""

# import argparse
# import csv
import gzip
import io

# import json
import os
import tempfile
import time
from argparse import ArgumentParser
from pathlib import Path

import jwt
import requests
from google.cloud import bigquery

from bigquery_etl.schema import Schema

#
# https://developer.apple.com/documentation/AppStoreConnectAPI/downloading-analytics-reports
#
# export CONNECT_ISSUER_ID=
# export CONNECT_KEY_ID=
# export CONNECT_KEY_FILE_PATH=
CONNECT_ISSUER_ID = os.environ.get("CONNECT_ISSUER_ID")
CONNECT_KEY_ID = os.environ.get("CONNECT_KEY_ID")
CONNECT_KEY_FILE_PATH = os.environ.get("CONNECT_KEY_FILE_PATH")

HOST = "https://api.appstoreconnect.apple.com/"
REPORT_TITLE = "Browser Choice Screen Engagement"
REPORT_DATA_DELIMITER = "\t"

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()


def generate_jwt_token(issuer_id, key_id, private_key_path):
    with open(private_key_path, "r") as key_file:
        private_key = key_file.read()

    payload = {
        "iss": issuer_id,
        "exp": int(time.time()) + 20 * 60,  # Token valid for 20 minutes
        "aud": "appstoreconnect-v1",
    }

    return jwt.encode(
        payload,
        private_key,
        algorithm="ES256",
        headers={"kid": key_id, "alg": "ES256", "typ": "JWT"},
    )


def download_gz_file(url, target_file_path):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        file_content = io.BytesIO()

        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # Filter out keep-alive new chunks
                file_content.write(chunk)

        # # Move the file content pointer to the beginning for reading
        file_content.seek(0)

        # # Decompress from in-memory buffer to file
        with gzip.GzipFile(fileobj=file_content, mode="rb") as f_in:
            with open(target_file_path, "wb") as f_out:
                f_out.write(f_in.read())


def api_call(url, jwt_token):
    """Handle making API requests to App Store Connect."""
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


def get_paginated_data(url, jwt_token):
    data = []
    while True:
        result = api_call(url, jwt_token)
        data.extend(result["data"])
        if next := result["links"].get("next"):
            url = next
        else:
            return data


def fetch_app_reports(jwt_token, app_id):
    """Retrieve the report requests for the given app."""
    endpoint = f"apps/{app_id}/analyticsReportRequests"
    response_data = api_call(f"{HOST}/v1/{endpoint}", jwt_token)
    return response_data.get("data")


def fetch_report_instances(jwt_token, report_id):
    endpoint = f"v1/analyticsReportInstances/{report_id}/segments"
    response_data = api_call(f"{HOST}/{endpoint}", jwt_token)
    return response_data.get("data")


def fetch_report_data(app_id, date, jwt_token, target_file_path):
    """Fetche and prints the report data."""

    # "ONGOING" provides current data and generates reports daily, weekly and monthly.
    # assumed there is only 1 ongoing report request at any given time
    ongoing_analytics_report_request = list(
        filter(
            lambda x: x["attributes"]["accessType"] == "ONGOING",
            fetch_app_reports(jwt_token, app_id),
        )
    )[0]

    analytics_reports = get_paginated_data(
        ongoing_analytics_report_request["relationships"]["reports"]["links"][
            "related"
        ],
        jwt_token,
    )

    # for now we just assume there is only one ongoing browser choice screen report
    browser_choice_screen_analytics_report = list(
        filter(
            lambda x: x["attributes"]["name"].startswith(REPORT_TITLE),
            analytics_reports,
        )
    )[0]

    report_instances = api_call(
        browser_choice_screen_analytics_report["relationships"]["instances"]["links"][
            "related"
        ],
        jwt_token,
    )["data"]

    daily_report_instances = list(
        filter(lambda x: x["attributes"]["granularity"] == "DAILY", report_instances)
    )
    specific_date_report_instance = list(
        filter(
            lambda x: x["attributes"]["processingDate"] == date, daily_report_instances
        )
    )[0]

    analytics_report_segments = api_call(
        specific_date_report_instance["relationships"]["segments"]["links"]["related"],
        jwt_token,
    )["data"][0]

    checksum = analytics_report_segments["attributes"]["checksum"]

    response = requests.get(analytics_report_segments["attributes"]["url"])
    response.raise_for_status()

    # target_file_path = f"/Users/kik/Downloads/{date.replace('-', '_')}_{REPORT_TITLE.lower().replace(' ', '_')}.csv"

    download_gz_file(
        analytics_report_segments["attributes"]["url"],
        target_file_path=target_file_path,
    )

    return target_file_path, checksum


def upload_to_bigquery(local_file_path, project, dataset, table_name, date):
    """Upload the data to bigquery."""

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        schema=SCHEMA,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=REPORT_DATA_DELIMITER,
    )

    client = bigquery.Client(project)
    destination = f"{project}.{dataset}.{table_name}"

    with open(local_file_path, "r+b") as file_obj:
        job = client.load_table_from_file(file_obj, destination, job_config=job_config)

    while client.get_job(job.job_id, location=job.location).state != "DONE":
        print("Waiting for the bq load job to be done.")
        time.sleep(5)

    return job.result(), destination


def main():
    """Input data, call functions, get stuff done."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument(
        "--connect_app_id", required=True, help="App Store Connect App ID"
    )
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="firefox_ios_derived")
    parser.add_argument(
        "--connect_issuer_id",
        default=CONNECT_ISSUER_ID,
        help="App Store Connect Issuer ID",
    )
    parser.add_argument(
        "--connect_key_id", default=CONNECT_KEY_ID, help="App Store Connect Key ID"
    )
    parser.add_argument(
        "--connect_private_key_path",
        default=CONNECT_KEY_FILE_PATH,
        help="Path to the private key (.p8) file",
    )
    args = parser.parse_args()

    jwt_token = generate_jwt_token(
        args.connect_issuer_id, args.connect_key_id, args.connect_private_key_path
    )

    with tempfile.NamedTemporaryFile() as temp_file:
        report_file, checksum = fetch_report_data(
            app_id=args.connect_app_id,
            date=args.date,
            jwt_token=jwt_token,
            target_file_path=temp_file.name,
        )

        # TODO: check the checksum before upload?
        print(report_file)

        table_name = "app_store_choice_screen_engagement_v1"
        job_result, destination_table = upload_to_bigquery(
            temp_file.name, args.project, args.dataset, table_name, args.date
        )

        print(destination_table)


if __name__ == "__main__":
    main()
