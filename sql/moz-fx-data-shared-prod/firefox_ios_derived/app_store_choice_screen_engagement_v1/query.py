"""Fetch Analytics Report for Choice Screen Engagement in the App store via App Store Connect."""

import gzip
import io
import logging
import os
import tempfile
import time
from argparse import ArgumentParser
from pathlib import Path

import jwt
import pandas as pd
import requests
from google.cloud import bigquery

from bigquery_etl.schema import Schema

#
# https://developer.apple.com/documentation/AppStoreConnectAPI/downloading-analytics-reports
CONNECT_ISSUER_ID = os.environ.get("CONNECT_ISSUER_ID")
CONNECT_KEY_ID = os.environ.get("CONNECT_KEY_ID")
CONNECT_KEY = os.environ.get("CONNECT_KEY")

HOST = "https://api.appstoreconnect.apple.com"
REPORT_TITLE = "Browser Choice Screen Engagement (iOS versions before 18.2)"
REPORT_DATA_DELIMITER = "\t"

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()


def generate_jwt_token(issuer_id, key_id, private_key):
    """Generate jtw token to be used for API authentication."""
    payload = {
        "iss": issuer_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + 20 * 60,  # Token valid for 20 minutes
        "aud": "appstoreconnect-v1",
    }
    algorithm = "ES256"

    return jwt.encode(
        payload,
        private_key.replace("\\n", "\n"),
        algorithm=algorithm,
        headers={"kid": key_id, "alg": algorithm, "typ": "JWT"},
    )


def download_gz_file(url, target_file_path):
    """Download and decompress a gz file."""
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
    """Get data from all result pages."""
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
    """Retrieve a list of report instances from the App Store."""
    endpoint = f"v1/analyticsReportInstances/{report_id}/segments"
    response_data = api_call(f"{HOST}/{endpoint}", jwt_token)
    return response_data.get("data")


def fetch_report_data(app_id, date, jwt_token, target_file_path):
    """Fetch and prints the report data."""
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
            lambda x: x["attributes"]["name"] == REPORT_TITLE,
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

    download_gz_file(
        analytics_report_segments["attributes"]["url"],
        target_file_path=target_file_path,
    )

    return target_file_path, checksum


def upload_to_bigquery(
    local_file_path, project, dataset, table_name, date, partition_field
):
    """Upload the data to bigquery."""
    df = pd.read_csv(local_file_path, delimiter=REPORT_DATA_DELIMITER)
    df[partition_field] = date
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df[partition_field] = pd.to_datetime(df["logical_date"], format="%Y-%m-%d")

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=SCHEMA,
    )

    client = bigquery.Client(project)
    destination = f"{project}.{dataset}.{table_name}${date.replace('-', '')}"

    job = client.load_table_from_dataframe(df, destination, job_config=job_config)

    while client.get_job(job.job_id, location=job.location).state != "DONE":
        logging.info("Waiting for the bq load job to be done, job_id: %s." % job.job_id)
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
    parser.add_argument("--table", default="app_store_choice_screen_engagement_v1")
    parser.add_argument("--partition_field", default="logical_date")
    parser.add_argument(
        "--connect_issuer_id",
        default=CONNECT_ISSUER_ID,
        help="App Store Connect Issuer ID",
    )
    parser.add_argument(
        "--connect_key_id", default=CONNECT_KEY_ID, help="App Store Connect Key ID"
    )
    parser.add_argument(
        "--connect_private_key",
        default=CONNECT_KEY,
        help="Private key",
    )
    args = parser.parse_args()

    jwt_token = generate_jwt_token(
        args.connect_issuer_id, args.connect_key_id, args.connect_private_key
    )

    with tempfile.NamedTemporaryFile() as temp_file:
        report_file, _ = fetch_report_data(
            app_id=args.connect_app_id,
            date=args.date,
            jwt_token=jwt_token,
            target_file_path=temp_file.name,
        )

        logging.info("Report file downloaded: %s" % report_file)

        _, destination_table = upload_to_bigquery(
            temp_file.name,
            args.project,
            args.dataset,
            args.table,
            args.date,
            args.partition_field,
        )

        logging.info("BigQuery table has been created: %s" % destination_table)


if __name__ == "__main__":
    main()
