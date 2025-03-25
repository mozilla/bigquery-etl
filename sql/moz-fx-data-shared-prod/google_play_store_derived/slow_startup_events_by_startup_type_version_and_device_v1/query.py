# Load libraries
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import pandas as pd
import os
import json

# Set variables
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.google_play_store_derived.slow_startup_events_by_startup_type_version_and_device_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "GOOGLE_PLAY_STORE/device_slow_startup_events_by_startup_type_%s.csv"
TIMEOUT_IN_SECONDS = 10
SCOPES = ["https://www.googleapis.com/auth/playdeveloperreporting"]
APP_NAMES = [
    "org.mozilla.firefox",
    "org.mozilla.firefox_beta",
    "org.mozilla.fenix",
    "org.mozilla.klar",
    "org.mozilla.firefox.vpn",
    "org.mozilla.focus",
]


def create_request_payload_using_logical_dag_date(date_to_pull_data_for):
    """Input: datetime.date, Output: JSON for request payload that pulls data for that same day"""
    # Get the date to pull data for, as year, month day
    date_to_pull_data_for_yr = date_to_pull_data_for.year
    date_to_pull_data_for_month = date_to_pull_data_for.month
    date_to_pull_data_for_day = date_to_pull_data_for.day

    # Add 1 day to date to pull data for
    end_date = date_to_pull_data_for + timedelta(days=1)

    day_after_date_to_pull_data_for_yr = end_date.year
    day_after_date_to_pull_data_for_month = end_date.month
    day_after_date_to_pull_data_for_day = end_date.day

    # Get the next day as year month day
    request_payload = {
        "timelineSpec": {
            "aggregationPeriod": "DAILY",
            "startTime": {
                "year": date_to_pull_data_for_yr,
                "month": date_to_pull_data_for_month,
                "day": date_to_pull_data_for_day,
            },
            "endTime": {
                "year": day_after_date_to_pull_data_for_yr,
                "month": day_after_date_to_pull_data_for_month,
                "day": day_after_date_to_pull_data_for_day,
            },
        },
        "metrics": ["slowStartRate", "slowStartRate7dUserWeighted", "distinctUsers"],
        "dimensions": [
            "startType",
            "apiLevel",
            "deviceType",
            "deviceRamBucket",
            "versionCode",
        ],
        "pageSize": 5000,
    }
    return request_payload


def get_slow_start_rates_by_app_and_date(
    access_token, app_name, request_payload, timeout_seconds
):
    """Call the API URL using the given credentials and the given timeout limit
    Inputs:
    * Access Token
    * Timeout Seconds (int)
    Outputs:
    * API call response"""
    api_url = f"https://playdeveloperreporting.googleapis.com/v1beta1/apps/{app_name}/slowStartRateMetricSet:query"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    response = requests.post(
        api_url, headers=headers, json=request_payload, timeout=timeout_seconds
    )
    return response


def main():
    """Call the API, save data to GCS, write data to BQ"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # Get the DAG logical run date
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    print("logical_dag_date")
    print(logical_dag_date)

    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")
    print("logical_dag_date_string: ", logical_dag_date_string)

    # Get 2 days prior - we always will pull data for the previous day
    data_pull_date = logical_dag_date - timedelta(days=2)
    data_pull_date_string = data_pull_date.strftime("%Y-%m-%d")
    print("data_pull_date")
    print(data_pull_date)
    print("data_pull_date_string")
    print(data_pull_date_string)

    service_account_info = json.loads(os.getenv("GOOGLE_PLAY_STORE_SRVC_ACCT_INFO"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES
    )
    credentials.refresh(Request())
    access_credentials = credentials.token

    # Calculate the request payload
    payload_for_api_call = create_request_payload_using_logical_dag_date(data_pull_date)

    # Initialize a dataframe to store the data
    final_df = pd.DataFrame(
        {
            "submission_date": [],
            "google_play_store_app_name": [],
            "startup_type": [],
            "api_level_label": [],
            "device_type": [],
            "device_ram_bucket": [],
            "version_code": [],
            "slow_start_rate": [],
            "slow_start_rate_7_day_user_weighted_avg": [],
            "nbr_distinct_users": [],
        }
    )

    # Loop through each app type and get the data
    for app in APP_NAMES:
        print("Pulling data for: ", app)

        api_call_result = get_slow_start_rates_by_app_and_date(
            access_token=access_credentials,
            app_name=app,
            request_payload=payload_for_api_call,
            timeout_seconds=TIMEOUT_IN_SECONDS,
        )

        # Get the data from the result
        result_json = api_call_result.json()

        # Code only set to handle 1 page, error out if more than 1 so it can be fixed
        if "nextPageToken" in result_json:
            raise NotImplementedError("Parsing for next page is not implemented yet.")

        # Loop through each row
        for row in result_json["rows"]:

            # Initialize each dimension and metric as empty until found in this row
            startup_type = None
            api_level_value_label = None
            device_type = None
            device_ram_bucket = None
            version_code = None
            slow_start_rate = None
            slowStartRate7dUserWeighted = None
            distinct_users = None

            # Parse dimension info for this row
            dimensions = row["dimensions"]
            for dim in dimensions:
                if dim["dimension"] == "startType":
                    startup_type = dim["stringValue"]
                if dim["dimension"] == "apiLevel":
                    api_level_value_label = dim["valueLabel"]
                if dim["dimension"] == "deviceType":
                    device_type = dim["valueLabel"]
                if dim["dimension"] == "deviceRamBucket":
                    device_ram_bucket = dim["valueLabel"]
                if dim["dimension"] == "versionCode":
                    version_code = dim["stringValue"]

            # Parse metric info for this row
            metrics = row["metrics"]
            for metric in metrics:
                if metric["metric"] == "slowStartRate":
                    slow_start_rate = metric["decimalValue"]["value"]
                if metric["metric"] == "distinctUsers":
                    distinct_users = metric["decimalValue"]["value"]
                if metric["metric"] == "slowStartRate7dUserWeighted":
                    slowStartRate7dUserWeighted = metric["decimalValue"]["value"]

            # Put the information parsed from this row for this dimension into a dataframe
            new_df = pd.DataFrame(
                {
                    "submission_date": [data_pull_date_string],
                    "google_play_store_app_name": [app],
                    "startup_type": [startup_type],
                    "api_level_label": [api_level_value_label],
                    "device_type": [device_type],
                    "device_ram_bucket": [device_ram_bucket],
                    "version_code": [version_code],
                    "slow_start_rate": [slow_start_rate],
                    "slow_start_rate_7_day_user_weighted_avg": [
                        slowStartRate7dUserWeighted
                    ],
                    "nbr_distinct_users": [distinct_users],
                }
            )

            # Append the data into the results dataframe
            final_df = pd.concat([final_df, new_df])

    # Let's load the data to CSV in GCS
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (logical_dag_date_string)
    print("final_results_fpath: ", final_results_fpath)
    final_df.to_csv(final_results_fpath, index=False)

    # Open a BQ client
    client = bigquery.Client(TARGET_PROJECT)

    # If there is anything already in the table for this day, delete it
    delete_query = f"""DELETE FROM `moz-fx-data-shared-prod.google_play_store_derived.slow_startup_events_by_startup_type_version_and_device_v1`
  WHERE submission_date = '{data_pull_date_string}'"""
    del_job = client.query(delete_query)
    del_job.result()

    # Append the latest data for this day into the BQ table
    load_csv_to_gcp_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_APPEND",
            schema=[
                {"name": "submission_date", "type": "DATE", "mode": "NULLABLE"},
                {
                    "name": "google_play_store_app_name",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "startup_type",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "api_level_label",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "device_type",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "device_ram_bucket",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "version_code",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {
                    "name": "slow_start_rate",
                    "type": "NUMERIC",
                    "mode": "NULLABLE",
                },
                {
                    "name": "slow_start_rate_7_day_user_weighted_avg",
                    "type": "NUMERIC",
                    "mode": "NULLABLE",
                },
                {
                    "name": "nbr_distinct_users",
                    "type": "NUMERIC",
                    "mode": "NULLABLE",
                },
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gcp_job.result()


if __name__ == "__main__":
    main()
