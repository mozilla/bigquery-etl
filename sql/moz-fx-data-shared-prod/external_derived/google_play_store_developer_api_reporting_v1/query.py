import os
import json
import google.auth
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import pandas as pd

# Set variables
METRIC_METADATA_API_URL = "https://playdeveloperreporting.googleapis.com/v1beta1/apps/org.mozilla.firefox/crashRateMetricSet"
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.google_play_store_developer_api_reporting_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "GOOGLE_PLAY_STORE/google_play_store_developer_api_reporting_%s.csv"
TIMEOUT_IN_SECONDS = 10
SERVICE_ACCOUNT_FILE = "/Users/kwindau/Documents/2025/202502/boxwood-axon-825-a8f9a0239d65.json"  #######TEMP - will replace later on when moved to airflow###########
SCOPES = ["https://www.googleapis.com/auth/playdeveloperreporting"]


def call_api(access_token, url, timeout_seconds):
    """Call the API URL using the given credentials and the given timeout limit
    Inputs:
    * Access Token
    * URL (string)
    * Timeout Seconds (int)
    Outputs:
    * API call response"""
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    response = requests.get(url, headers=headers, timeout=timeout_seconds)
    return response


def parse_metric_set_metadata(metric_set_metadata_json):
    """Takes a JSON, returns 2 pandas dataframes = 1 for hourly info, 1 for daily info"""
    # Initialize a dataframe
    metric_set_hourly_metadata_df = pd.DataFrame(
        {
            "latest_end_time_year": [],
            "latest_end_time_month": [],
            "latest_end_time_day": [],
            "latest_end_time_hours": [],
            "latest_end_time_timezone": [],
        }
    )

    metric_set_daily_metadata_df = pd.DataFrame(
        {
            "latest_end_time_year": [],
            "latest_end_time_month": [],
            "latest_end_time_day": [],
            "latest_end_time_timezone": [],
        }
    )

    for freshness in metric_set_metadata_json["freshnessInfo"]["freshnesses"]:
        agg_period = freshness["aggregationPeriod"]
        print("agg_period")
        print(agg_period)
        if agg_period == "HOURLY":
            latest_end_time_year = freshness["latestEndTime"]["year"]
            latest_end_time_month = freshness["latestEndTime"]["month"]
            latest_end_time_day = freshness["latestEndTime"]["day"]
            latest_end_time_hours = freshness["latestEndTime"]["hours"]
            latest_end_time_tz = freshness["latestEndTime"]["timeZone"]["id"]
            new_hourly_df = pd.DataFrame(
                {
                    "latest_end_time_year": [latest_end_time_year],
                    "latest_end_time_month": [latest_end_time_month],
                    "latest_end_time_day": [latest_end_time_day],
                    "latest_end_time_hours": [latest_end_time_hours],
                    "latest_end_time_timezone": [latest_end_time_tz],
                }
            )
            metric_set_hourly_metadata_df = pd.concat(
                [metric_set_hourly_metadata_df, new_hourly_df]
            )

        if agg_period == "DAILY":
            latest_end_time_year = freshness["latestEndTime"]["year"]
            latest_end_time_month = freshness["latestEndTime"]["month"]
            latest_end_time_day = freshness["latestEndTime"]["day"]
            latest_end_time_tz = freshness["latestEndTime"]["timeZone"]["id"]
            new_daily_df = pd.DataFrame(
                {
                    "latest_end_time_year": [latest_end_time_year],
                    "latest_end_time_month": [latest_end_time_month],
                    "latest_end_time_day": [latest_end_time_day],
                    "latest_end_time_timezone": [latest_end_time_tz],
                }
            )
            metric_set_daily_metadata_df = pd.concat(
                [metric_set_daily_metadata_df, new_daily_df]
            )

    return metric_set_hourly_metadata_df, metric_set_daily_metadata_df


def main():
    """Call the API, save data to GCS, write data to BQ"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")

    print("logical_dag_date_string: ", logical_dag_date_string)

    # Get credentials #######TEMP - will replace later on when moved to airflow###########
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    credentials.refresh(Request())
    access_token = credentials.token

    # Get the metric metadata
    metricset_metadata = call_api(
        access_token, METRIC_METADATA_API_URL, TIMEOUT_IN_SECONDS
    )
    print("metricset_metadata")
    print(metricset_metadata.json())

    metadata_hourly_df, metadata_daily_df = parse_metric_set_metadata(
        metricset_metadata.json()
    )
    print("metadata_hourly_df")
    print(metadata_hourly_df)

    print("metadata_daily_df")
    print(metadata_daily_df)

    # Now, let's query the metric sets


if __name__ == "__main__":
    main()
