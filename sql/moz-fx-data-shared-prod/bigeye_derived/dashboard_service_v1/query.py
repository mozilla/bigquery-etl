"""Pull from BigEye API from dashboardService and upload to Big Query."""

import json
import os
import time
from typing import Any, Dict

# Libraries specific for pulling from API and uploading to bigquery.
import pandas as pd
import requests
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from requests.exceptions import RequestException

# Configuration
TARGET_PROJECT = os.getenv("TARGET_PROJECT", "moz-fx-data-shared-prod")
TARGET_TABLE = os.getenv(
    "TARGET_TABLE", "moz-fx-data-shared-prod.bigeye_derived.dashboard_service"
)
BIGEYE_API_KEY = os.environ["BIGEYE_API_KEY"]
WORKSPACE_IDS = json.loads(os.getenv("WORKSPACE_IDS", "[463, 508, 509]"))
API_URL = "https://app.bigeye.com/api/v1/dashboard/calculate-data-points-request"

# Data point types to request
DATA_POINT_TYPES = [
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_OPEN_ISSUES",
    "DASHBOARD_DATA_POINT_TYPE_AVERAGE_ISSUE_TIME_TO_CLOSE",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_OPEN_ISSUES_BY_COLLECTION",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_METRICS_BY_COLLECTION",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_ISSUES_CLOSED_BY_USER",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_BILLABLE_TABLES",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_ISSUES_CLOSED",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_METRICS_WITH_ISSUE_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_METRICS_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_PCT_METRICS_WO_ISSUE_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_TOTAL_TABLES",
    "DASHBOARD_DATA_POINT_TYPE_TOTAL_TABLES_WITH_METRICS",
    "DASHBOARD_DATA_POINT_TYPE_PCT_TABLES_WITH_METRICS",
    "DASHBOARD_DATA_POINT_TYPE_TOTAL_TABLES_WITH_METRICS_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_PCT_TABLES_WITH_METRICS_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_TOTAL_TABLES_WITH_ISSUE_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_NUMBER_ISSUES_OPENED",
    "DASHBOARD_DATA_POINT_TYPE_PCT_ISSUES_INTERACTED",
    "DASHBOARD_DATA_POINT_TYPE_PCT_TABLES_WITHOUT_ISSUE_BY_CATEGORY",
    "DASHBOARD_DATA_POINT_TYPE_TOTAL_TABLES_WITHOUT_ISSUE_BY_CATEGORY",
]


def make_api_request(workspace_id: int) -> Dict[str, Any]:
    """Make API request to Bigeye for a specific workspace."""
    payload = {
        "aggregationType": "DASHBOARD_DATE_AGGREGATION_TYPE_DAY",
        "dataPointTypes": DATA_POINT_TYPES,
        "workspaceIds": [workspace_id],
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": BIGEYE_API_KEY,
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        raise Exception(f"API request failed for workspace {workspace_id}: {str(e)}")


def process_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """Process API response into a DataFrame."""
    try:
        df = pd.json_normalize(
            response_data["dataPointSeries"],
            record_path="dataPoints",
            meta=["dataPointType"],
            errors="ignore",
        )
        df["workspaceIds"] = df["workspaceIds"].str[0]  # Take first element

        # Drop sourceIds if it exists
        df.drop(columns=["sourceIds"], errors="ignore", inplace=True)

        # Rename columns to database-friendly names
        df = df.rename(
            columns={
                "dataPointType": "data_point_type",
                "workspaceIds": "workspace_id",
                "dataTimestamp": "timestamp",
            }
        )

        # Ensure your DataFrame column is datetime type
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        return df
    except KeyError as e:
        raise Exception(f"Missing expected key in response data: {str(e)}")


def get_bigeye_data() -> pd.DataFrame:
    """Fetch data from Bigeye API for all workspaces and combine into a single DataFrame."""
    all_data = []

    for workspace_id in WORKSPACE_IDS:
        try:
            response_data = make_api_request(workspace_id)
            df = process_response(response_data)
            all_data.append(df)
            print(f"Successfully processed data for workspace {workspace_id}")
        except Exception as e:
            print(f"Error processing workspace {workspace_id}: {str(e)}")
            continue

    if not all_data:
        raise Exception("No data was successfully processed from any workspace")

    return pd.concat(all_data, ignore_index=True)


def validate_dataframe(df: pd.DataFrame) -> None:
    """Perform basic validation on the DataFrame."""
    required_columns = {"data_point_type", "timestamp", "workspace_id", "value"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")


def load_to_bigquery(df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery."""
    client = bigquery.Client(TARGET_PROJECT)

    try:
        job = client.load_table_from_dataframe(
            df,
            TARGET_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job.result()
        print(f"Successfully loaded data to {TARGET_TABLE}")
    except GoogleAPIError as e:
        raise Exception(f"BigQuery load failed: {str(e)}")


def main() -> None:
    """Pull from BigEye API dashboard service endpoint then upload to BigQuery."""
    start_time = time.time()
    print("Starting data pipeline...")

    try:
        # Get and process data
        df = get_bigeye_data()

        # Validate data
        validate_dataframe(df)

        # Load to BigQuery
        load_to_bigquery(df)

        elapsed = time.time() - start_time
        print(f"Pipeline completed successfully in {elapsed:.2f} seconds")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
