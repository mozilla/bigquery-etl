"""Pull from BigEye API from dashboardService and upload to Big Query."""

import logging
import os
from argparse import ArgumentParser
from typing import Any, Dict

import pandas as pd
import requests
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader

BIGEYE_API_KEY = os.getenv("BIGEYE_API_KEY")
WORKSPACE_IDS = ConfigLoader.get("monitoring", "bigeye_workspace_ids")
API_URL = "https://app.bigeye.com/api/v1/dashboard/calculate-data-points-request"

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

    response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def process_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """Process API response into a DataFrame."""
    df = pd.json_normalize(
        response_data["dataPointSeries"],
        record_path="dataPoints",
        meta=["dataPointType"],
        errors="ignore",
    )
    df["workspaceIds"] = df["workspaceIds"].str[0]

    df.drop(columns=["sourceIds"], errors="ignore", inplace=True)
    df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    df = df.rename(
        columns={
            "dataPointType": "data_point_type",
            "workspaceIds": "workspace_id",
            "dataTimestamp": "timestamp",
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

    return df


def get_bigeye_data() -> pd.DataFrame:
    """Fetch data from Bigeye API for all workspaces and combine into a single DataFrame."""
    all_data = []

    for workspace_id in WORKSPACE_IDS:
        try:
            response_data = make_api_request(workspace_id)
            df = process_response(response_data)
            all_data.append(df)
            logging.info(f"Successfully processed data for workspace {workspace_id}")
        except Exception as e:
            logging.info(f"Error processing workspace {workspace_id}: {str(e)}")
            continue

    if not all_data:
        raise Exception("No data was successfully processed from any workspace")

    return pd.concat(all_data, ignore_index=True)


def validate_dataframe(df: pd.DataFrame) -> None:
    """Perform basic validation on the DataFrame."""
    required_columns = {
        "data_point_type",
        "timestamp",
        "workspace_id",
        "value",
        "refreshed_at",
    }
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")


def load_to_bigquery(project_id, dataset, table, df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery."""
    client = bigquery.Client(project_id)

    TARGET_TABLE = f"{project_id}.{dataset}.{table}"

    job = client.load_table_from_dataframe(
        df,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    logging.info(f"Successfully loaded data to {TARGET_TABLE}")


def main() -> None:
    """Pull from BigEye API dashboard service endpoint then upload to BigQuery."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="bigeye_derived")
    parser.add_argument("--table", default="dashboards")
    args = parser.parse_args()

    df = get_bigeye_data()

    validate_dataframe(df)

    load_to_bigquery(args.project, args.dataset, args.table, df)


if __name__ == "__main__":
    main()
