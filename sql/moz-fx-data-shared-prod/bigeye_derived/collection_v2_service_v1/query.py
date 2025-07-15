"""Pull from BigEye API from dashboardService and upload to Big Query."""

import logging
import os
from argparse import ArgumentParser
from typing import Any, Dict

import pandas as pd
import requests
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader

TARGET_TABLE = "moz-fx-data-shared-prod.bigeye_derived.collection_v2_service"
BIGEYE_API_KEY = os.environ["BIGEYE_API_KEY"]
WORKSPACE_IDS = ConfigLoader.get("monitoring", "bigeye_workspace_ids")
API_URL = "https://app.bigeye.com/api/v2/collections/info?workspaceId="


def make_api_request(workspace_id: int) -> Dict[str, Any]:
    """Make API request to Bigeye for a specific workspace."""
    headers = {
        "accept": "application/json",
        "authorization": BIGEYE_API_KEY,
    }

    response = requests.get(API_URL + str(workspace_id), headers=headers)
    response.raise_for_status()
    return response.json()


def process_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """Process API response into a DataFrame."""
    df = pd.json_normalize(
        response_data["collectionInfos"],
        record_path=None,
        meta=["collectionMetricStatus"],
        meta_prefix=None,
        record_prefix="",
        errors="raise",
        sep="_",
        max_level=None,
    )

    df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    df.columns = df.columns.str.replace("collectionConfiguration_", "")
    df.columns = df.columns.str.replace("collectionMetricStatus_", "")

    df = df.rename(
        columns={
            "isFavorite": "is_favorite",
            "mutedUntilTimestamp": "muted_until_timestamp",
            "notificationChannels": "notification_channels",
            "metricIds": "metric_id",
            "tags": "tag",
            "entityInfo_createdBy": "created_by",
            "entityInfo_createdEpochSeconds": "created_by_epoch_seconds",
            "entityInfo_updatedBy": "updated_by",
            "entityInfo_updatedEpochSeconds": "updated_by_epoch_seconds",
            "metricsCount": "metrics_count",
            "alertingMetricsCount": "alerting_mnetrics_count",
            "earliestUpdatedMetricSeconds": "earliest_updated_metric_seconds",
            "latestUpdatedMetricSeconds": "latest_updated_metric_seconds",
            "openIssuesCount": "open_issues_count",
            "triageIssuesCount": "triage_issues_count",
            "acknowledgeIssuesCount": "acknowledge_issues_count",
            "monitoringIssuesCount": "monitoring_issues_count",
        }
    )

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


def load_to_bigquery(project_id, df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery."""
    client = bigquery.Client(project_id)

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
    args = parser.parse_args()

    df = get_bigeye_data()

    load_to_bigquery(args.project, df)


if __name__ == "__main__":
    main()
