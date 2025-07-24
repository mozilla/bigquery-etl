"""Pull from BigEye API from Collection V2 Service and upload to Big Query."""

import logging
import os
import re
from argparse import ArgumentParser
from typing import Any, Dict

import pandas as pd
import requests
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader

BIGEYE_API_KEY = os.environ["BIGEYE_API_KEY"]
WORKSPACE_IDS = ConfigLoader.get("monitoring", "bigeye_workspace_ids")
API_URL = "https://app.bigeye.com/api/v2/collections/info?workspaceId="


def make_api_request(workspace_id: int) -> Dict[str, Any]:
    """Make API request to Bigeye for a specific workspace."""
    headers = {
        "accept": "application/json",
        "authorization": f"apikey {BIGEYE_API_KEY}",
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

    column_list = df.columns.tolist()
    for column_name in column_list:
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", column_name)
        snake_case_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
        df = df.rename(columns={f"{column_name}": f"{snake_case_name}"})

    df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    return df


def get_bigeye_data() -> pd.DataFrame:
    """Fetch data from Bigeye API for all workspaces and combine into a single DataFrame."""
    all_data = []

    if not isinstance(WORKSPACE_IDS, list):
        raise Exception("WORKSPACE_IDS is not a list")

    for workspace_id in WORKSPACE_IDS:
        try:
            response_data = make_api_request(workspace_id)
            df = process_response(response_data)
            all_data.append(df)
            logging.info(f"Successfully processed data for workspace {workspace_id}")
        except Exception as e:
            logging.info(f"Error processing workspace {workspace_id}: {str(e)}")

    if not all_data:
        raise Exception("No data was successfully processed from any workspace")

    return pd.concat(all_data, ignore_index=True)


def load_to_bigquery(project_id, dataset, table, df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery."""
    client = bigquery.Client(project_id)

    target_table = f"{project_id}.{dataset}.{table}"

    job = client.load_table_from_dataframe(
        df,
        target_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    logging.info(f"Successfully loaded data to {target_table}")


def main() -> None:
    """Pull from BigEye API dashboard service endpoint then upload to BigQuery."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="bigeye_derived")
    parser.add_argument("--table", default="collection_service_v1")
    args = parser.parse_args()

    df = get_bigeye_data()

    load_to_bigquery(args.project, args.dataset, args.table, df)


if __name__ == "__main__":
    main()
