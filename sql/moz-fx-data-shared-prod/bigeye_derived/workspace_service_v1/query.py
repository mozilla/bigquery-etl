"""Pull from BigEye API from Workspace Service and upload to Big Query."""

import logging
import os
import re
from argparse import ArgumentParser
from typing import Any, Dict

import pandas as pd
import requests
from google.cloud import bigquery

BIGEYE_API_KEY = os.environ["BIGEYE_API_KEY"]
API_URL = "https://app.bigeye.com/api/v1/workspaces"


def make_api_request() -> Dict[str, Any]:
    """Make API request to Bigeye."""
    headers = {
        "accept": "application/json",
        "authorization": BIGEYE_API_KEY,
    }

    response = requests.get(API_URL, headers=headers)
    response.raise_for_status()
    return response.json()


def process_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """Process API response into a DataFrame."""
    df = pd.json_normalize(
        response_data["workspaces"],
        record_path=["sources"],
        meta=["id", "name", "isDefault"],
        sep="_",
        meta_prefix="workspace_",
    )

    column_list = df.columns.tolist()
    for column_name in column_list:
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", column_name)
        snake_case_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
        df = df.rename(columns={f"{column_name}": f"{snake_case_name}"})

    df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    return df


def get_bigeye_data() -> pd.DataFrame:
    """Fetch data from Bigeye API and combine into a single DataFrame."""
    response_data = make_api_request()
    df = process_response(response_data)

    return df


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
    parser.add_argument("--table", default="workspaces")
    args = parser.parse_args()

    df = get_bigeye_data()

    load_to_bigquery(args.project, args.dataset, args.table, df)


if __name__ == "__main__":
    main()
