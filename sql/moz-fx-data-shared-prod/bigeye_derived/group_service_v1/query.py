"""Pull from BigEye API from Group Service and upload to Big Query."""

import logging
import os
import re
from argparse import ArgumentParser
from typing import Any, Dict

import pandas as pd
import requests
from google.cloud import bigquery

BIGEYE_API_KEY = os.environ["BIGEYE_API_KEY"]
API_URL = "https://app.bigeye.com/api/v1/groups"


def make_api_request() -> Dict[str, Any]:
    """Make API request to Bigeye."""
    headers = {
        "accept": "application/json",
        "authorization": f"apikey {BIGEYE_API_KEY}",
    }

    response = requests.get(API_URL, headers=headers)
    response.raise_for_status()
    return response.json()


def process_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """Process API response into a DataFrame."""
    users_df = pd.json_normalize(
        response_data["groups"],
        record_path=["users"],
        meta=["id", "name"],
        meta_prefix="group_",
    ).rename(columns={"id": "user_id", "displayName": "user_displayName"})

    memberships_df = pd.json_normalize(
        response_data["groups"], record_path=["memberships"], meta=["id"], sep="_"
    )

    grants_df = pd.json_normalize(
        response_data["groups"], record_path=["grants"], sep="_"
    )

    memberships_df["user_lastLoginAt"] = memberships_df["user_lastLoginAt"].astype(
        "Int64"
    )
    memberships_df["user_lastLoginAt"] = pd.to_datetime(
        memberships_df["user_lastLoginAt"], unit="s"
    )
    grants_df["id"] = grants_df["id"].astype("Int64")
    grants_df["workspace_id"] = grants_df["workspace_id"].astype("Int64")

    memberships_df = memberships_df.rename(columns={"id": "group_id"})

    merged_df = users_df.merge(
        memberships_df, on=["user_id", "group_id"], how="outer"
    ).merge(grants_df, on=["group_id"], how="outer")

    column_list = merged_df.columns.tolist()
    for column_name in column_list:
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", column_name)
        snake_case_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
        merged_df = merged_df.rename(columns={f"{column_name}": f"{snake_case_name}"})

    merged_df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    return merged_df


def get_bigeye_data() -> pd.DataFrame:
    """Fetch data from Bigeye API and combine into a single DataFrame."""
    response_data = make_api_request()
    df = process_response(response_data)

    return df


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
    parser.add_argument("--table", default="group_service_v1")
    args = parser.parse_args()

    df = get_bigeye_data()

    load_to_bigquery(args.project, args.dataset, args.table, df)


if __name__ == "__main__":
    main()
