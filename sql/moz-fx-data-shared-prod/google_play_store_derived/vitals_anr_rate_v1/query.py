"""Script to pull Vitals ANR rate metrics from Google API."""

import itertools
import json
import logging
import os
import re
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Self, Union

import pandas as pd
import requests
from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.oauth2 import service_account

from bigquery_etl.schema import Schema

# TODO: could consider extracting this into config in the future
APP_NAMES = [
    "org.mozilla.firefox",
    "org.mozilla.firefox_beta",
    "org.mozilla.fenix",
    "org.mozilla.klar",
    "org.mozilla.firefox.vpn",
    "org.mozilla.focus",
]
METRICS = [
    "anrRate",
    "anrRate28dUserWeighted",
    "userPerceivedAnrRate",
    "userPerceivedAnrRate28dUserWeighted",
    "distinctUsers",
]
DIMENSIONS = [
    "versionCode",
]
PAGE_SIZE = 1000
METRIC_SET_NAME = "anrRateMetricSet"
DEFAULT_TABLE_NAME = Path(__file__).parent

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()
PARTITION_FIELD = "submission_date"


def camel_to_snake_case(text: str) -> str:
    """Convert camelCase string to be snake_case."""
    return re.sub(r"(?<=[a-z])(?=[A-Z])", "_", text).lower()


@dataclass
class Dimension:
    """Represent dimension returned the Google API."""

    name: str
    value: str

    @classmethod
    def from_dict(cls, kwargs: Dict[Any, Any]) -> Self:
        """Convert dictionary into Dimension model."""
        return cls(
            name=camel_to_snake_case(kwargs["dimension"]),
            value=kwargs["stringValue"],
        )


@dataclass
class Metric:
    """Represent metric returned the Google API."""

    name: str
    value: float

    @classmethod
    def from_dict(cls, kwargs: Dict[Any, Any]) -> Self:
        """Convert dictionary into Metric model."""
        return cls(
            name=camel_to_snake_case(kwargs["metric"]),
            value=float(kwargs["decimalValue"]["value"]),
        )


@dataclass
class Row:
    """Represent row returned the Google API."""

    dimensions: List[Dimension]
    metrics: List[Metric]

    @classmethod
    def from_dict(cls, dict_obj: Dict[Any, Any]) -> Self:
        """Convert dictionary into Row model."""
        return cls(
            dimensions=[Dimension.from_dict(x) for x in dict_obj["dimensions"]],
            metrics=[Metric.from_dict(x) for x in dict_obj["metrics"]],
        )

    def merge_and_dump(self) -> Dict[str, Union[str, float]]:
        """Merge all Dimensions and Metrics in the Row into a single dictionary and return it."""
        entries = itertools.chain.from_iterable([self.metrics, self.dimensions])
        return {entry.name: entry.value for entry in entries}


def generate_request_payload(date: datetime) -> Dict[str, Any]:
    """Generate request payload to retrieve data from Google API."""
    end_date = date + timedelta(days=1)

    request_payload = {
        "timelineSpec": {
            "aggregationPeriod": "DAILY",
            "startTime": {
                "year": date.year,
                "month": date.month,
                "day": date.day,
            },
            "endTime": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            },
        },
        "metrics": METRICS,
        "dimensions": DIMENSIONS,
        "pageSize": PAGE_SIZE,
    }
    return request_payload


def generate_auth_token(service_account_info: str) -> str:
    """Generate and return auth token."""
    scopes = ["https://www.googleapis.com/auth/playdeveloperreporting"]
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_info), scopes=scopes
    )
    credentials.refresh(Request())
    return credentials.token


def fetch_data(
    access_token: str, app_name: str, request_payload: Dict[str, Any]
) -> Dict[Any, Any]:
    """Make a request to Google API to fetch metrics and return JSON response."""
    url = f"https://playdeveloperreporting.googleapis.com/v1beta1/apps/{app_name}/{METRIC_SET_NAME}:query"
    timeout_seconds = 20

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    response = requests.post(
        url,
        headers=headers,
        json=request_payload,
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    return response.json()


def main(
    date: datetime,
    project: str,
    dataset: str,
    table_name: str,
    service_account_info: str,
) -> None:
    """Retrieve data for each app and upload it to BigQuery."""
    access_credentials = generate_auth_token(service_account_info)
    payload = generate_request_payload(date)
    df = pd.DataFrame()

    for app_name in APP_NAMES:
        logging.info("Processing %s" % app_name)

        result_json = fetch_data(
            access_token=access_credentials,
            app_name=app_name,
            request_payload=payload,
        )

        # TODO: Code only set to handle 1 page, error out if more than 1 to implement pagination.
        if "nextPageToken" in result_json:
            raise NotImplementedError("Result pagination not yet implemented.")

        processed_rows = [
            Row.from_dict(row).merge_and_dump() for row in result_json.get("rows", [])
        ]

        if not processed_rows:
            error_msg = (
                "Processing %s, failed. No records returned by the service." % app_name
            )
            logging.error(error_msg)
            raise Exception(error_msg)

        app_df = pd.DataFrame.from_records(processed_rows)
        app_df[PARTITION_FIELD] = pd.to_datetime(date)
        app_df["app_name"] = app_name

        df = pd.concat([df, app_df], ignore_index=True)

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.table.TimePartitioning(field=PARTITION_FIELD),
    )

    client = bigquery.Client(project)
    table_id = f"{project}.{dataset}.{table_name}${str(date).replace('-', '')}"

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    logging.info(
        "Loaded %s rows and %s columns to %s"
        % (table.num_rows, len(table.schema), table_id)
    )


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--project", type=str, default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", type=str, default="google_play_store_derived")
    parser.add_argument(
        "--table_name", type=str, default=os.path.basename(DEFAULT_TABLE_NAME)
    )
    parser.add_argument(
        "--service_account_info",
        type=str,
        default=os.getenv("GOOGLE_PLAY_STORE_SRVC_ACCT_INFO", None),
    )

    args = parser.parse_args()

    main(
        args.date,
        args.project,
        args.dataset,
        args.table_name,
        args.service_account_info,
    )
