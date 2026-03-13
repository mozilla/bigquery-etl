"""Script for pulling fb conversion events and pushing them to Meta Conversions API."""

# https://developers.facebook.com/docs/marketing-api/conversions-api/get-started

import os
from argparse import ArgumentParser
from datetime import date
from itertools import batched
from pathlib import Path
from typing import List

from facebook_business.adobjects.serverside.action_source import ActionSource
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi
from google.cloud import bigquery

# from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema import Schema

API_URI = "https://graph.facebook.com/"
API_VERSION = "v25.0"
API_ENDPOINT = "events"
FBC_FORMAT_TEMPLATE = "fbc.{subdomain_index}.{creation_time}.{fbclid}"
SUBDOMAIN_INDEX = 1
PARTITION_FIELD = "activity_date"

DEFAULT_TABLE_NAME = Path(__file__).parent

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()


# Both contain different conversion events so union should be fine.
# TODO: should we be checking if a specific fbclid and conversion_name combination already been sent to Facebook before?
UNION_QUERY = """
SELECT activity_date, fbclid, ga_event_timestamp, ga_country, conversion_name,
FROM `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_2_v1`
WHERE activity_date = @activity_date AND ga_country IN ("United States", "India")
UNION ALL
SELECT activity_date, fbclid, ga_event_timestamp, ga_country, conversion_name,
FROM `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_7_v1`
WHERE activity_date = @activity_date AND ga_country IN ("United States", "India")
"""


# TODO: still needs to be completed.
def run_query(bq_client, activity_date, query):
    """TODO."""
    query_job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date),
        ),
    )
    query_job.result()

    print("The query data:")
    for row in query_job:
        print(row)


# TODO: this needs a bit more work.
def load_to_bigquery(bq_client, project_id, dataset, table_name, activity_date, data):
    """Load DataFrame to BigQuery."""
    target_table = (
        f"{project_id}.{dataset}.{table_name}${activity_date.replace('-', '')}"
    )

    job = bq_client.load_table_from_dataframe(
        data,
        target_table,
        job_config=bigquery.LoadJobConfig(
            time_partitioning=bigquery.TimePartitioning(field=PARTITION_FIELD),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=SCHEMA,
        ),
    )
    job.result()
    print(f"Successfully loaded data to {target_table}")


# Looks like there's a library for building event:
# https://developers.facebook.com/documentation/ads-commerce/conversions-api/guides/business-sdk-features#http-service-interface
def create_event(
    event_name: str, event_timestamp: int, fbclid: str, fbclid_creation_timestamp: int
) -> Event:
    """TODO."""
    return Event(
        event_name=event_name,
        event_time=event_timestamp,
        user_data=UserData(
            fbc=FBC_FORMAT_TEMPLATE.format(
                subdomain_index=SUBDOMAIN_INDEX,
                creation_time=fbclid_creation_timestamp,
                fbclid=fbclid,
            )
        ),
        action_source=ActionSource.OTHER,
    )


def execute_request(pixel_id: int, events: List[Event]):
    """TODO."""
    event_request = EventRequest(
        pixel_id=pixel_id,
        events=events,
    )
    return event_request.execute()


def main(
    activity_date: date,
    project_id: str,
    dataset: str,
    table_name: str,
    access_token: str,
    pixel_id: int,
) -> None:
    """TODO."""
    if not (pixel_id and access_token):
        raise Exception(
            "Missing required test config. Got pixel_id: '{pixel_id}', access_token: '{access_token}'".format(
                pixel_id=pixel_id, access_token=access_token
            )
        )

    bq_client = bigquery.Client(project_id)

    query_results = run_query(bq_client, activity_date, UNION_QUERY)
    conversion_events = [
        create_event(conversion_event) for conversion_event in query_results
    ]

    FacebookAdsApi.init(access_token=access_token, crash_log=False)

    # TODO: check this batching method works as intended.
    for batch in batched(conversion_events, 1000):
        response = execute_request([batch])
        # TODO: Investigate if can determine which fclid conversions got successfully uploaded and write to table?
        print(response)

    load_to_bigquery(
        bq_client, project_id, dataset, table_name, activity_date, query_results
    )


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--activity_date", type=date.fromisoformat, required=True)
    parser.add_argument("--project_id", type=str, default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", type=str, default="firefoxdotcom_derived")
    parser.add_argument(
        "--table_name", type=str, default=os.path.basename(DEFAULT_TABLE_NAME)
    )
    parser.add_argument(
        "--access_token",
        type=str,
        default=os.getenv("FB_ACCESS_TOKEN", None),
    )  # TODO: need access token
    parser.add_argument(
        "--pixel_id",
        type=str,
        default=os.getenv("FB_PIXEL_ID", None),
    )

    args = parser.parse_args()

    main(
        args.activity_date,
        args.project_id,
        args.dataset,
        args.table_name,
        args.access_token,
        args.pixel_id,
    )
