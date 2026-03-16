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

DEFAULT_TABLE_NAME = Path(__file__).parent

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()
PARTITION_FIELD = "activity_date"


# Both contain different conversion events so union should be fine.
UNION_QUERY = """
SELECT activity_date, fbclid, ga_event_timestamp, ga_country, conversion_name,
FROM `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_2_v1`
WHERE activity_date = @activity_date AND ga_country IN ("United States", "India")
UNION ALL
SELECT activity_date, fbclid, ga_event_timestamp, ga_country, conversion_name,
FROM `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_7_v1`
WHERE activity_date = @activity_date AND ga_country IN ("United States", "India")
"""

# Expected fbc format: "fbc.{subdomain_index}.{creation_time}.{fbclid}"
DATA_SELECT_QUERY = """
SELECT activity_date, CONCAT("fbc.0", CAST(ga_event_timestamp AS STRING), ".", fbclid) AS fbc, conversion_name,
FROM `{destination_table}`
WHERE activity_date = @activity_date
"""


def update_table(bq_client, activity_date, query, destination_table):
    """Run query and store the result inside BQ (destination) table to act as store for data export."""
    query_job = bq_client.query(
        query=query,
        job_config=bigquery.QueryJobConfig(
            destination=destination_table,
            time_partitioning=bigquery.TimePartitioning(field=PARTITION_FIELD),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            use_legacy_sql=False,
            schema=SCHEMA,
            query_parameters=[
                bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date),
            ],
        ),
    )

    query_job.result()


def get_results_from_bigquery(bq_client, activity_date, query):
    """Prepare and retrieve data from a BQ table that should be sent to the Conversions API."""
    return bq_client.query(
        query=query,
        query_parameters=[
            bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date),
        ],
    ).to_dataframe()


# Looks like there's a library for building event:
# https://developers.facebook.com/documentation/ads-commerce/conversions-api/guides/business-sdk-features#http-service-interface
def create_event(event_name: str, event_timestamp: int, fbc: str) -> Event:
    """Format data into Event."""
    return Event(
        event_name=event_name,
        event_time=event_timestamp,
        user_data=UserData(
            fbc=fbc,
        ),
        action_source=ActionSource.OTHER,
    )


def execute_request(pixel_id: int, events: List[Event]):
    """Send event info to Conversions API."""
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
    """Update table to include data to be pushed to Conversions API, retrieve data for a specific activity_date, format and send it to Conversions API."""
    if not (pixel_id and access_token):
        raise Exception(
            "Missing required test config. Got pixel_id: '{pixel_id}', access_token: '{access_token}'".format(
                pixel_id=pixel_id, access_token=access_token
            )
        )

    bq_client = bigquery.Client(project_id)

    table_partition = f"{table_name}${activity_date.replace('-', '')}"
    destination_table = f"{project_id}.{dataset}.{table_partition}"

    update_table(bq_client, activity_date, UNION_QUERY, destination_table)
    result_df = get_results_from_bigquery(
        bq_client,
        activity_date,
        DATA_SELECT_QUERY.format(source_table=destination_table),
    )
    print(result_df)

    conversion_events = [
        create_event(conversion_event) for conversion_event in result_df
    ]
    print(conversion_events)
    return

    FacebookAdsApi.init(access_token=access_token, crash_log=False)

    # TODO: check this batching method works as intended.
    for batch in batched(conversion_events, 1000):
        response = execute_request(pixel_id=pixel_id, events=[batch])
        # TODO: Investigate what the response looks like and if there's some interesting information we may want to persist?
        print(response)


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
    )
    parser.add_argument(
        "--pixel_id",
        type=str,
        default=os.getenv("FB_PIXEL_ID", None),
    )

    args = parser.parse_args()

    main(
        activity_date=args.activity_date,
        project_id=args.project_id,
        dataset=args.dataset,
        table_name=args.table_name,
        access_token=args.access_token,
        pixel_id=args.pixel_id,
    )
