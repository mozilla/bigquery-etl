"""Script for pulling fb conversion events and pushing them to Meta Conversions API."""

# https://developers.facebook.com/docs/marketing-api/conversions-api/get-started

import logging
import os
import sys
from argparse import ArgumentParser
from datetime import date
from pathlib import Path
from typing import Any, List

from facebook_business.adobjects.serverside.action_source import ActionSource
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi
from google.cloud import bigquery
from pandas import DataFrame

from bigquery_etl.schema import Schema

API_URI = "https://graph.facebook.com/"
API_VERSION = "v25.0"
API_ENDPOINT = "events"

DEFAULT_TABLE_NAME = Path(__file__).parent

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()
PARTITION_FIELD = "submission_date"


SQL_QUERY = """
SELECT
  submission_date,
  (activity_date).TIMESTAMP().UNIX_SECONDS() AS activity_unix_timestamp,
  -- Expected fbc format: 'fb.subdomain_index.creation_time.fbclid'
  CONCAT("fb.0.", CAST((ga_event_timestamp).TIMESTAMP_MICROS().UNIX_SECONDS() AS STRING), ".", fbclid) AS fbc,
  conversion_name,
  CURRENT_TIMESTAMP() AS run_timestamp,
FROM
  `moz-fx-data-shared-prod.firefoxdotcom.fbclid_desktop_conversion_events`
WHERE
  submission_date = @submission_date
  AND ga_country IN ("United States", "India")
"""

EXPORT_QUERY = """
SELECT activity_unix_timestamp, fbc, conversion_name
FROM `{source_table}`
WHERE submission_date = @submission_date
"""


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of specified size."""
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def update_table(
    bq_client: bigquery.Client,
    submission_date: date,
    query: str,
    destination_table: str,
) -> None:
    """Run query and store the result inside BQ (destination) table to act as store for data export."""
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        time_partitioning=bigquery.TimePartitioning(field=PARTITION_FIELD),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        use_legacy_sql=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("submission_date", "DATE", submission_date),
        ],
    )

    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()


def get_results_from_bigquery(
    bq_client: bigquery.Client, submission_date: date, query: str
) -> DataFrame:
    """Prepare and retrieve data from a BQ table that should be sent to the Conversions API."""
    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("submission_date", "DATE", submission_date),
        ],
    )

    return bq_client.query(query=query, job_config=job_config).to_dataframe()


# Looks like there's a library for building event:
# https://developers.facebook.com/documentation/ads-commerce/conversions-api/guides/business-sdk-features#http-service-interface
def create_event(event_data: DataFrame) -> Event:
    """Format data into Event."""
    return Event(
        event_name=event_data["conversion_name"],
        event_time=event_data["activity_unix_timestamp"],
        user_data=UserData(
            fbc=event_data["fbc"],
        ),
        action_source=ActionSource.PHYSICAL_STORE,
    )


def execute_request(pixel_id: int, events: List[Event]) -> Any:
    """Send event info to Conversions API."""
    event_request = EventRequest(
        pixel_id=pixel_id,
        events=events,
    )
    return event_request.execute()


def main(
    submission_date: date,
    project_id: str,
    dataset: str,
    table_name: str,
    access_token: str,
    pixel_id: int,
) -> None:
    """Update table to include data to be pushed to Conversions API, retrieve data for a specific submission_date, format and send it to Conversions API."""
    if not (pixel_id and access_token):
        raise Exception(
            "Missing required test config. Please make sure both pixel_id and access_token are provided."
        )

    bq_client = bigquery.Client(project_id)

    partition_decorator = str(submission_date).replace("-", "")
    destination_table = f"{project_id}.{dataset}.{table_name}"
    destination_table_with_decorator = f"{destination_table}${partition_decorator}"

    logging.info("Updating table: %s" % destination_table_with_decorator)

    update_table(
        bq_client,
        submission_date,
        SQL_QUERY,
        destination_table_with_decorator,
    )

    logging.info("Finished updating table: %s" % destination_table_with_decorator)
    logging.info(
        "Getting results to export from table: %s for date: %s"
        % (destination_table, submission_date)
    )

    result_df = get_results_from_bigquery(
        bq_client,
        submission_date,
        EXPORT_QUERY.format(source_table=destination_table),
    )
    logging.info(
        "Finished getting results to export from table: %s for date: %s"
        % (destination_table, submission_date)
    )

    if len(result_df) == 0:
        # TODO: should this cause a failure?
        logging.warning("No results found for export.")
        return

    conversion_events = [
        create_event(conversion_event[1]) for conversion_event in result_df.iterrows()
    ]

    logging.info(
        "Num of conversion events ready for export: %s" % len(conversion_events)
    )

    FacebookAdsApi.init(access_token=access_token, crash_log=False)

    for batch_num, batch in enumerate(chunk_list(conversion_events, 1000), start=1):
        logging.info("Processing batch number: %s" % batch_num)

        response = execute_request(pixel_id=pixel_id, events=batch)
        logging.info(
            "Finished processing batch number: %s, response: %s."
            % (batch_num, response)
        )


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--submission_date", type=date.fromisoformat, required=True)
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
        type=int,
        default=os.getenv("FB_PIXEL_ID", None),
    )

    args = parser.parse_args()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    main(
        submission_date=args.submission_date,
        project_id=args.project_id,
        dataset=args.dataset,
        table_name=args.table_name,
        access_token=args.access_token,
        pixel_id=args.pixel_id,
    )
