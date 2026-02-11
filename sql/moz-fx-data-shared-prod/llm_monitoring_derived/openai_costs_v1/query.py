"""Integration script to fetch OpenAI costs data and load it into BigQuery."""

import logging
import os
import sys
from argparse import ArgumentParser
from datetime import datetime

import google.auth
import requests
from google.cloud import bigquery


class BigQueryAPI:
    """API client for loading data into BigQuery tables."""

    def __init__(self) -> None:
        """Initialize BigQueryAPI with a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_usage_data(self, destination_table: str, buckets: list[dict]):
        """Load usage data to partitioned BQ table."""
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        records = []
        for bucket in buckets:

            bucket_date = datetime.fromtimestamp(bucket["start_time"]).strftime(
                "%Y-%m-%d"
            )

            for result in bucket.get("results", []):
                record = {
                    "date": bucket_date,
                    "project_id": result.get("project_id"),
                    "line_item": result.get("line_item"),
                    "organization_id": result.get("organization_id"),
                    "amount_value": result.get("amount", {}).get("value"),
                    "currency": result.get("amount", {}).get("currency"),
                }
                records.append(record)

        # Use the partition decorator to write to specific partition
        partitioned_table = destination_table

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("project_id", "STRING"),
                bigquery.SchemaField("line_item", "STRING"),
                bigquery.SchemaField("organization_id", "STRING"),
                bigquery.SchemaField("amount_value", "FLOAT"),
                bigquery.SchemaField("currency", "STRING"),
            ],
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )

        job = client.load_table_from_json(
            records, partitioned_table, job_config=job_config
        )
        job.result()
        self.logger.info(f"Loaded {len(records)} records to {partitioned_table}")


class OpenAICostsAPI:
    """API client for fetching costs data from OpenAI."""

    BASE_URL = "https://api.openai.com/v1/organization/costs"

    def __init__(self) -> None:
        """Initialize OpenAICostsAPI with API key from environment."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = os.environ.get("OPENAI_ADMIN_API_KEY")
        if not self.api_key:
            self.logger.critical("OPENAI_ADMIN_API_KEY environment variable not set")
            sys.exit(1)

    def get_usage_for_date(self, date) -> list[dict]:
        """Fetch usage data for a specific date, grouped by project and line item."""
        start_time = int(date.timestamp())

        OPENAI_ADMIN_API_KEY = os.environ.get("OPENAI_ADMIN_API_KEY")

        headers = {
            "Authorization": f"Bearer {OPENAI_ADMIN_API_KEY}",
            "Content-Type": "application/json",
        }

        params = {
            "start_time": start_time,  # Required: Start time (Unix seconds)
            "end_time": start_time,
            "bucket_width": "1d",  # Optional: '1m', '1h', or '1d' (default '1d')
            "group_by": ["project_id", "line_item"],  # Optional: Fields to group by
            "limit": 100,  # Optional: Number of buckets to return, this will chunk the data into 100 buckets
        }

        all_data = []

        page_cursor = None

        while True:
            if page_cursor:
                params["page"] = page_cursor

            response = requests.get(self.BASE_URL, headers=headers, params=params)

            if response.status_code == 200:
                data_json = response.json()
                all_data.extend(data_json.get("data", []))

                page_cursor = data_json.get("next_page")
                if not page_cursor:
                    break
            else:
                print(f"Error: {response.status_code}")
                break

        if all_data:
            print("Data retrieved successfully!")
        else:
            print("Issue: No data available to retrieve.")
        return all_data


class OpenAIBigQueryIntegration:
    """Integration orchestrator for fetching OpenAI costs data and loading to BigQuery."""

    def __init__(self) -> None:
        """Initialize integration with a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        """Execute the integration workflow to fetch and load OpenAI costs data."""
        self.logger.info("Starting OpenAI Costs BigQuery Integration ...")

        date = datetime.strptime(args.date, "%Y-%m-%d")
        date_partition = date.strftime("%Y%m%d")

        openai_api = OpenAICostsAPI()
        bq_api = BigQueryAPI()

        records = openai_api.get_usage_for_date(date)

        self.logger.info(f"Fetched {len(records)} usage records for {args.date}")

        if records:
            bq_api.load_usage_data(args.destination, records, date_partition)
        else:
            self.logger.info("No usage records found for this date")

        self.logger.info("End of OpenAI Costs BigQuery Integration")


def main():
    """Run the OpenAI costs data integration script."""
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.llm_monitoring_derived.openai_costs_v1",
        required=False,
    )
    parser.add_argument(
        "--date",
        dest="date",
        required=True,
        help="Date to fetch usage data for (YYYY-MM-DD format)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = OpenAIBigQueryIntegration()
    integration.run(args)


if __name__ == "__main__":
    main()
