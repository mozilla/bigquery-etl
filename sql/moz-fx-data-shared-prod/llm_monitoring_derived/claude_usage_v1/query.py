"""Integration script to fetch Claude API usage data and load it into BigQuery."""

import logging
import os
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta

import google.auth
import requests
from google.cloud import bigquery


class BigQueryAPI:
    """API client for loading data into BigQuery tables."""

    def __init__(self) -> None:
        """Initialize BigQueryAPI with a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_usage_data(
        self, destination_table: str, records: list[dict], date_partition: str
    ):
        """Load usage data to partitioned BQ table."""
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        # Use the partition decorator to write to specific partition
        partitioned_table = f"{destination_table}${date_partition}"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("api_key_id", "STRING"),
                bigquery.SchemaField("model", "STRING"),
                bigquery.SchemaField("uncached_input_tokens", "INTEGER"),
                bigquery.SchemaField("cache_read_input_tokens", "INTEGER"),
                bigquery.SchemaField("cache_creation_5m_input_tokens", "INTEGER"),
                bigquery.SchemaField("cache_creation_1h_input_tokens", "INTEGER"),
                bigquery.SchemaField("output_tokens", "INTEGER"),
                bigquery.SchemaField("web_search_requests", "INTEGER"),
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


class AnthropicUsageAPI:
    """API client for fetching usage data from Anthropic."""

    BASE_URL = "https://api.anthropic.com/v1/organizations/usage_report/messages"

    def __init__(self) -> None:
        """Initialize AnthropicUsageAPI with API key from environment."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = os.environ.get("ANTHROPIC_ADMIN_API_KEY")
        if not self.api_key:
            self.logger.critical("ANTHROPIC_ADMIN_API_KEY environment variable not set")
            sys.exit(1)

    def get_usage_for_date(self, date: datetime) -> list[dict]:
        """Fetch usage data for a specific date, grouped by model and API key."""
        starting_at = date.strftime("%Y-%m-%dT00:00:00Z")
        ending_at = (date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

        headers = {
            "anthropic-version": "2023-06-01",
            "x-api-key": self.api_key,
        }

        all_records = []
        page = None

        while True:
            params = {
                "starting_at": starting_at,
                "ending_at": ending_at,
                "bucket_width": "1d",
                "group_by[]": ["model", "api_key_id"],
            }
            if page:
                params["page"] = page

            try:
                response = requests.get(self.BASE_URL, headers=headers, params=params)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.critical(
                    "Failed while fetching usage data from Anthropic API"
                )
                sys.exit(1)

            if not (200 <= response.status_code <= 299):
                self.logger.error(
                    f"ERROR: response.status_code = {response.status_code}"
                )
                self.logger.error(f"ERROR: response.text = {response.text}")
                self.logger.critical(
                    "Failed while fetching usage data from Anthropic API"
                )
                sys.exit(1)

            data = response.json()

            for bucket in data.get("data", []):
                bucket_date = bucket["starting_at"][:10]  # Extract YYYY-MM-DD
                for result in bucket.get("results", []):
                    cache_creation = result.get("cache_creation", {})
                    server_tool_use = result.get("server_tool_use", {})

                    record = {
                        "date": bucket_date,
                        "api_key_id": result.get("api_key_id"),
                        "model": result.get("model"),
                        "uncached_input_tokens": result.get("uncached_input_tokens", 0),
                        "cache_read_input_tokens": result.get(
                            "cache_read_input_tokens", 0
                        ),
                        "cache_creation_5m_input_tokens": cache_creation.get(
                            "ephemeral_5m_input_tokens", 0
                        ),
                        "cache_creation_1h_input_tokens": cache_creation.get(
                            "ephemeral_1h_input_tokens", 0
                        ),
                        "output_tokens": result.get("output_tokens", 0),
                        "web_search_requests": server_tool_use.get(
                            "web_search_requests", 0
                        ),
                    }
                    all_records.append(record)

            if not data.get("has_more", False):
                break

            page = data.get("next_page")

        return all_records


class AnthropicBigQueryIntegration:
    """Integration orchestrator for fetching Claude usage data and loading to BigQuery."""

    def __init__(self) -> None:
        """Initialize integration with a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        """Execute the integration workflow to fetch and load Claude usage data."""
        self.logger.info("Starting Anthropic Usage BigQuery Integration ...")

        date = datetime.strptime(args.date, "%Y-%m-%d")
        date_partition = date.strftime("%Y%m%d")

        anthropic_api = AnthropicUsageAPI()
        bq_api = BigQueryAPI()

        records = anthropic_api.get_usage_for_date(date)
        self.logger.info(f"Fetched {len(records)} usage records for {args.date}")

        if records:
            bq_api.load_usage_data(args.destination, records, date_partition)
        else:
            self.logger.info("No usage records found for this date")

        self.logger.info("End of Anthropic Usage BigQuery Integration")


def main():
    """Run the Claude usage data integration script."""
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.claude_api_derived.usage_v1",
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

    integration = AnthropicBigQueryIntegration()
    integration.run(args)


if __name__ == "__main__":
    main()
