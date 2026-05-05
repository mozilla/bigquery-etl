"""Integration script to fetch Anthropic costs data and load it into BigQuery."""

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

    def load_costs_data(
        self, destination_table: str, records: list[dict], date_partition: str
    ):
        """Load costs data to partitioned BQ table."""
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        partitioned_table = f"{destination_table}${date_partition}"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("workspace_id", "STRING"),
                bigquery.SchemaField("amount", "NUMERIC"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("cost_type", "STRING"),
                bigquery.SchemaField("token_type", "STRING"),
                bigquery.SchemaField("context_window", "STRING"),
                bigquery.SchemaField("model", "STRING"),
                bigquery.SchemaField("service_tier", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("inference_geo", "STRING"),
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


class AnthropicCostsAPI:
    """API client for fetching costs data from the Anthropic Admin API."""

    BASE_URL = "https://api.anthropic.com/v1/organizations/cost_report"

    def __init__(self) -> None:
        """Initialize AnthropicCostsAPI with API key from environment."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = os.environ.get("ANTHROPIC_ADMIN_API_KEY")
        if not self.api_key:
            self.logger.critical("ANTHROPIC_ADMIN_API_KEY environment variable not set")
            sys.exit(1)

    def get_costs_for_date(self, date: datetime) -> list[dict]:
        """Fetch costs data for a specific date, grouped by workspace and description."""
        starting_at = date.strftime("%Y-%m-%dT00:00:00Z")
        ending_at = (date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        bucket_date = date.strftime("%Y-%m-%d")

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
                "group_by[]": ["workspace_id", "description"],
            }
            if page:
                params["page"] = page

            try:
                response = requests.get(self.BASE_URL, headers=headers, params=params)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.critical(f"Failed while fetching data from {self.BASE_URL}")
                sys.exit(1)

            if not (200 <= response.status_code <= 299):
                self.logger.error(
                    f"ERROR: response.status_code = {response.status_code}"
                )
                self.logger.error(f"ERROR: response.text = {response.text}")
                self.logger.critical(f"Failed while fetching data from {self.BASE_URL}")
                sys.exit(1)

            data = response.json()

            for bucket in data.get("data", []):
                for result in bucket.get("results", []):
                    record = {
                        "date": bucket_date,
                        "workspace_id": result.get("workspace_id"),
                        "amount": round(float(result.get("amount") or 0), 9),
                        "currency": result.get("currency"),
                        "cost_type": result.get("cost_type"),
                        "token_type": result.get("token_type"),
                        "context_window": result.get("context_window"),
                        "model": result.get("model"),
                        "service_tier": result.get("service_tier"),
                        "description": result.get("description"),
                        "inference_geo": result.get("inference_geo"),
                    }
                    all_records.append(record)

            if not data.get("has_more", False):
                break

            page = data.get("next_page")

        return all_records


class AnthropicCostsBigQueryIntegration:
    """Integration orchestrator for fetching Anthropic costs data and loading to BigQuery."""

    def __init__(self) -> None:
        """Initialize integration with a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        """Execute the integration workflow to fetch and load Anthropic costs data."""
        self.logger.info("Starting Anthropic Costs BigQuery Integration ...")

        date = datetime.strptime(args.date, "%Y-%m-%d")
        date_partition = date.strftime("%Y%m%d")

        anthropic_api = AnthropicCostsAPI()
        bq_api = BigQueryAPI()

        records = anthropic_api.get_costs_for_date(date)
        self.logger.info(f"Fetched {len(records)} cost records for {args.date}")

        if records:
            bq_api.load_costs_data(args.destination, records, date_partition)
        else:
            self.logger.info("No cost records found for this date")

        self.logger.info("End of Anthropic Costs BigQuery Integration")


def main():
    """Run the Anthropic costs data integration script."""
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.ai_usage_derived.claude_costs_v1",
        required=False,
    )
    parser.add_argument(
        "--date",
        dest="date",
        required=True,
        help="Date to fetch costs data for (YYYY-MM-DD format)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = AnthropicCostsBigQueryIntegration()
    integration.run(args)


if __name__ == "__main__":
    main()
