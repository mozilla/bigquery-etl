from argparse import ArgumentParser
import os
import sys
import logging

import requests
from google.cloud import bigquery
import google.auth


class BigQueryAPI:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_api_keys_data(self, destination_table: str, records: list[dict]):
        """Load API keys data to BQ table."""
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("api_key_id", "STRING"),
                bigquery.SchemaField("api_key_name", "STRING"),
                bigquery.SchemaField("api_key_status", "STRING"),
                bigquery.SchemaField("created_by_user_id", "STRING"),
                bigquery.SchemaField("user_email", "STRING"),
                bigquery.SchemaField("user_name", "STRING"),
                bigquery.SchemaField("workspace_id", "STRING"),
            ],
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = client.load_table_from_json(records, destination_table, job_config=job_config)
        job.result()
        self.logger.info(f"Loaded {len(records)} records to {destination_table}")


class AnthropicAdminAPI:
    USERS_URL = "https://api.anthropic.com/v1/organizations/users"
    API_KEYS_URL = "https://api.anthropic.com/v1/organizations/api_keys"

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = os.environ.get("ANTHROPIC_ADMIN_API_KEY")
        if not self.api_key:
            self.logger.critical("ANTHROPIC_ADMIN_API_KEY environment variable not set")
            sys.exit(1)

    def _get_headers(self):
        return {
            "anthropic-version": "2023-06-01",
            "x-api-key": self.api_key,
        }

    def _fetch_paginated(self, url: str, data_key: str = "data") -> list[dict]:
        """Fetch all pages of data from a paginated endpoint."""
        all_records = []
        params = {"limit": 100}

        while True:
            try:
                response = requests.get(url, headers=self._get_headers(), params=params)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.critical(f"Failed while fetching data from {url}")
                sys.exit(1)

            if not (200 <= response.status_code <= 299):
                self.logger.error(f"ERROR: response.status_code = {response.status_code}")
                self.logger.error(f"ERROR: response.text = {response.text}")
                self.logger.critical(f"Failed while fetching data from {url}")
                sys.exit(1)

            data = response.json()
            all_records.extend(data.get(data_key, []))

            if not data.get("has_more", False):
                break

            params["after_id"] = data.get("last_id")

        return all_records

    def get_users(self) -> dict[str, dict]:
        """Fetch all users and return as dict keyed by user_id."""
        users = self._fetch_paginated(self.USERS_URL)
        return {user["id"]: user for user in users}

    def get_api_keys(self) -> list[dict]:
        """Fetch all API keys."""
        return self._fetch_paginated(self.API_KEYS_URL)


class AnthropicAPIKeysBigQueryIntegration:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        self.logger.info("Starting Anthropic API Keys BigQuery Integration ...")

        anthropic_api = AnthropicAdminAPI()
        bq_api = BigQueryAPI()

        # Fetch users and API keys
        users = anthropic_api.get_users()
        self.logger.info(f"Fetched {len(users)} users")

        api_keys = anthropic_api.get_api_keys()
        self.logger.info(f"Fetched {len(api_keys)} API keys")

        # Join API keys with user info
        records = []
        for key in api_keys:
            created_by = key.get("created_by", {})
            created_by_user_id = created_by.get("id") if created_by else None
            user = users.get(created_by_user_id, {})

            record = {
                "api_key_id": key.get("id"),
                "api_key_name": key.get("name"),
                "api_key_status": key.get("status"),
                "created_by_user_id": created_by_user_id,
                "user_email": user.get("email"),
                "user_name": user.get("name"),
                "workspace_id": key.get("workspace_id"),
            }
            records.append(record)

        if records:
            bq_api.load_api_keys_data(args.destination, records)
        else:
            self.logger.info("No API keys found")

        self.logger.info("End of Anthropic API Keys BigQuery Integration")


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default="moz-fx-data-shared-prod.claude_api_derived.api_keys_v1",
        required=False,
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = AnthropicAPIKeysBigQueryIntegration()
    integration.run(args)


if __name__ == "__main__":
    main()
