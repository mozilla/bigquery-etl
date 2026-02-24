from argparse import ArgumentParser
from datetime import datetime, timezone
import logging
import os
import sys

from google.cloud import bigquery
import google.auth
import requests
from requests.auth import HTTPBasicAuth


OUTPUT_SCHEMA = [
    bigquery.SchemaField("issue_key", "STRING"),
    bigquery.SchemaField("project_key", "STRING"),
    bigquery.SchemaField("issue_type", "STRING"),
    bigquery.SchemaField("summary", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("created", "TIMESTAMP"),
    bigquery.SchemaField("resolved", "TIMESTAMP"),
]

REQUEST_FIELDS = [
    "summary",
    "status",
    "issuetype",
    "project",
    "created",
    "resolutiondate",
]


class BigQueryAPI:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_issue_data(self, destination_table: str, issues: list[dict]):
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        client = bigquery.Client(credentials=credentials, project=project)

        job_config = bigquery.LoadJobConfig(
            schema=OUTPUT_SCHEMA,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = client.load_table_from_json(issues, destination_table, job_config=job_config)
        job.result()
        self.logger.info("Loaded %s records to %s", len(issues), destination_table)


class JiraAPI:
    SEARCH_ENDPOINT = "/rest/api/3/search/jql"

    def __init__(self, args) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        jira_username = os.path.expandvars("$JIRA_USERNAME")
        jira_token = os.path.expandvars("$JIRA_TOKEN")

        if not jira_username or jira_username == "$JIRA_USERNAME":
            self.logger.critical("JIRA_USERNAME environment variable not set")
            sys.exit(1)
        if not jira_token or jira_token == "$JIRA_TOKEN":
            self.logger.critical("JIRA_TOKEN environment variable not set")
            sys.exit(1)

        self.base_jira_url = args.base_jira_url.rstrip("/")
        self.jql = args.jql
        self.auth = HTTPBasicAuth(jira_username, jira_token)

    @staticmethod
    def _to_bq_timestamp(value: str | None) -> str | None:
        if not value:
            return None

        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return (
                    datetime.strptime(value, fmt)
                    .astimezone(timezone.utc)
                    .isoformat(timespec="seconds")
                )
            except ValueError:
                continue

        return None

    def get_issues(self, max_results: int = 100) -> list[dict]:
        headers = {"Accept": "application/json"}
        issues = []
        next_page_token = None

        while True:
            url = f"{self.base_jira_url}{self.SEARCH_ENDPOINT}"
            params = {
                "jql": self.jql,
                "maxResults": max_results,
                "fields": ",".join(REQUEST_FIELDS),
            }
            if next_page_token:
                params["nextPageToken"] = next_page_token

            try:
                response = requests.get(
                    url,
                    headers=headers,
                    auth=self.auth,
                    params=params,
                    timeout=60,
                )
            except Exception as e:
                self.logger.error(str(e))
                self.logger.critical("Failed while getting Jira issues")
                sys.exit(1)

            if not (200 <= response.status_code <= 299):
                self.logger.error("ERROR: response.status_code = %s", response.status_code)
                self.logger.error("ERROR: response.text = %s", response.text)
                self.logger.error("ERROR: response.reason = %s", response.reason)
                self.logger.critical("Failed while getting Jira issues")
                sys.exit(1)

            payload = response.json()
            raw_issues = payload.get("issues", [])

            for issue in raw_issues:
                fields = issue.get("fields", {})
                issues.append(
                    {
                        "issue_key": issue.get("key"),
                        "project_key": (fields.get("project") or {}).get("key"),
                        "issue_type": (fields.get("issuetype") or {}).get("name"),
                        "summary": fields.get("summary"),
                        "status": (fields.get("status") or {}).get("name"),
                        "created": self._to_bq_timestamp(fields.get("created")),
                        "resolved": self._to_bq_timestamp(fields.get("resolutiondate")),
                    }
                )

            next_page_token = payload.get("nextPageToken")
            if not raw_issues or not next_page_token:
                break

        return issues


class JiraIssueBigQueryIntegration:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        self.logger.info("Starting Jira Issue BigQuery Integration ...")

        jira = JiraAPI(args)
        bq_api = BigQueryAPI()

        issues = jira.get_issues()
        self.logger.info("Fetched %s Jira issues", len(issues))

        bq_api.load_issue_data(args.destination, issues)

        self.logger.info("End of Jira Issue BigQuery Integration")


def main(default_destination: str, default_jql: str):
    parser = ArgumentParser()
    parser.add_argument(
        "--destination",
        dest="destination",
        default=default_destination,
        required=False,
    )
    parser.add_argument(
        "--base-url",
        dest="base_jira_url",
        default="https://mozilla-hub.atlassian.net",
        required=False,
    )
    parser.add_argument(
        "--jql",
        dest="jql",
        default=default_jql,
        required=False,
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8",
    )

    integration = JiraIssueBigQueryIntegration()
    integration.run(args)
