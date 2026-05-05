"""Fetch Jira issues and load them into BigQuery."""

import logging
import os
from datetime import datetime, timezone

import requests
from google.cloud import bigquery
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
    """BigQuery operations used by the Jira integration."""

    def __init__(self) -> None:
        """Initialize a logger for BigQuery load operations."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_issue_data(self, destination_table: str, issues: list[dict]):
        """Load Jira issue records into the destination BigQuery table."""
        job_config = bigquery.LoadJobConfig(
            schema=OUTPUT_SCHEMA,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        client = bigquery.Client()
        job = client.load_table_from_json(
            issues, destination_table, job_config=job_config
        )
        job.result()
        self.logger.info("Loaded %s records to %s", len(issues), destination_table)


class JiraAPI:
    """Client for reading issue data from the Jira search API."""

    SEARCH_ENDPOINT = "/rest/api/3/search/jql"

    def __init__(self, base_jira_url: str, jql: str) -> None:
        """Create an authenticated Jira API client from environment credentials."""
        self.logger = logging.getLogger(self.__class__.__name__)
        jira_username = os.environ.get("JIRA_USERNAME")
        jira_token = os.environ.get("JIRA_TOKEN")

        if not jira_username:
            raise ValueError("JIRA_USERNAME environment variable not set")
        if not jira_token:
            raise ValueError("JIRA_TOKEN environment variable not set")

        self.base_jira_url = base_jira_url.rstrip("/")
        self.jql = jql
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
        """Fetch issues from Jira and map fields to the BigQuery output schema."""
        headers = {"Accept": "application/json"}
        issues = []
        next_page_token = None

        while True:
            url = f"{self.base_jira_url}{self.SEARCH_ENDPOINT}"
            params: dict[str, str | int] = {
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
            except requests.RequestException as exc:
                raise RuntimeError("Failed while getting Jira issues") from exc

            if not (200 <= response.status_code <= 299):
                raise RuntimeError(
                    "Failed while getting Jira issues: "
                    f"status_code={response.status_code}, reason={response.reason}, "
                    f"response_text={response.text}"
                )

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
    """Orchestrate fetching Jira issues and loading them to BigQuery."""

    def __init__(self) -> None:
        """Initialize a logger for integration-level progress messages."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, args):
        """Run the Jira-to-BigQuery integration with parsed CLI arguments."""
        self.logger.info("Starting Jira Issue BigQuery Integration ...")

        jira = JiraAPI(base_jira_url=args.base_jira_url, jql=args.jql)
        bq_api = BigQueryAPI()

        issues = jira.get_issues()
        self.logger.info("Fetched %s Jira issues", len(issues))

        bq_api.load_issue_data(args.destination, issues)

        self.logger.info("End of Jira Issue BigQuery Integration")
