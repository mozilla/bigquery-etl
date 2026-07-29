"""Fetch Jira issues and load them into BigQuery."""

import logging

from google.cloud import bigquery

from .client import JiraClient

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
        self.client = JiraClient(base_jira_url)
        self.jql = jql

    def get_issues(self, max_results: int = 100) -> list[dict]:
        """Fetch issues from Jira and map fields to the BigQuery output schema."""
        issues = []

        for issue in self.client.paginate_token(
            self.SEARCH_ENDPOINT,
            {
                "maxResults": max_results,
                "jql": self.jql,
                "fields": ",".join(REQUEST_FIELDS),
            },
            "issues",
        ):
            fields = issue.get("fields", {})
            issues.append(
                {
                    "issue_key": issue.get("key"),
                    "project_key": (fields.get("project") or {}).get("key"),
                    "issue_type": (fields.get("issuetype") or {}).get("name"),
                    "summary": fields.get("summary"),
                    "status": (fields.get("status") or {}).get("name"),
                    "created": self.client.to_bq_timestamp(fields.get("created")),
                    "resolved": self.client.to_bq_timestamp(
                        fields.get("resolutiondate")
                    ),
                }
            )

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
