#!/usr/bin/env python3

"""Collect tables and views marked as deprecated in bigquery-etl metadata.

Scans all metadata.yaml files in the sql/ directory for `deprecated: true` and
stores tables in `monitoring_derived.deprecated_tables_v1`. Tables past their
deletion_date are reported to Slack.
"""

import logging
import os
from datetime import date
from functools import lru_cache
from pathlib import Path

import click
import requests
from google.cloud import bigquery

from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata

logger = logging.getLogger(__name__)

DELETION_REQUEST_URL = (
    "https://mozilla-hub.atlassian.net/jira/software/c/projects/DENG/form/1610"
)

SCHEMA = [
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("deletion_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("owners", "STRING", mode="REPEATED"),
]


@click.command
@click.option("--sql-dir", "--sql_dir", default="sql")
@click.option("--project", default="moz-fx-data-shared-prod")
@click.option(
    "--destination-dataset", "--destination_dataset", default="monitoring_derived"
)
@click.option(
    "--destination-table", "--destination_table", default="deprecated_tables_v1"
)
@click.option("--slack-channel", "--slack_channel", default="#dataops-alerts")
def main(sql_dir, project, destination_dataset, destination_table, slack_channel):
    """Write deprecated tables to the destination table."""
    logging.basicConfig(level=logging.INFO)

    rows = get_deprecated_tables(sql_dir)
    logger.info(f"Found {len(rows)} deprecated tables")

    client = bigquery.Client(project)
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    destination = f"{project}.{destination_dataset}.{destination_table}"
    client.load_table_from_json(rows, destination, job_config=job_config).result()

    today = date.today().isoformat()
    past_deletion_date = [
        row for row in rows if row["deletion_date"] and row["deletion_date"] < today
    ]
    if not past_deletion_date:
        logger.info("No deprecated tables past their deletion date")
        return

    token = os.getenv("SLACK_API_KEY")
    if not token:
        raise ValueError("Environment variable SLACK_API_KEY is not set!")

    send_slack_message(
        token, slack_channel, format_slack_message(token, past_deletion_date)
    )
    logger.info(f"Reported {len(past_deletion_date)} tables to {slack_channel}")


def get_deprecated_tables(sql_dir):
    """Return each table whose metadata has it marked as deprecated."""
    rows = []
    for metadata_file in sorted(Path(sql_dir).glob(f"*/*/*/{METADATA_FILE}")):
        try:
            metadata = Metadata.from_file(metadata_file)
        except Exception as e:
            logger.warning(f"Skipping invalid metadata {metadata_file}: {e}")
            continue

        if not metadata.deprecated:
            continue

        table_dir = metadata_file.parent
        rows.append(
            {
                "project_id": table_dir.parent.parent.name,
                "dataset_id": table_dir.parent.name,
                "table_id": table_dir.name,
                "deletion_date": (
                    metadata.deletion_date.isoformat()
                    if metadata.deletion_date
                    else None
                ),
                "owners": metadata.owners,
            }
        )
    return rows


@lru_cache
def lookup_slack_user_id(token, email):
    """Return the Slack user ID for an email address, or None if not found."""
    response = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        headers={"Authorization": f"Bearer {token}"},
        params={"email": email},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        logger.warning(f"Slack user lookup failed for {email}: {body.get('error')}")
        return None
    return body["user"]["id"]


def mention(token, owner):
    """Return a Slack mention for an owner, falling back to the owner as text."""
    # owners can also be GitHub identities, which can't be looked up in Slack
    if "@" not in owner:
        return owner
    user_id = lookup_slack_user_id(token, owner)
    return f"<@{user_id}>" if user_id else owner


def format_slack_message(token, rows):
    """Return a Slack message listing tables past their deletion date."""
    lines = [
        ":wastebasket: The following datasets are deprecated and their deletion "
        "date has passed, but they have not yet been deleted. Please delete them "
        "from BigQuery and private-/bigquery-etl or file a ticket requesting "
        f"deletion <{DELETION_REQUEST_URL}|here>."
    ]
    for row in sorted(rows, key=lambda r: r["deletion_date"]):
        table = f"{row['project_id']}.{row['dataset_id']}.{row['table_id']}"
        owners = " ".join(mention(token, owner) for owner in row["owners"])
        lines.append(
            f"• {owners} `{table}` (deletion date: {row['deletion_date']})"
        )
    return "\n".join(lines)


def send_slack_message(token, channel, text):
    """Post a message to a Slack channel."""
    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}"},
        json={"channel": channel, "text": text},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    # Slack returns HTTP 200 with ok=false for API errors
    if not body.get("ok"):
        raise RuntimeError(f"Failed to send Slack message: {body.get('error')}")


if __name__ == "__main__":
    main()
