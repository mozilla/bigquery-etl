"""Upload changes in Suppression list to Campaign Monitor API."""

from pathlib import Path
from typing import List

import requests
import rich_click as click
from google.cloud import bigquery
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from bigquery_etl.schema import Schema

BASE_URL = "https://api.createsend.com/api/v3.3/clients"
UPLOAD_BATCH_SIZE = 100

PROJECT_ID = Path(__file__).parent.parent.parent.name
DATASET_ID = Path(__file__).parent.parent.name
TABLE_NAME = Path(__file__).parent.name

SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()


def list_new_suppressions() -> List[str]:
    """Download the new_suppression_list_entries_for_mofo_v1 table into an array."""
    client = bigquery.Client()
    query = """
        SELECT
            main.email
        FROM
            `moz-fx-data-shared-prod.marketing_suppression_list_derived.main_suppression_list_v1` AS main
        LEFT JOIN
            `moz-fx-data-shared-prod.marketing_suppression_list_external.campaign_monitor_suppression_list_v1` AS current_mofo
            ON main.email = current_mofo.email
        LEFT JOIN
            `moz-fx-data-shared-prod.marketing_suppression_list_external.send_suppression_list_update_to_campaign_monitor_v1` AS not_uploadable
            ON main.email = not_uploadable.email
        WHERE
            current_mofo.email IS NULL
            AND not_uploadable.email IS NULL
    """
    data = client.query(query).result()
    return [row["email"] for row in list(data)]


def upload_new_suppressions_to_campaign_monitor(
    base_url: str, client_id: str, api_key: str, emails: List[str]
) -> None:
    """Add emails to suppression list in Campaign Monitor."""
    auth = HTTPBasicAuth(api_key, "")
    add_suppression_list_url = f"{base_url}/{client_id}/suppress.json"
    payload = {"EmailAddresses": emails}
    response = requests.post(url=add_suppression_list_url, auth=auth, json=payload)
    response.raise_for_status()


def store_data_in_bigquery(data, schema, destination_project, destination_table_id):
    """Upload data to Bigquery in a single, non partitioned table."""
    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_json(
        data, destination_table_id, location="US", job_config=job_config
    )
    load_job.result()
    click.echo(f"Loaded {len(data)} new rows into {destination_table_id}.")


@click.command
@click.option(
    "--api_key",
    required=True,
    help="Campaign Monitor API key to use for authentication.",
)
@click.option(
    "--client_id",
    required=True,
    help="Client Id for Campaign Monitor.",
)
def main(api_key: str, client_id: str) -> None:
    """Download list of new suppression from BigQuery and upload it to the Campaign Monitor API."""
    base_url = BASE_URL
    # Get emails to upload
    emails = list_new_suppressions()
    number_of_emails = len(emails)

    # try to upload in UPLOAD_BATCH_SIZE chunks
    not_uploaded_emails = []
    uploaded = 0
    for i in range(0, number_of_emails, UPLOAD_BATCH_SIZE):
        partial_emails = emails[i : min(i + UPLOAD_BATCH_SIZE, number_of_emails)]
        try:
            upload_new_suppressions_to_campaign_monitor(
                base_url=base_url,
                api_key=api_key,
                client_id=client_id,
                emails=partial_emails,
            )
            uploaded += len(partial_emails)
            click.echo(f"Uploaded {uploaded}/{number_of_emails} emails.")
        except HTTPError as err:
            click.echo(err)
            not_uploaded_emails += partial_emails

    # try to upload the emails that were not able to upload in batch one by one
    not_uploadable_emails = []
    for email in not_uploaded_emails:
        try:
            upload_new_suppressions_to_campaign_monitor(
                base_url=base_url, api_key=api_key, client_id=client_id, emails=[email]
            )
        except HTTPError as err:
            click.echo(err)
            not_uploadable_emails.append({"email": email})

    # add not uploadable emails in table
    if len(not_uploadable_emails) > 0:
        destination_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        store_data_in_bigquery(
            data=not_uploadable_emails,
            schema=SCHEMA,
            destination_project=PROJECT_ID,
            destination_table_id=destination_table_id,
        )
        click.echo(
            f"Added {len(not_uploadable_emails)} emails to the list of emails that can't be uploaded due to bad formatting."
        )


if __name__ == "__main__":
    main()
