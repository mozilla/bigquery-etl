"""Upload changes in Suppression list to Campaign Monitor API."""

from typing import List

import requests
import rich_click as click
from google.cloud import bigquery
from requests.auth import HTTPBasicAuth

BASE_URL = "https://api.createsend.com/api/v3.3/clients"


def list_new_suppressions() -> List[str]:
    """Download the new_suppression_list_entries_for_mofo_v1 table into an array."""
    client = bigquery.Client()
    query = """
        SELECT
            email
        FROM
            `moz-fx-data-shared-prod.marketing_suppression_list_external.new_suppression_list_entries_for_mofo_v1`
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
def main(api_key: str, client_id: str, base_url: str) -> None:
    """Download list of new suppression from BigQuery and upload it to the Campaign Monitor API."""
    emails = list_new_suppressions()
    upload_new_suppressions_to_campaign_monitor(
        base_url=base_url, api_key=api_key, client_id=client_id, emails=emails
    )


if __name__ == "__main__":
    main(base_url=BASE_URL)
