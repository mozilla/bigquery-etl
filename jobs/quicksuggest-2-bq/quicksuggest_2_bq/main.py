# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import kinto_http
import logging
import requests

from dataclasses import asdict, dataclass
from google.cloud import bigquery
from typing import Dict, List


@dataclass
class KintoSuggestion():
    """Class that holds information about a Suggestion returned by Kinto."""

    # By being explicit about the fields we expect and not
    # just storing the raw JSON, the conversion will fail
    # if there's new unexpected fields, ensuring we take the
    # appropriate actions to update the table schemas.
    advertiser: str
    click_url: str
    iab_category: str
    icon: str
    impression_url: str
    id: int
    keywords: List[str]
    title: str
    url: str


def download_suggestions(client: kinto_http.Client) -> Dict[int, KintoSuggestion]:
    """Get records, download attachments and return the suggestions."""

    # Retrieve the base_url for attachments
    server_info = client.server_info()
    attachments_base_url = server_info["capabilities"]["attachments"]["base_url"]

    # Only consider "data" records, search for the following code in Merino
    # for record in remote_settings_client.records_of_type("data".to_string())
    data_records = [
        record for record in client.get_records() if record["type"] == "data"
    ]

    # Make use of connection pooling because all requests go to the same host
    requests_session = requests.Session()

    suggestions = {}

    for record in data_records:
        attachment_url = f"{attachments_base_url}{record['attachment']['location']}"

        response = requests_session.get(attachment_url)

        if response.status_code != 200:
            # Ignore unsuccessful requests for now
            logging.error(
                (
                    "Failed to download attachment for record with ID '%s'."
                    " Response status code %s."
                ),
                record["id"],
                response.status_code,
            )
            continue

        # Each attachment is a list of suggestion objects and each suggestion
        # object contains a list of keywords. Load the suggestions into pydantic
        # model instances to discard all fields which we don't care about here.
        suggestions.update(
            {
                suggestion_data["id"]: KintoSuggestion(**suggestion_data)
                for suggestion_data in response.json()
            }
        )

    return suggestions


def store_suggestions(
    date: str,
    destination_table_id: str,
    kinto_suggestions: Dict[int, KintoSuggestion]
):
    """Get records, download attachments and return the suggestions."""

    # Turn the suggestions into dicts and augment them with
    # an insertion date.
    suggestions = [
        {**asdict(suggestion), "insert_date": date}
        for suggestion in kinto_suggestions.values()
    ]

    client = bigquery.Client()

    # This assumes the table was already created and the fields in the
    # suggestions match the schema of the table.
    errors = client.insert_rows_json(destination_table_id, suggestions)
    if not errors:
        logging.info(
            (
                f"Added {len(suggestions)} suggestions to {destination_table_id}"
                f" for {date}"
            )
        )
    else:
        logging.error(f"Encountered errors while appending rows: {errors}")


@click.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="date for which to store the results"
)
@click.option(
    "--destination-table-id",
    required=True,
    type=str,
    help="the table id to append data to, e.g. `projectid.dataset.table`"
)
@click.option(
    "--kinto-server",
    default="https://firefox.settings.services.mozilla.com",
    type=str,
    help="the Kinto server to fetch the data from"
)
@click.option(
    "--kinto-bucket",
    default="main",
    type=str,
    help="the Kinto bucket to fetch the data from"
)
@click.option(
    "--kinto-collection",
    default="quicksuggest",
    type=str,
    help="the Kinto server to fetch the data from"
)
def main(
    date,
    destination_table_id,
    kinto_server,
    kinto_bucket,
    kinto_collection
):
    kinto_client = kinto_http.Client(
        server_url=kinto_server,
        bucket=kinto_bucket,
        collection=kinto_collection,
    )

    logging.info(
        (
            f"Downloading suggestions from {kinto_server}"
            f" [{kinto_bucket}/{kinto_collection}]"
        )
    )
    kinto_suggestions = download_suggestions(kinto_client)

    logging.info(f"Downloaded {len(kinto_suggestions.keys())} suggestions")

    store_suggestions(date, destination_table_id, kinto_suggestions)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
