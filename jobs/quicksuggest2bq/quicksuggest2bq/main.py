# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import datetime
import kinto_http
import logging
import requests

from dataclasses import asdict, dataclass
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from typing import Any, Dict, Iterator, List, Optional


@dataclass
class FullKeyword:
    """Class that defines the record for a full keyword tuple"""

    keyword: str
    count: str


@dataclass
class KintoSuggestion:
    """Class that holds information about a Suggestion returned by Kinto."""

    # By being explicit about the fields we expect and not
    # just storing the raw JSON, the conversion will fail
    # if there's new unexpected fields, ensuring we take the
    # appropriate actions to update the table schemas.
    advertiser: str
    iab_category: str
    icon: str
    id: int
    keywords: List[str]
    title: str
    url: str
    full_keywords: List[FullKeyword]

    # `click_url` and `impression_url` are both optional, they're currently only
    # used by suggestions from adMarketplace. Mozilla's in-house Wikipedia suggestions
    # do not provide those fields.
    click_url: Optional[str] = None
    impression_url: Optional[str] = None

    # `score` is optional in the schema but should be included in every suggestion.
    score: Optional[float] = None


def download_suggestions(client: kinto_http.Client) -> Iterator[KintoSuggestion]:
    """Get records, download attachments and return the suggestions."""

    # Retrieve the base_url for attachments
    server_info = client.server_info()
    attachments_base_url = server_info["capabilities"]["attachments"]["base_url"]

    # Load records for both "type: data" and "type: offline-expansion-data".
    # See details in: https://mozilla-hub.atlassian.net/browse/CONSVC-1818
    data_records = [
        record
        for record in client.get_records()
        if record["type"] in ["data", "offline-expansion-data"]
    ]

    # Make use of connection pooling because all requests go to the same host
    requests_session = requests.Session()

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
        for suggestion_data in response.json():
            suggestion: Dict[str, Any] = {
                **suggestion_data,
                "full_keywords": [
                    {"keyword": kw, "count": count}
                    for kw, count in suggestion_data.get("full_keywords", [])
                ],
            }
            yield KintoSuggestion(**suggestion)


def store_suggestions(
    today: datetime.date,
    destination_project: str,
    destination_table_id: str,
    kinto_suggestions: Iterator[KintoSuggestion],
):
    """Upload suggestions to BigQuery."""

    today_as_iso = today.isoformat()

    # Turn the suggestions into dicts and augment them with
    # an insertion date.
    suggestions = [
        {**asdict(suggestion), "submission_date": today_as_iso}
        for suggestion in kinto_suggestions
    ]

    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        schema=[
            bigquery.SchemaField("submission_date", "DATE"),
            bigquery.SchemaField("advertiser", "STRING"),
            bigquery.SchemaField("click_url", "STRING"),
            bigquery.SchemaField("iab_category", "STRING"),
            bigquery.SchemaField("icon", "STRING"),
            bigquery.SchemaField("impression_url", "STRING"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("keywords", "STRING", "REPEATED"),
            bigquery.SchemaField(
                "full_keywords",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("keyword", "STRING"),
                    bigquery.SchemaField("count", "INTEGER"),
                ],
            ),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("score", "FLOAT64"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_json(
        suggestions,
        destination_table_id,
        location="US",
        job_config=job_config,
    )

    try:
        # Catch the exception so that we can print the errors
        # in case of failure.
        load_job.result()
    except BadRequest as ex:
        for e in load_job.errors:
            logging.error(f"ERROR: {e['message']}")
        # Re-raise the exception to make the job fail.
        raise ex

    stored_table = client.get_table(destination_table_id)
    logging.info(f"Loaded {stored_table.num_rows} rows.")


@click.command()
@click.option(
    "--destination-project",
    required=True,
    type=str,
    help="the GCP project to use for writing data to",
)
@click.option(
    "--destination-table-id",
    required=True,
    type=str,
    help="the table id to append data to, e.g. `projectid.dataset.table`",
)
@click.option(
    "--kinto-server",
    default="https://firefox.settings.services.mozilla.com",
    type=str,
    help="the Kinto server to fetch the data from",
)
@click.option(
    "--kinto-bucket",
    default="main",
    type=str,
    help="the Kinto bucket to fetch the data from",
)
@click.option(
    "--kinto-collection",
    default="quicksuggest",
    type=str,
    help="the Kinto server to fetch the data from",
)
def main(
    destination_project,
    destination_table_id,
    kinto_server,
    kinto_bucket,
    kinto_collection,
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

    logging.info("Download finished, uploading suggestions to BigQuery")

    store_suggestions(
        datetime.date.today(),
        destination_project,
        destination_table_id,
        kinto_suggestions,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
