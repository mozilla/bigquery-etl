"""Snapshot the members and guests of the Deepnote workspace (GET /v2/members)."""

import logging
import os
from argparse import ArgumentParser
from datetime import date

import requests
from google.cloud import bigquery

API_URL = "https://api.deepnote.com"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, type=date.fromisoformat)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="members_v1")


def main():
    """Write today's member snapshot into the date partition."""
    args = parser.parse_args()
    response = requests.get(
        f"{API_URL}/v2/members",
        headers={"Authorization": f"Bearer {os.environ['DEEPNOTE_API_KEY']}"},
        timeout=60,
    )
    response.raise_for_status()
    rows = [
        {
            "snapshot_date": args.date.isoformat(),
            "user_id": member["userId"],
            "email": member.get("email"),
            "access_level": member.get("accessLevel"),
        }
        for member in response.json()["members"]
    ]

    client = bigquery.Client(args.project)
    destination = (
        f"{args.project}.{args.dataset}.{args.table}${args.date.strftime('%Y%m%d')}"
    )
    table = client.get_table(f"{args.project}.{args.dataset}.{args.table}")
    job = client.load_table_from_json(
        rows,
        destination,
        job_config=bigquery.LoadJobConfig(
            schema=table.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    logging.info(f"Loaded {len(rows)} members into {destination}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
