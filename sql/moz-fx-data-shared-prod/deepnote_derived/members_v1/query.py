"""Snapshot the members and guests of Deepnote workspaces (GET /v2/members).

Snapshots every workspace with a DEEPNOTE_*API_KEY environment variable.
"""

import logging
from argparse import ArgumentParser
from datetime import date

from google.cloud import bigquery

from bigquery_etl.deepnote import workspaces

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, type=date.fromisoformat)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="members_v1")


def main():
    """Write today's member snapshot of all workspaces into the date partition."""
    args = parser.parse_args()
    rows = [
        {
            "snapshot_date": args.date.isoformat(),
            "workspace_id": workspace.id,
            "user_id": member["userId"],
            "email": member.get("email"),
            "access_level": member.get("accessLevel"),
        }
        for workspace in workspaces()
        for member in workspace.get("/v2/members")["members"]
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
