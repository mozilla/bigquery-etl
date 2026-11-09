"""Snapshot the members and guests of Deepnote workspaces (GET /v2/members).

Snapshots the workspaces whose API keys are passed with --api-key-env-vars.
"""

import logging
import os
from argparse import ArgumentParser
from datetime import date

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

API_URL = "https://api.deepnote.com"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, type=date.fromisoformat)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="members_v1")
parser.add_argument(
    "--api-key-env-vars",
    "--api_key_env_vars",
    nargs="+",
    required=True,
    help="Environment variables holding the API keys, one per workspace",
)


class Workspace:
    """Deepnote API session for the workspace an API key belongs to."""

    def __init__(self, api_key):
        """Create a session that retries rate limited requests and look up the workspace."""
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        workspace = self.get("/v2/me")["workspace"]
        self.id = workspace["id"]

    def get(self, path, params=None):
        """GET a Deepnote API path and return the parsed response."""
        response = self.session.get(f"{API_URL}{path}", params=params, timeout=60)
        response.raise_for_status()
        return response.json()


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
        for workspace in [Workspace(os.environ[name]) for name in args.api_key_env_vars]
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
