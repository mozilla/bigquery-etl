"""Import the Deepnote audit log from the Deepnote API (GET /v2/audit-logs).

Requires an API key of a workspace admin. Only events created within the lookback
window are fetched (use --lookback-days 0 to fetch the full history) and merged
into the table on the event ID, so overlapping runs are idempotent.

The `ip` field of the audit log is never written to BigQuery.
"""

import logging
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

API_URL = "https://api.deepnote.com"
# Mozilla's default Deepnote workspace
DEFAULT_WORKSPACE_ID = "5730346c-9297-42b0-80b0-35611206a1d7"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="audit_log_v1")
parser.add_argument("--workspace-id", "--workspace_id", default=DEFAULT_WORKSPACE_ID)
parser.add_argument("--lookback-days", "--lookback_days", type=int, default=3)

MERGE_QUERY = """
MERGE
  `{destination}` AS target
USING
  `{staging}` AS source
ON
  target.event_id = source.event_id
WHEN NOT MATCHED
THEN
  INSERT
    ROW
"""


def api_session():
    """Return a session authenticated with the API key that retries rate limited requests."""
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {os.environ['DEEPNOTE_API_KEY']}"
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def api_get(session, path, params=None):
    """GET a Deepnote API path and return the parsed response."""
    response = session.get(f"{API_URL}{path}", params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def list_audit_logs(session, params):
    """Yield all audit log events matching params."""
    page_token = None
    while True:
        page_params = {"pageSize": 1000, **params}
        if page_token:
            page_params["pageToken"] = page_token
        page = api_get(session, "/v2/audit-logs", page_params)
        yield from page["auditLogs"]
        if not page["pagination"].get("hasMore"):
            return
        page_token = page["pagination"]["nextPageToken"]


def to_row(event, workspace_id, loaded_at):
    """Convert an API audit log event into a table row (dropping the IP address)."""
    return {
        "event_id": event["id"],
        "created_at": event["createdAt"],
        "actor_type": event.get("actorType") or None,
        "user_id": event.get("userId") or None,
        "email": event.get("email") or None,
        "workspace_id": workspace_id,
        "resource_id": event.get("resourceId") or None,
        "user_agent": event.get("userAgent") or None,
        "event_type": event["type"],
        "metadata": event.get("metadata"),
        "loaded_at": loaded_at,
    }


def main():
    """Fetch recent audit log events and merge them into the table."""
    args = parser.parse_args()
    session = api_session()

    # API keys are scoped to a workspace; make sure it's the expected one
    workspace_id = api_get(session, "/v2/me")["workspace"]["id"]
    if workspace_id != args.workspace_id:
        raise ValueError(
            f"API key belongs to workspace {workspace_id}, expected {args.workspace_id}"
        )

    now = datetime.now(timezone.utc)
    params = {"to": now.isoformat(timespec="seconds").replace("+00:00", "Z")}
    if args.lookback_days:
        start = now - timedelta(days=args.lookback_days)
        params["from"] = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    # the API occasionally returns the same event twice (seen for
    # workspace_member_added), so deduplicate on the event ID
    events = {event["id"]: event for event in list_audit_logs(session, params)}
    rows = [to_row(event, workspace_id, now.isoformat()) for event in events.values()]

    client = bigquery.Client(args.project)
    destination = f"{args.project}.{args.dataset}.{args.table}"
    staging = f"{destination}__staging"
    client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=client.get_table(destination).schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()
    try:
        job = client.query(MERGE_QUERY.format(destination=destination, staging=staging))
        job.result()
    finally:
        client.delete_table(staging, not_found_ok=True)
    logging.info(
        f"Fetched {len(rows)} audit log events, merged {job.num_dml_affected_rows} "
        f"new ones into {destination}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
