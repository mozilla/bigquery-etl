"""Import the audit logs of Deepnote workspaces from the Deepnote API (GET /v2/audit-logs).

Imports every workspace with a DEEPNOTE_*API_KEY environment variable; the keys
must belong to workspace admins. Only events created within the lookback window are
fetched (use --lookback-days 0 to fetch the full history) and merged into the table
on the event ID, so overlapping runs are idempotent.

The `ip` field of the audit log is never written to BigQuery.
"""

import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from bigquery_etl.deepnote import workspaces

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="audit_log_v1")
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


def list_events(workspace, params):
    """Return a workspace's audit log events matching params, deduplicated on the event ID."""
    events = {}
    page_events = 0
    for event in workspace.paginate("/v2/audit-logs", "auditLogs", params, 1000):
        # the API occasionally returns the same event twice (seen for
        # workspace_member_added)
        events[event["id"]] = event
        page_events += 1
        if page_events == 1000:
            logging.info(
                f"{workspace.name}: fetched {len(events)} events back to "
                f"{event['createdAt']}"
            )
            page_events = 0
    logging.info(f"{workspace.name}: fetched {len(events)} events")
    return events.values()


def main():
    """Fetch recent audit log events of all workspaces and merge them into the table."""
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    params = {"to": now.isoformat(timespec="seconds").replace("+00:00", "Z")}
    if args.lookback_days:
        start = now - timedelta(days=args.lookback_days)
        params["from"] = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    rows = [
        to_row(event, workspace.id, now.isoformat())
        for workspace in workspaces()
        for event in list_events(workspace, params)
    ]

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
