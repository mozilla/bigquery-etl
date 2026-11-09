"""Import notebook run history (scheduled and API runs) from the Deepnote API.

Imports the workspaces whose API keys are passed with --api-key-env-vars.

Runs are listed per notebook, newest first. Only runs created within the lookback
window are fetched (use --lookback-days 0 to fetch the full history) and merged
into the table on run_id, so status updates of recent runs are picked up and runs
of deleted notebooks are kept.
"""

import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery

from bigquery_etl.deepnote import workspaces

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="notebook_runs_v1")
parser.add_argument("--lookback-days", "--lookback_days", type=int, default=7)
parser.add_argument(
    "--api-key-env-vars",
    "--api_key_env_vars",
    nargs="+",
    required=True,
    help="Environment variables holding the API keys, one per workspace",
)

MERGE_QUERY = """
MERGE
  `{destination}` AS target
USING
  `{staging}` AS source
ON
  target.run_id = source.run_id
WHEN MATCHED
THEN
  UPDATE SET
    status = source.status,
    completed_at = source.completed_at,
    last_seen_at = source.last_seen_at
WHEN NOT MATCHED
THEN
  INSERT
    ROW
"""


def list_runs(workspace, project_id, notebook_id, cutoff, seen_at):
    """Return a notebook's runs created after cutoff (all runs if cutoff is None)."""
    rows = []
    runs = workspace.paginate(f"/v2/notebooks/{notebook_id}/runs", "runs")
    try:
        run = next(runs, None)
    except requests.HTTPError as e:
        # notebooks that were never run return 404
        if e.response is not None and e.response.status_code == 404:
            return rows
        raise
    for run in [run, *runs] if run else []:
        created_at = datetime.fromisoformat(run["createdAt"].replace("Z", "+00:00"))
        if cutoff and created_at < cutoff:
            break
        rows.append(
            {
                "run_id": run["runId"],
                "workspace_id": workspace.id,
                "project_id": project_id,
                "notebook_id": notebook_id,
                "status": run.get("status"),
                "created_at": run.get("createdAt"),
                "completed_at": run.get("completedAt"),
                "last_seen_at": seen_at,
            }
        )
    return rows


def main():
    """Fetch recent runs of all notebooks of all workspaces and merge them into the table."""
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.lookback_days) if args.lookback_days else None
    rows = [
        run
        for workspace in workspaces(args.api_key_env_vars)
        for project in workspace.paginate("/v2/projects", "projects")
        for notebook in project.get("notebooks", [])
        for run in list_runs(
            workspace, project["id"], notebook["id"], cutoff, now.isoformat()
        )
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
        client.query(
            MERGE_QUERY.format(destination=destination, staging=staging)
        ).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
    logging.info(f"Merged {len(rows)} runs into {destination}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
