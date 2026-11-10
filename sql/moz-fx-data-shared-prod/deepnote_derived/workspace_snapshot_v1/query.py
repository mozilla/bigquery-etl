"""Snapshot projects, notebooks, Streamlit apps and machines of Deepnote workspaces.

Snapshots the workspaces whose API keys are passed with --api-key-env-vars.
"""

import logging
from argparse import ArgumentParser
from datetime import date

import requests
from google.cloud import bigquery

from bigquery_etl.deepnote import workspaces

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, type=date.fromisoformat)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="workspace_snapshot_v1")
parser.add_argument(
    "--api-key-env-vars",
    "--api_key_env_vars",
    nargs="+",
    required=True,
    help="Environment variables holding the API keys, one per workspace",
)


def list_streamlit_apps(workspace, project_id):
    """Return a project's Streamlit apps."""
    apps = workspace.get("/v2/streamlit-apps", {"projectId": project_id})
    return [
        {
            "streamlit_app_id": app["id"],
            "entrypoint": app.get("entrypoint"),
            "url": app.get("url"),
            "created_at": app.get("createdAt"),
        }
        for app in apps["streamlitApps"]
    ]


def get_schedule(workspace, notebook_id):
    """Return a scheduled notebook's schedule (cron, timezone, paused state)."""
    schedule = workspace.get(f"/v2/notebooks/{notebook_id}/schedule")["schedule"]
    return {
        "cron": schedule.get("cron"),
        "timezone": schedule.get("timezone"),
        "next_run_at": schedule.get("nextRunAt"),
        "is_paused": schedule.get("isPaused"),
        "pause_reason": schedule.get("pauseReason"),
    }


def get_machine(workspace, project_id):
    """Return the machine of a project's editor/apps."""
    try:
        machine = workspace.get(f"/v2/projects/{project_id}/machine")["machine"]
    except requests.HTTPError as e:
        logging.warning(f"Failed to get machine of project {project_id}: {e}")
        return None
    if not machine:
        return None
    machine_type = machine.get("type") or {}
    return {
        "status": machine.get("status"),
        "type_id": machine_type.get("id"),
        "type_name": machine_type.get("name"),
        "vcpus": machine_type.get("vCpus"),
        "memory_gib": machine_type.get("memoryGiB"),
        "gpu_count": machine_type.get("gpuCount"),
        "idle_shutdown_seconds": machine.get("idleShutdownSeconds"),
    }


def get_files(workspace, project_id):
    """Return a project's file paths."""
    project = workspace.get(f"/v2/projects/{project_id}")["project"]
    return [f["path"] for f in project.get("files") or [] if f.get("type") == "file"]


def to_row(workspace, snapshot_date, project):
    """Convert an API project into a table row."""
    folder = project.get("folder") or {}
    return {
        "snapshot_date": snapshot_date.isoformat(),
        "workspace_id": workspace.id,
        "workspace_name": workspace.name,
        "project_id": project["id"],
        "project_name": project.get("name"),
        "project_type": project.get("projectType"),
        "created_at": project.get("createdAt"),
        "updated_at": project.get("updatedAt"),
        "folder_id": folder.get("id"),
        "folder_path": "/".join(p["name"] for p in folder.get("path", [])) or None,
        "notebooks": [
            {
                "notebook_id": notebook["id"],
                "name": notebook.get("name"),
                "created_at": notebook.get("createdAt"),
                "is_init": notebook.get("isInit"),
                "is_scheduled": notebook.get("isScheduled"),
                "last_run_at": notebook.get("lastRunAt"),
                "schedule": (
                    get_schedule(workspace, notebook["id"])
                    if notebook.get("isScheduled")
                    else None
                ),
            }
            for notebook in project.get("notebooks", [])
        ],
        "streamlit_apps": list_streamlit_apps(workspace, project["id"]),
        "machine": get_machine(workspace, project["id"]),
        "files": get_files(workspace, project["id"]),
    }


def main():
    """Write today's snapshot of all workspaces into the date partition."""
    args = parser.parse_args()

    rows = [
        to_row(workspace, args.date, project)
        for workspace in workspaces(args.api_key_env_vars)
        for project in workspace.paginate("/v2/projects", "projects")
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
    logging.info(f"Loaded {len(rows)} projects into {destination}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
