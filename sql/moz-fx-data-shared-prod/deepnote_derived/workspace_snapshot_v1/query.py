"""Snapshot projects, notebooks, Streamlit apps and machines of the Deepnote workspace.

The public REST API has no machine endpoint, so machine types are read through the
read-only `get_machine` tool of the Deepnote MCP server, which accepts the same API key
(https://deepnote.com/docs/deepnote-mcp).
"""

import logging
import os
from argparse import ArgumentParser
from datetime import date

import requests
from google.cloud import bigquery

API_URL = "https://api.deepnote.com"
MCP_URL = "https://deepnote.com/mcp"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, type=date.fromisoformat)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="workspace_snapshot_v1")


def api_get(session, path, params=None):
    """GET a Deepnote API path and return the parsed response."""
    response = session.get(f"{API_URL}{path}", params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def list_projects(session):
    """Return all projects (with their notebooks) visible to the API key."""
    projects, page_token = [], None
    while True:
        params = {"pageSize": 100}
        if page_token:
            params["pageToken"] = page_token
        page = api_get(session, "/v2/projects", params)
        projects += page["projects"]
        if not page["pagination"].get("hasMore"):
            return projects
        page_token = page["pagination"]["nextPageToken"]


def list_streamlit_apps(session, project_id):
    """Return a project's Streamlit apps with their current status."""
    apps = api_get(session, "/v2/streamlit-apps", {"projectId": project_id})
    rows = []
    for app in apps["streamlitApps"]:
        status = api_get(session, f"/v2/streamlit-apps/{app['id']}/status")
        rows.append(
            {
                "streamlit_app_id": app["id"],
                "entrypoint": app.get("entrypoint"),
                "url": app.get("url"),
                "created_at": app.get("createdAt"),
                "status": status.get("status"),
            }
        )
    return rows


def get_schedule(session, notebook_id):
    """Return a scheduled notebook's schedule (cron, timezone, paused state)."""
    schedule = api_get(session, f"/v2/notebooks/{notebook_id}/schedule")["schedule"]
    return {
        "cron": schedule.get("cron"),
        "timezone": schedule.get("timezone"),
        "next_run_at": schedule.get("nextRunAt"),
        "is_paused": schedule.get("isPaused"),
        "pause_reason": schedule.get("pauseReason"),
    }


def get_machine(session, project_id):
    """Return the machine of a project's editor/apps via the MCP get_machine tool."""
    response = session.post(
        MCP_URL,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "get_machine", "arguments": {"projectId": project_id}},
        },
        headers={"Accept": "application/json, text/event-stream"},
        timeout=60,
    )
    response.raise_for_status()
    result = response.json().get("result") or {}
    machine = (result.get("structuredContent") or {}).get("machine")
    if result.get("isError") or not machine:
        logging.warning(f"No machine for project {project_id}: {result}")
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


def to_row(session, snapshot_date, project):
    """Convert an API project into a table row."""
    folder = project.get("folder") or {}
    return {
        "snapshot_date": snapshot_date.isoformat(),
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
                    get_schedule(session, notebook["id"])
                    if notebook.get("isScheduled")
                    else None
                ),
            }
            for notebook in project.get("notebooks", [])
        ],
        "streamlit_apps": list_streamlit_apps(session, project["id"]),
        "machine": get_machine(session, project["id"]),
    }


def main():
    """Write today's workspace snapshot into the date partition."""
    args = parser.parse_args()
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {os.environ['DEEPNOTE_API_KEY']}"

    rows = [to_row(session, args.date, p) for p in list_projects(session)]

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
