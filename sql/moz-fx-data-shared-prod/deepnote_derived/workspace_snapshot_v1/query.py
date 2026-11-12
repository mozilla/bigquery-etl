"""Snapshot projects, notebooks, Streamlit apps and machines of Deepnote workspaces.

Snapshots the workspaces whose API keys are passed with --api-key-env-vars.
"""

import logging
import re
from argparse import ArgumentParser
from datetime import date

import requests
from google.cloud import bigquery
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import Version

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

# Streamlit caches are shared between viewers unless scope="session" (Streamlit 1.54+)
CACHE_DECORATOR = re.compile(
    r"^[ \t]*@(?:st|streamlit)\.cache_(?:data|resource)\b.*$", re.M
)
SESSION_SCOPE = re.compile(r"scope\s*=\s*['\"]session['\"]")
SERVICE_ACCOUNT = re.compile(
    r"service_account|SERVICE_ACCOUNT|GOOGLE_APPLICATION_CREDENTIALS"
)
STREAMLIT_REQUIREMENT = re.compile(
    r"^[ \t]*streamlit(?![\w.-])[ \t]*([^#;\n]*)", re.M | re.I
)
SESSION_CACHE_VERSION = Version("1.54.0")


def read_file(workspace, project_id, path):
    """Return a project file's content, or None if it can't be downloaded."""
    try:
        return workspace.download(project_id, path)
    except requests.HTTPError as e:
        logging.warning(f"Failed to download {path} of project {project_id}: {e}")
        return None


def scan_app(content):
    """Return the cache and credential usage of a Streamlit app's entrypoint."""
    if content is None:
        return {"unscoped_cache_count": None, "uses_service_account": None}
    decorators = CACHE_DECORATOR.findall(content)
    return {
        "unscoped_cache_count": sum(
            1 for d in decorators if not SESSION_SCOPE.search(d)
        ),
        "uses_service_account": bool(SERVICE_ACCOUNT.search(content)),
    }


def excludes_session_cache(spec):
    """Return whether a version specifier only allows Streamlit versions before 1.54."""
    try:
        specifiers = SpecifierSet(spec)
    except InvalidSpecifier:
        return None
    candidates = [Version(f"1.{minor}.0") for minor in range(54, 100)] + [
        Version(s.version)
        for s in specifiers
        if not s.version.endswith("*") and Version(s.version) >= SESSION_CACHE_VERSION
    ]
    return not any(specifiers.contains(v) for v in candidates)


def get_streamlit_requirement(workspace, project_id, files):
    """Return the streamlit requirement of the project's requirements.txt, if any."""
    for path in files:
        if path.rsplit("/", 1)[-1].lower() != "requirements.txt":
            continue
        match = STREAMLIT_REQUIREMENT.search(
            read_file(workspace, project_id, path) or ""
        )
        if match:
            spec = match.group(1).replace(" ", "")
            return {
                "file": path,
                "specifier": spec or None,
                "excludes_session_cache": (
                    excludes_session_cache(spec) if spec else False
                ),
            }
    return None


def list_streamlit_apps(workspace, project_id):
    """Return a project's Streamlit apps."""
    apps = workspace.get("/v2/streamlit-apps", {"projectId": project_id})
    return [
        {
            "streamlit_app_id": app["id"],
            "entrypoint": app.get("entrypoint"),
            "url": app.get("url"),
            "created_at": app.get("createdAt"),
            **scan_app(
                read_file(workspace, project_id, app["entrypoint"])
                if app.get("entrypoint")
                else None
            ),
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


def get_details(workspace, project_id):
    """Return a project's file paths and attached integrations."""
    project = workspace.get(f"/v2/projects/{project_id}")["project"]
    files = [f["path"] for f in project.get("files") or [] if f.get("type") == "file"]
    integrations = [
        {
            "integration_id": integration["id"],
            "name": integration.get("name"),
            "type": integration.get("type"),
        }
        for integration in project.get("integrations") or []
    ]
    return files, integrations


def to_row(workspace, snapshot_date, project):
    """Convert an API project into a table row."""
    folder = project.get("folder") or {}
    files, integrations = get_details(workspace, project["id"])
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
        "files": files,
        "integrations": integrations,
        "streamlit_requirement": get_streamlit_requirement(
            workspace, project["id"], files
        ),
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
