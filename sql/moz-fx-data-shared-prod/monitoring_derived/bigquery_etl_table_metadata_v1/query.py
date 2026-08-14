#!/usr/bin/env python3

"""Snapshot the metadata bigquery-etl declares for every table and view.

One row per ``sql/<project>/<dataset>/<table>/metadata.yaml``, carrying
ownership, the scheduling DAG and its impact tier, the authored labels, and
who last changed the entity. Built from three sources:

1. The repo checkout inside the Docker image (the Dockerfile does
   ``COPY . .``): metadata.yaml for owners, labels and deprecation, and
   dags.yaml for the DAG's ``impact/tier_*`` tag, owner, alert emails and
   schedule. These reflect main at image build time, not the live Airflow
   metadata DB -- for runtime DAG state see ``monitoring_derived.airflow_dag_*``.
2. INFORMATION_SCHEMA.TABLES at run time, for ``table_type``. Only BigQuery
   knows what a deployed object actually is: ``definition_type`` is the repo's
   view of it and the two legitimately disagree, since some query.sql
   directories deploy as VIEW or CLONE and directories holding no SQL at all
   are still real tables.
3. ``git log`` over the same checkout, for the ``last_changed_*`` columns,
   which link an entity to the merged PR that last touched it.

Both external lookups can fail without the rest of the snapshot being wrong,
so each carries a status column -- ``table_type_status`` and
``last_changed_status`` -- that separates "we looked and it is not there" from
"we could not look". A null with no status is never a silent failure.

UDFs and stored procedures are excluded: they are routines rather than
tables, are never scheduled, and conventionally declare no owners, so they
would distort ownership-coverage denominators.

Every metadata.yaml produces a row even if it fails to parse: the row is
emitted with ``metadata_parse_error`` set, ``owner_count`` 0 and the other
metadata-derived columns NULL. Dropping such rows would make a broken file
indistinguishable from an entity that does not exist.
"""

import argparse
import datetime
import logging
import re
import subprocess
from pathlib import Path
from typing import Any, Optional

import yaml
from google.cloud import bigquery

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.schema import Schema

logger = logging.getLogger(__name__)

# Repo root, resolved from this file's location so the script works both in the
# Docker image (/app/sql/...) and from a local checkout.
REPO_ROOT = Path(__file__).resolve().parents[4]

# In production bqetl deploys the destination table from this schema.yaml before
# the job runs, and a load job whose schema disagrees with the deployed table
# fails outright. Read the same file rather than restating the fields here, so
# the two cannot drift.
SCHEMA_FILE = Path(__file__).resolve().parent / "schema.yaml"

IMPACT_TAG_PREFIX = "impact/"

# Mirrors the alias table in bigquery_etl/query_scheduling/utils.py, which is a
# local inside schedule_interval_delta and so cannot be imported. `once` is
# deliberately absent: that function maps it to "* * * * *" to make delta
# arithmetic work, which would render here as "every minute" -- the opposite of
# what it means.
SCHEDULE_ALIASES = {
    "yearly": "0 0 1 1 *",
    "monthly": "0 0 1 * *",
    "weekly": "0 0 * * 0",
    "daily": "0 0 * * *",
    "hourly": "0 * * * *",
}

TIMEDELTA_RE = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")

# A squash-merged PR leaves "subject (#1234)"; an explicit merge commit leaves
# "Merge pull request #1234 from ...". Either form on the default branch means
# the change arrived via a merged PR. A subject matching neither came from a
# direct push, and gets null PR columns rather than a guessed number.
PR_SQUASH_RE = re.compile(r"^(?P<title>.*?)\s*\(#(?P<pr>\d+)\)$")
PR_MERGE_RE = re.compile(r"^Merge pull request #(?P<pr>\d+) from \S+\s*(?P<title>.*)$")

# Fallback when the git remote cannot be read.
DEFAULT_GITHUB_SLUG = "mozilla/bigquery-etl"

# Cron day-of-week accepts both 0 and 7 for Sunday.
DAY_NAMES = {
    0: "Sunday",
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}
MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

# The file that defines an entity, in priority order. No directory in the repo
# holds more than one of these, so the order is not load-bearing; it only
# decides the answer if that ever changes. The filename is used verbatim as
# definition_type -- it is unambiguous and needs no separate vocabulary.
DEFINITION_FILES = (
    "query.sql",
    "query.py",
    "script.sql",
    "materialized_view.sql",
    "view.sql",
    "udf.sql",
    "stored_procedure.sql",
)

# Routines are dropped entirely rather than flagged: they are not tables, are
# never scheduled, and mozfun UDF metadata.yaml files conventionally declare no
# owners, so including them buries the genuinely unowned tables and views.
EXCLUDED_DEFINITION_FILES = {"udf.sql", "stored_procedure.sql"}

# Resolves table_type. definition_type says how the repo defines an entity;
# only BigQuery knows what the deployed object actually is, and the two
# disagree for ~1% of query.sql directories (VIEW or CLONE) as well as for
# every directory holding no SQL at all.
TABLE_TYPE_QUERY = """
SELECT table_schema, table_name, table_type
FROM `{project}.region-US.INFORMATION_SCHEMA.TABLES`
WHERE table_schema IN UNNEST(@datasets)
"""


def github_slug() -> str:
    """Return the ``owner/repo`` of the git remote, e.g. mozilla/bigquery-etl.

    PR numbers are per-repository, so the link has to point at whichever repo
    this checkout's history came from. That is not necessarily ``dag_repo``: in
    production the job runs from the private-bigquery-etl image, whose commits
    and PR numbers are its own even for entities published from the public repo.
    """
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return DEFAULT_GITHUB_SLUG
    if result.returncode != 0:
        logger.warning("No git remote 'origin'; PR links assume the public repo.")
        return DEFAULT_GITHUB_SLUG
    url = result.stdout.strip().removesuffix(".git")
    match = re.search(r"github\.com[:/](?P<slug>[^/]+/[^/]+)$", url)
    if not match:
        logger.warning(f"Could not parse a GitHub slug from {url!r}.")
        return DEFAULT_GITHUB_SLUG
    return match.group("slug")


def parse_pr(subject: str) -> tuple[Optional[int], str]:
    """Split a commit subject into (PR number, title).

    Returns ``(None, subject)`` when the subject shows no sign of a merged PR,
    so direct pushes are reported with a title but no PR link.
    """
    for pattern in (PR_SQUASH_RE, PR_MERGE_RE):
        match = pattern.match(subject)
        if match:
            title = match.group("title").strip()
            return int(match.group("pr")), title or subject
    return None, subject


def git_last_changed(
    sql_dir: Path,
) -> tuple[dict[str, dict[str, Any]], str]:
    """Return entity-directory -> last-commit details, plus a status.

    One ``git log`` pass over ``sql_dir`` newest-first: the first time a path
    appears is by definition its most recent change, so the whole tree resolves
    in a single subprocess (~0.3s for the full history).

    Tracks the entity *directory*, not just the definition file, because a
    schema.yaml or metadata.yaml edit can break a task just as easily as a
    query.sql edit; ``last_changed_file`` records which one it was.

    Uses the committer date rather than the author date: when correlating with a
    task failure what matters is when the change landed on the branch, not when
    someone first wrote it.

    The status is ``ok`` when history is usable, otherwise a reason. A shallow
    clone is treated as unusable rather than partially trusted -- it would map
    every entity to the build commit and read as "everything changed today".
    """
    try:
        shallow = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        logger.warning("git is not installed; last_changed_* will be null.")
        return {}, "git_unavailable"
    if shallow.returncode != 0:
        logger.warning(
            f"Not a git repository at {REPO_ROOT}; last_changed_* will be null."
        )
        return {}, "git_unavailable"
    if shallow.stdout.strip() == "true":
        logger.warning(
            "Git history is shallow, so per-entity change dates would all point "
            "at the build commit; last_changed_* will be null."
        )
        return {}, "shallow_history"

    result = subprocess.run(
        [
            "git",
            "log",
            "--format=%x01%H%x1f%cI%x1f%an%x1f%s",
            "--name-only",
            "--",
            str(sql_dir),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning(f"git log failed: {result.stderr.strip()[:200]}")
        return {}, "git_unavailable"

    slug = github_slug()
    last: dict[str, dict[str, Any]] = {}
    commit: Optional[dict[str, Any]] = None
    for line in result.stdout.splitlines():
        if line.startswith("\x01"):
            _sha, when, author, subject = line[1:].split("\x1f", 3)
            pr, title = parse_pr(subject)
            commit = {
                "last_changed_at": when,
                "last_changed_by": author,
                "last_changed_pr": pr,
                "last_changed_pr_url": (
                    f"https://github.com/{slug}/pull/{pr}" if pr else None
                ),
                "last_changed_title": title,
            }
        elif line and commit is not None:
            parts = line.split("/")
            # sql/<project>/<dataset>/<entity>/<file>
            if len(parts) < 5:
                continue
            key = "/".join(parts[:4])
            if key not in last:
                last[key] = {**commit, "last_changed_file": line}

    logger.info(f"Resolved git history for {len(last)} entity directories")
    return last, "ok"


def _plural(n: int, unit: str) -> str:
    """Render "hour" / "3 hours", dropping the redundant "1"."""
    return unit if n == 1 else f"{n} {unit}s"


def _hm(minute: str, hour: str) -> str:
    """Render a zero-padded HH:MM clock time from two cron fields."""
    return f"{int(hour):02d}:{int(minute):02d}"


def _at(minute: str, hour: str) -> str:
    """Render a single HH:MM UTC clock time."""
    return f"{_hm(minute, hour)} UTC"


def _names(field: str, lookup: dict[int, str]) -> str:
    """Render a comma-separated cron field via a name lookup, e.g. days."""
    return ", ".join(lookup[int(v)] for v in field.split(","))


def describe_schedule(interval: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Render a schedule_interval as (frequency bucket, human description).

    Handles the grammar accepted by ``is_schedule_interval`` in
    bigquery_etl/query_scheduling/utils.py: the named aliases, a 5-field cron
    with lists/steps/ranges, or a ``NhNmNs`` timedelta. Anything outside that --
    including the 6- and 7-field crons the regex permits but no DAG currently
    uses -- falls back to (``other``, the raw string) rather than guessing.

    All times are UTC, which is what Airflow schedules on.
    """
    if not interval:
        return None, None
    interval = interval.strip()

    if interval == "once":
        return "once", "Runs once; no recurring schedule"

    cron = SCHEDULE_ALIASES.get(interval, interval)

    delta = TIMEDELTA_RE.fullmatch(cron)
    if delta and any(delta.groups()):
        hours, minutes, seconds = (int(g) if g else 0 for g in delta.groups())
        total_h = hours + minutes / 60 + seconds / 3600
        parts = [
            _plural(v, u)
            for v, u in ((hours, "hour"), (minutes, "minute"), (seconds, "second"))
            if v
        ]
        freq = (
            "hourly" if total_h == 1 else ("sub_hourly" if total_h < 1 else "intraday")
        )
        return freq, "Every " + " ".join(parts)

    fields = cron.split()
    if len(fields) != 5:
        return "other", interval

    # Every field may legally hold a range (`1-5`) or an out-of-range number,
    # neither of which the renderers below can turn into a name or a clock time.
    # Catching that here keeps one odd DAG config from failing the whole
    # snapshot, and makes the "falls back rather than guessing" promise true.
    try:
        return _describe_cron(*fields, interval=interval)
    except (ValueError, KeyError):
        logger.warning(
            f"Could not interpret schedule_interval {interval!r}; "
            f"reporting frequency as 'other'."
        )
        return "other", interval


def _describe_cron(
    minute: str, hour: str, dom: str, month: str, dow: str, interval: str
) -> tuple[str, str]:
    """Render a 5-field cron. May raise ValueError/KeyError on exotic fields."""
    # Sub-daily: the hour field is unconstrained or steps within the day.
    if hour == "*":
        if minute.startswith("*/"):
            return "sub_hourly", f"Every {_plural(int(minute[2:]), 'minute')}"
        if "," in minute:
            mins = ", ".join(f":{int(m):02d}" for m in minute.split(","))
            return "sub_hourly", f"Hourly at {mins}"
        if minute.isdigit():
            return "hourly", f"Hourly at :{int(minute):02d}"
        return "other", interval
    if hour.startswith("*/"):
        return (
            "intraday",
            f"Every {_plural(int(hour[2:]), 'hour')} at :{int(minute):02d}",
        )
    if "/" in hour:
        start, step = hour.split("/", 1)
        return "intraday", (
            f"Every {_plural(int(step), 'hour')} from {_at(minute, start)}"
        )
    if "," in hour:
        times = ", ".join(_hm(minute, h) for h in hour.split(","))
        return "intraday", f"Daily at {times} UTC"
    if not hour.isdigit() or not minute.isdigit():
        return "other", interval

    when = _at(minute, hour)
    if dow != "*":
        return "weekly", f"Weekly on {_names(dow, DAY_NAMES)} at {when}"
    if dom != "*" and month != "*":
        return "yearly", f"Yearly on {_names(month, MONTH_NAMES)} {dom} at {when}"
    if dom != "*":
        return "monthly", f"Monthly on day {dom} at {when}"
    return "daily", f"Daily at {when}"


def dag_tier(tags: list[str]) -> Optional[str]:
    """Return the tier suffix of the DAG's ``impact/*`` tag, e.g. ``tier_1``.

    All 155 DAGs in dags.yaml currently carry exactly one impact tag, so the
    sort is defensive: if a DAG is ever tagged with several, the lowest (most
    critical) wins rather than an arbitrary one. Returns None for a DAG with no
    impact tag.
    """
    tiers = sorted(
        tag[len(IMPACT_TAG_PREFIX) :]
        for tag in tags
        if tag.startswith(IMPACT_TAG_PREFIX)
    )
    return tiers[0] if tiers else None


def repo_relative_path(path: Path) -> str:
    """Return ``path`` relative to the repo root, or absolute if it is outside.

    A --sql-dir pointing somewhere other than the repo checkout (used for
    testing) is not under REPO_ROOT, and relative_to would raise rather than
    degrade.
    """
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def source_url(path: Optional[str], repo: Optional[str]) -> Optional[str]:
    """Return a GitHub link to ``path`` on the published branch.

    Mirrors ``Task.source_url`` in bigquery_etl/query_scheduling/task.py so the
    links here match the ones bqetl puts in generated DAG docs. ``repo`` comes
    from the scheduling DAG; unscheduled entities have none, and default to the
    public repo, which is correct for everything in the public checkout.
    """
    if path is None:
        return None
    repo = repo or "bigquery-etl"
    branch = (
        "private-generated-sql" if repo == "private-bigquery-etl" else "generated-sql"
    )
    return f"https://github.com/mozilla/{repo}/blob/{branch}/{path}"


def definition_type(table_dir: Path) -> Optional[str]:
    """Return the filename that defines the entity, e.g. ``query.sql``.

    Returns None when the directory contains no recognised definition file. That
    is mostly tables with only metadata.yaml + schema.yaml (defined in BigQuery
    but ingested by an external system), plus a couple of directories whose SQL
    is generated by a helper script rather than named for what it defines.
    """
    for filename in DEFINITION_FILES:
        if (table_dir / filename).exists():
            return filename
    return None


def bigquery_table_types(
    client: bigquery.Client, keys: set[tuple[str, str]]
) -> tuple[dict[tuple[str, str, str], str], set[str]]:
    """Look up INFORMATION_SCHEMA.TABLES.table_type for the given projects.

    ``keys`` is the set of (project, dataset) pairs to resolve. Returns the
    (project, dataset, table) -> table_type mapping plus the set of projects
    that could not be queried at all, so callers can tell "this object is not in
    BigQuery" apart from "we were unable to look".
    """
    datasets_by_project: dict[str, list[str]] = {}
    for project, dataset in keys:
        datasets_by_project.setdefault(project, []).append(dataset)

    table_types: dict[tuple[str, str, str], str] = {}
    unavailable: set[str] = set()
    for project, datasets in sorted(datasets_by_project.items()):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("datasets", "STRING", sorted(datasets))
            ]
        )
        try:
            rows = client.query(
                TABLE_TYPE_QUERY.format(project=project), job_config=job_config
            ).result()
        except Exception as e:
            logger.warning(f"Could not read INFORMATION_SCHEMA for {project}: {e}")
            unavailable.add(project)
            continue
        for row in rows:
            table_types[(project, row.table_schema, row.table_name)] = row.table_type

    return table_types, unavailable


def authored_labels(metadata_file: Path) -> dict[str, Any]:
    """Return the ``labels`` block exactly as written in metadata.yaml.

    Read from the raw YAML rather than the parsed Metadata because
    ``Metadata.from_file`` rewrites the block before returning it: it overwrites
    ``owner{n}`` with the local parts of the ``owners`` emails and injects a
    ``dag`` key from ``scheduling.dag_name``. The labels actually applied to the
    deployed BigQuery table are already available in
    ``monitoring_derived.bigquery_tables_inventory_v1``; what is not recorded
    anywhere else is what a human wrote here, including values the tooling
    silently discards.
    """
    try:
        with open(metadata_file, "r") as f:
            raw = yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"Could not read raw labels from {metadata_file}: {e}")
        return {}
    labels = (raw or {}).get("labels") or {}
    if not isinstance(labels, dict):
        logger.warning(f"labels in {metadata_file} is not a mapping; ignoring.")
        return {}
    return {str(k): v for k, v in labels.items()}


def label_value(value: Any) -> str:
    """Render a label value as a string for the ``labels`` map.

    Label values are not all strings: booleans appear as YAML ``true``/``false``
    and review_bugs is a list. Lowercasing booleans keeps them consistent with
    how they are written in the file rather than rendering Python's "True".
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    return str(value)


def label_bool(labels: dict[str, Any], key: str) -> Optional[bool]:
    """Read a label as a boolean, tolerating both YAML bools and strings."""
    if key not in labels:
        return None
    value = labels[key]
    if isinstance(value, bool):
        return value
    if str(value).strip().lower() in ("true", "false"):
        return str(value).strip().lower() == "true"
    return None


def label_string(labels: dict[str, Any], key: str) -> Optional[str]:
    """Read a label as a string, or None when absent."""
    return label_value(labels[key]) if key in labels else None


def dag_columns(dag) -> dict[str, Any]:
    """Return the DAG-derived columns for a resolved Dag, or empty defaults."""
    if dag is None:
        return {
            "dag_tier": None,
            "dag_tags": [],
            "dag_owner": None,
            "dag_emails": [],
            "dag_schedule_interval": None,
            "dag_schedule_frequency": None,
            "dag_schedule_description": None,
            "dag_repo": None,
        }

    interval = str(dag.schedule_interval)
    frequency, description = describe_schedule(interval)
    return {
        "dag_tier": dag_tier(dag.tags),
        "dag_tags": list(dag.tags),
        "dag_owner": dag.default_args.owner,
        "dag_emails": list(dag.default_args.email or []),
        "dag_schedule_interval": interval,
        "dag_schedule_frequency": frequency,
        "dag_schedule_description": description,
        "dag_repo": dag.repo,
    }


def build_row(
    metadata_file: Path,
    dags: DagCollection,
    submission_date: str,
) -> dict[str, Any]:
    """Build one destination row from a table's metadata.yaml."""
    table_dir = metadata_file.parent
    project_id = table_dir.parents[1].name
    dataset_id = table_dir.parent.name
    table_id = table_dir.name

    definition_file = definition_type(table_dir)
    definition_path = (
        repo_relative_path(table_dir / definition_file) if definition_file else None
    )

    # Read straight from the YAML, so labels survive even when Metadata
    # validation rejects the file for an unrelated reason such as a malformed
    # owner. Only a YAML syntax error loses them.
    labels = authored_labels(metadata_file)

    row: dict[str, Any] = {
        "submission_date": submission_date,
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "full_table_id": f"{project_id}.{dataset_id}.{table_id}",
        "definition_type": definition_file,
        "definition_url": source_url(definition_path, None),
        "labels": [
            {"key": k, "value": label_value(v)} for k, v in sorted(labels.items())
        ],
        "level": label_string(labels, "level"),
        "application": label_string(labels, "application"),
        "schedule": label_string(labels, "schedule"),
        "incremental": label_bool(labels, "incremental"),
        "metadata_path": repo_relative_path(metadata_file),
        "metadata_parse_error": None,
    }

    try:
        metadata = Metadata.from_file(metadata_file)
    except Exception as e:
        logger.warning(f"Failed to parse {row['metadata_path']}: {e}")
        row["metadata_parse_error"] = f"{type(e).__name__}: {e}"
        row["owners"] = []
        row["owner_count"] = 0
        row["dag_tags"] = []
        row["dag_emails"] = []
        return row

    owners = list(metadata.owners or [])
    scheduling = metadata.scheduling or {}
    dag_name = scheduling.get("dag_name")
    dag_cols = dag_columns(dags.dag_by_name(dag_name) if dag_name else None)

    row.update(
        {
            "friendly_name": metadata.friendly_name,
            "owners": owners,
            "owner_count": len(owners),
            "deprecated": bool(metadata.deprecated),
            "dag_name": dag_name,
            "is_scheduled": dag_name is not None,
            # Now that the DAG is resolved, redo the link with its repo so
            # private queries point at private-bigquery-etl.
            "definition_url": source_url(definition_path, dag_cols["dag_repo"]),
            **dag_cols,
        }
    )
    return row


def collect_rows(
    client: bigquery.Client, sql_dir: Path, dags_config: Path, submission_date: str
) -> list[dict[str, Any]]:
    """Build a row for every table-level metadata.yaml under ``sql_dir``."""
    dags = DagCollection.from_file(dags_config)
    logger.info(f"Loaded {len(dags.dags)} DAGs from {dags_config}")

    # Depth-4 glob deliberately excludes dataset_metadata.yaml (different
    # filename) and any metadata.yaml nested deeper than a table directory.
    metadata_files = sorted(sql_dir.glob("*/*/*/metadata.yaml"))
    logger.info(f"Found {len(metadata_files)} table metadata files under {sql_dir}")

    rows = [build_row(f, dags, submission_date) for f in metadata_files]

    excluded = [r for r in rows if r["definition_type"] in EXCLUDED_DEFINITION_FILES]
    rows = [r for r in rows if r["definition_type"] not in EXCLUDED_DEFINITION_FILES]
    logger.info(
        f"Excluded {len(excluded)} routines "
        f"({'/'.join(sorted(EXCLUDED_DEFINITION_FILES))}); {len(rows)} rows remain."
    )

    table_types, unavailable = bigquery_table_types(
        client, {(r["project_id"], r["dataset_id"]) for r in rows}
    )
    for row in rows:
        key = (row["project_id"], row["dataset_id"], row["table_id"])
        if row["project_id"] in unavailable:
            row["table_type"] = None
            row["table_type_status"] = "project_unavailable"
        elif key in table_types:
            row["table_type"] = table_types[key]
            row["table_type_status"] = "found"
        else:
            row["table_type"] = None
            row["table_type_status"] = "not_in_bigquery"

    changes, change_status = git_last_changed(sql_dir)
    CHANGE_COLS = (
        "last_changed_at",
        "last_changed_by",
        "last_changed_pr",
        "last_changed_pr_url",
        "last_changed_title",
        "last_changed_file",
    )
    for row in rows:
        entity_dir = str(Path(row["metadata_path"]).parent)
        change = changes.get(entity_dir)
        if change_status != "ok":
            row.update({c: None for c in CHANGE_COLS})
            row["last_changed_status"] = change_status
        elif change:
            row.update({c: change[c] for c in CHANGE_COLS})
            row["last_changed_status"] = "found"
        else:
            row.update({c: None for c in CHANGE_COLS})
            row["last_changed_status"] = "not_in_git"

    if change_status == "ok":
        dated = sum(1 for r in rows if r["last_changed_status"] == "found")
        with_pr = sum(1 for r in rows if r["last_changed_pr"])
        logger.info(
            f"Resolved last change for {dated}/{len(rows)} rows from git; "
            f"{with_pr} came from a merged PR"
        )

    resolved = sum(1 for r in rows if r["table_type_status"] == "found")
    logger.info(
        f"Resolved table_type for {resolved}/{len(rows)} rows from "
        f"INFORMATION_SCHEMA; {len(unavailable)} project(s) unavailable"
        + (f": {', '.join(sorted(unavailable))}" if unavailable else "")
    )

    # Stamped once for the whole snapshot rather than per row, so run_timestamp
    # identifies the run and never varies within a partition.
    run_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for row in rows:
        row["run_timestamp"] = run_timestamp

    return rows


def save_rows(
    client: bigquery.Client, rows: list[dict[str, Any]], date: str, destination: str
) -> None:
    """Overwrite the run's submission_date partition with the snapshot."""
    job_config = bigquery.LoadJobConfig(
        schema=Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="submission_date"
        ),
        clustering_fields=["dataset_id", "dag_name"],
    )
    partition = date.replace("-", "")
    client.load_table_from_json(
        rows, f"{destination}${partition}", job_config=job_config
    ).result()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", required=True, help="Run date (YYYY-MM-DD); the partition key."
    )
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination-dataset", default="monitoring_derived")
    parser.add_argument("--destination-table", default="bigquery_etl_table_metadata_v1")
    parser.add_argument("--sql-dir", type=Path, default=REPO_ROOT / "sql")
    parser.add_argument("--dags-config", type=Path, default=REPO_ROOT / "dags.yaml")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the rows and log a summary without writing to BigQuery.",
    )
    args = parser.parse_args()
    try:
        datetime.date.fromisoformat(args.date)
    except ValueError:
        parser.error(f"--date must be ISO (YYYY-MM-DD); got {args.date!r}")
    return args


def main() -> None:
    """Snapshot repo-declared table ownership and DAG tier into BigQuery."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    # Needed even for --dry-run: table_type is resolved by reading
    # INFORMATION_SCHEMA, so a dry run that skipped it would not exercise the
    # part most likely to fail on permissions.
    client = bigquery.Client(args.project)
    rows = collect_rows(client, args.sql_dir, args.dags_config, args.date)

    # An empty result means the repo checkout is missing from the image or the
    # glob broke -- writing it would blow away the partition with zero rows and
    # read as "no tables are owned".
    if not rows:
        raise RuntimeError(
            f"No metadata.yaml files found under {args.sql_dir}. Refusing to "
            f"overwrite the {args.date} partition with an empty snapshot."
        )

    errors = sum(1 for r in rows if r["metadata_parse_error"])
    tier_1 = sum(1 for r in rows if r.get("dag_tier") == "tier_1")
    unowned = sum(1 for r in rows if r.get("owner_count") == 0)
    logger.info(
        f"{len(rows)} tables and views; {tier_1} on tier-1 DAGs; "
        f"{unowned} without an owner; {errors} metadata parse errors."
    )

    # A resolved DAG always carries at least a `repo/*` tag (Dag.from_dict adds
    # one), so empty dag_tags on a scheduled table means its dag_name was absent
    # from dags.yaml. A handful is normal -- a table can reference a DAG defined
    # in another repo. A large fraction means we loaded the wrong dags.yaml (in
    # production this job runs in the private-bigquery-etl image, whose
    # dags.yaml is not guaranteed to contain the public DAG definitions), which
    # would otherwise write a full snapshot with every dag_tier silently NULL.
    scheduled = [r for r in rows if r.get("is_scheduled")]
    unresolved = [r for r in scheduled if not r.get("dag_tags")]
    if unresolved:
        sample = ", ".join(sorted({r["dag_name"] for r in unresolved})[:5])
        logger.warning(
            f"{len(unresolved)}/{len(scheduled)} scheduled tables reference a "
            f"DAG missing from {args.dags_config}; e.g. {sample}"
        )
    if scheduled and len(unresolved) > 0.1 * len(scheduled):
        raise RuntimeError(
            f"{len(unresolved)} of {len(scheduled)} scheduled tables could not "
            f"resolve their DAG in {args.dags_config}. Refusing to write a "
            f"snapshot with mostly-NULL tier data."
        )

    destination = f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    if args.dry_run:
        logger.info(f"Dry run: would write {len(rows)} rows to {destination}")
        return

    save_rows(client, rows, args.date, destination)
    logger.info(f"Wrote {len(rows)} rows to {destination}${args.date.replace('-', '')}")


if __name__ == "__main__":
    main()
