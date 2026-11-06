#!/usr/bin/env python3

"""Load hourly values of selected Cloud Monitoring metrics into BigQuery.

Metric definitions are kept free of query-language details so the fetch can be
switched from the Cloud Monitoring PromQL endpoint to the Monitoring SDK
without changing the definitions or the output table.

Values are the increase of the metric over each hour. DELTA metrics use
PromQL's increase(). Dataflow user counters are GAUGE metrics holding a running
total, which occasionally reads double for a minute. increase() takes the drop
back to the real total as a counter reset and adds the whole running total to
that hour, so for these the value is the last sample in the hour minus the
last sample before it.

Cloud Monitoring keeps roughly 6 weeks to 3 months of data depending on the
metric, so older partitions cannot be rebuilt.

PromQL was chosen over the monitoring SDK because the code is simpler.
"""

import datetime
import json
import time
from dataclasses import dataclass, field, replace
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import click
import google.auth
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bigquery_etl.schema import SCHEMA_FILE, Schema

# Queries go through the metrics scope of this project, which covers all the
# projects below. The Airflow service account has monitoring.viewer on it, but
# not on the other projects. See
# cloudops-infra/projects/data-shared/tf/prod/project/monitoring.tf.
SCOPING_PROJECT = "moz-fx-data-shared-prod"

# Every project listed here must be in the scoping project's metrics scope.
BEAM_PROJECTS = (
    "moz-fx-data-beam-nonprod-b5f4",
    "moz-fx-data-beam-prod-11f7",
)
PUBSUB_PROJECTS = BEAM_PROJECTS + (
    "moz-fx-data-shared-prod",
    "moz-fx-ingestion-sink-nonprod",
    "moz-fx-ingestion-sink-prod",
    "moz-fx-data-shar-nonprod-efed",
)
# The edge load balancers live in shared GCPv2 projects. Airflow can't read
# monitoring in the nonprod one (moz-fx-dataservices-high-nonpr).
LOAD_BALANCER_PROJECTS = ("moz-fx-dataservices-high-prod",)


@dataclass(frozen=True)
class MetricDefinition:
    """A Cloud Monitoring metric to aggregate by hour.

    name is written to the metric column. projects lists the projects to query.
    group_by lists the resource or metric labels to keep (project_id is always
    kept). label_filters maps label names to RE2 regular expressions that must
    fully match. running_total marks GAUGE metrics that hold a running total
    instead of a per-interval value.
    """

    name: str
    metric_type: str
    projects: Tuple[str, ...]
    group_by: Tuple[str, ...]
    label_filters: Dict[str, str] = field(default_factory=dict)
    running_total: bool = False


METRICS = (
    MetricDefinition(
        name="pubsub_topic_byte_cost",
        metric_type="pubsub.googleapis.com/topic/byte_cost",
        projects=PUBSUB_PROJECTS,
        group_by=("topic_id", "operation_type"),
    ),
    MetricDefinition(
        name="pubsub_subscription_byte_cost",
        metric_type="pubsub.googleapis.com/subscription/byte_cost",
        projects=PUBSUB_PROJECTS,
        group_by=("subscription_id", "operation_type"),
    ),
    MetricDefinition(
        name="dataflow_predecompress_submission_bytes",
        metric_type="dataflow.googleapis.com/job/user_counter",
        projects=BEAM_PROJECTS,
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/predecompress_submission_bytes"},
        running_total=True,
    ),
    MetricDefinition(
        name="dataflow_valid_submission_bytes",
        metric_type="dataflow.googleapis.com/job/user_counter",
        projects=BEAM_PROJECTS,
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/valid_submission_bytes"},
        running_total=True,
    ),
    MetricDefinition(
        name="dataflow_valid_submission",
        metric_type="dataflow.googleapis.com/job/user_counter",
        projects=BEAM_PROJECTS,
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/valid_submission"},
        running_total=True,
    ),
    # Other services share the edge load balancer projects, so only keep the
    # ingestion edge forwarding rules.
    MetricDefinition(
        name="edge_load_balancer_request_bytes",
        metric_type="loadbalancing.googleapis.com/https/request_bytes_count",
        projects=LOAD_BALANCER_PROJECTS,
        group_by=("forwarding_rule_name", "response_code_class"),
        label_filters={"forwarding_rule_name": ".*ingestion-edge.*"},
    ),
    MetricDefinition(
        name="edge_load_balancer_response_bytes",
        metric_type="loadbalancing.googleapis.com/https/response_bytes_count",
        projects=LOAD_BALANCER_PROJECTS,
        group_by=("forwarding_rule_name", "response_code_class"),
        label_filters={"forwarding_rule_name": ".*ingestion-edge.*"},
    ),
)

SCHEMA = Schema.from_schema_file(
    Path(__file__).parent / SCHEMA_FILE
).to_bigquery_schema()

STEP = datetime.timedelta(hours=1)
WARNING_ATTEMPTS = 3
# Number of metric and project queries to run at once.
PARALLELISM = 8
# Pub/Sub and Dataflow points are ingested up to 7 minutes after they're sampled.
INGEST_DELAY = datetime.timedelta(minutes=10)


def _promql_string(value: str) -> str:
    # JSON string escaping is valid PromQL double-quoted string syntax.
    return json.dumps(value)


def build_promql(metric: MetricDefinition, project: str) -> str:
    """Build a PromQL query for the hourly increase of a metric in one project."""
    matchers = [
        f"__name__={_promql_string(metric.metric_type)}",
        f"project_id={_promql_string(project)}",
    ] + [
        f"{label}=~{_promql_string(regex)}"
        for label, regex in sorted(metric.label_filters.items())
    ]
    selector = "{" + ", ".join(matchers) + "}"
    group_by = ", ".join(("project_id",) + metric.group_by)
    # With the window equal to the step, adjacent hours don't share samples.
    window = f"{int(STEP.total_seconds())}s"
    if metric.running_total:
        # Series are per job (job_id is a label), so a series with no sample in
        # the previous 6 hours belongs to a job that started within the hour,
        # and its whole total is that hour's increase.
        last = f"last_over_time({selector}[{window}])"
        previous = f"last_over_time({selector}[6h] offset {window})"
        return f"sum by ({group_by}) (({last} - {previous}) or {last})"
    # Hourly Pub/Sub values matched the API's ALIGN_DELTA sums exactly.
    return f"sum by ({group_by}) (increase({selector}[{window}]))"


def create_session(quota_project: Optional[str]) -> AuthorizedSession:
    """Create an authorized HTTP session that retries transient errors."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/monitoring.read"],
        quota_project_id=quota_project,
    )
    # Credential refresh isn't locked, so get a token now rather than letting
    # every worker thread refresh at once on its first request. A run is much
    # shorter than the token lifetime.
    credentials.refresh(Request())
    session = AuthorizedSession(credentials)
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=PARALLELISM))
    return session


def query_range(
    session: AuthorizedSession,
    project: str,
    query: str,
    start: datetime.datetime,
    end: datetime.datetime,
) -> List[Dict[str, Any]]:
    """Run a PromQL range query for one project through the scoping project.

    The query itself must filter on the project's project_id label. project is
    only used in error messages.
    """
    url = (
        "https://monitoring.googleapis.com/v1/projects/"
        f"{SCOPING_PROJECT}/location/global/prometheus/api/v1/query_range"
    )
    for attempt in range(1, WARNING_ATTEMPTS + 1):
        response = session.get(
            url,
            params={
                "query": query,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "step": f"{int(STEP.total_seconds())}s",
            },
            timeout=120,
        )
        if not response.ok:
            raise RuntimeError(
                f"PromQL query failed for {project} ({response.status_code}): "
                f"{response.text}"
            )
        body = response.json()
        if body.get("status") != "success":
            raise RuntimeError(f"PromQL query failed for {project}: {body}")
        # Warnings such as execution_errors can mean the result is partial.
        # They have been transient so far, so retry before giving up
        warnings = body.get("warnings")
        if not warnings:
            # result is null rather than an empty list when nothing matches
            return body["data"]["result"] or []
        print(f"Warning from {project} (attempt {attempt}): {warnings}")
        if attempt < WARNING_ATTEMPTS:
            time.sleep(10 * attempt)
    raise RuntimeError(f"PromQL query for {project} kept returning warnings")


def to_rows(
    metric: MetricDefinition, project: str, result: Iterable[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Convert a PromQL matrix result into table rows."""
    rows = []
    for series in result:
        labels = series["metric"]
        label_rows = [
            {"key": key, "value": labels[key]}
            for key in metric.group_by
            if key in labels
        ]
        for timestamp, value in series["values"]:
            # Most Dataflow counters are idle in most hours. A missing row means zero
            if float(value) == 0:
                continue
            # Each point is the increase over the step ending at its timestamp
            end = datetime.datetime.fromtimestamp(
                float(timestamp), tz=datetime.timezone.utc
            )
            start = end - STEP
            rows.append(
                {
                    "submission_date": start.date().isoformat(),
                    "interval_start_time": start.isoformat(),
                    "interval_end_time": end.isoformat(),
                    "metric": metric.name,
                    "metric_type": metric.metric_type,
                    "project_id": labels.get("project_id", project),
                    "labels": label_rows,
                    "value": float(value),
                }
            )
    return rows


def fetch_day(
    session: AuthorizedSession,
    date: datetime.date,
    metrics: Iterable[MetricDefinition],
    now: Optional[datetime.datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch hourly rows for every metric in each of its projects on a date."""
    day_start = datetime.datetime.combine(date, datetime.time(), datetime.timezone.utc)
    # Points are evaluated at the end of each hour, from 01:00 to 24:00
    start, end = day_start + STEP, day_start + datetime.timedelta(days=1)
    # When loading the current day, stop at the last hour that has ended and
    # has had time to be ingested. Later hourly runs rewrite the partition
    now = now or datetime.datetime.now(datetime.timezone.utc)
    latest = (now - INGEST_DELAY).replace(minute=0, second=0, microsecond=0)
    end = min(end, latest)
    if end < start:
        print(f"No complete hours yet for {date}")
        return []

    tasks = [(metric, project) for metric in metrics for project in metric.projects]

    def fetch(task: Tuple[MetricDefinition, str]) -> List[Dict[str, Any]]:
        metric, project = task
        result = query_range(
            session, project, build_promql(metric, project), start, end
        )
        return to_rows(metric, project, result)

    # The Dataflow queries take the longest, so run queries in parallel. The
    # session is shared between threads. It only makes GET requests and its
    # connection pool is sized for PARALLELISM.
    with ThreadPool(PARALLELISM) as pool:
        results = pool.map(fetch, tasks, chunksize=1)

    rows = []
    for (metric, project), metric_rows in zip(tasks, results):
        print(f"{metric.name} in {project}: {len(metric_rows)} rows")
        rows.extend(metric_rows)
    return rows


def stored_metrics(
    client: bigquery.Client, destination_table: str, date: datetime.date
) -> Set[str]:
    """Return the metrics already stored in the date's partition."""
    try:
        rows = client.query_and_wait(
            f"SELECT DISTINCT metric FROM `{destination_table}` "
            "WHERE submission_date = @date",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", date)]
            ),
        )
    except NotFound:
        return set()
    return {row.metric for row in rows}


@click.command()
@click.option(
    "--date",
    required=True,
    type=datetime.date.fromisoformat,
    help="UTC date to load, in yyyy-mm-dd",
)
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Only query these projects. Defaults to each metric's projects.",
)
@click.option(
    "--metric",
    "metric_names",
    multiple=True,
    type=click.Choice([m.name for m in METRICS]),
    help="Only load these metrics. Defaults to all.",
)
@click.option(
    "--destination_table",
    "--destination-table",
    default="moz-fx-data-shared-prod.monitoring_derived.gcp_monitoring_metrics_hourly_v1",
    show_default=True,
)
@click.option(
    "--quota_project",
    "--quota-project",
    help="Project billed for Cloud Monitoring API requests. "
    "Defaults to the credentials' project.",
)
@click.option("--dry_run", "--dry-run", is_flag=True, default=False)
def main(date, projects, metric_names, destination_table, quota_project, dry_run):
    """Load one day of hourly metric values into a date partition."""
    if (metric_names or projects) and not dry_run:
        # Writing a subset would drop the other rows from the partition.
        raise click.UsageError("--metric and --project can only be used with --dry-run")
    metrics = [m for m in METRICS if not metric_names or m.name in metric_names]
    if projects:
        metrics = [
            replace(m, projects=tuple(p for p in m.projects if p in projects))
            for m in metrics
        ]

    session = create_session(quota_project)
    rows = fetch_day(session, date, metrics)

    if not rows:
        # Data past the retention window comes back empty
        raise click.ClickException(f"No metric data returned for {date}")

    if dry_run:
        print(f"Dry run: fetched {len(rows)} rows, not writing")
        return

    partition = f"{destination_table}${date.strftime('%Y%m%d')}"
    client = bigquery.Client(project=destination_table.split(".")[0])
    # Metrics have different retention (6 weeks for Pub/Sub and Dataflow, 24
    # months for load balancers), so rerunning an old date can return only
    # some of them. Don't replace stored rows that can't be fetched again.
    missing = (
        stored_metrics(client, destination_table, date) & {m.name for m in METRICS}
    ) - {row["metric"] for row in rows}
    if missing:
        raise click.ClickException(
            f"Not replacing {partition}: no data returned for "
            f"{', '.join(sorted(missing))}, which is already in the table"
        )
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="submission_date"),
        clustering_fields=["metric", "project_id"],
    )
    client.load_table_from_json(rows, partition, job_config=job_config).result()
    print(f"Wrote {len(rows)} rows to {partition}")


if __name__ == "__main__":
    main()
