#!/usr/bin/env python3

"""Load hourly values of selected Cloud Monitoring metrics into BigQuery.

Metric definitions are kept free of query-language details so the fetch can be
switched from the Cloud Monitoring PromQL endpoint to the Monitoring SDK
without changing the definitions or the output table.

Values are the increase of the metric over each hour, which fits DELTA metrics
and cumulative counters reported as GAUGE (e.g. Dataflow user counters).
Cloud Monitoring keeps roughly 6 weeks to 3 months of data depending on the
metric, so older partitions cannot be rebuilt.
"""

import datetime
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_PROJECTS = (
    "moz-fx-data-beam-nonprod-b5f4",
    "moz-fx-data-beam-prod-11f7",
    "moz-fx-data-inge-nonprod-e7b1",
    "moz-fx-data-ingesti-prod-71dc",
    "moz-fx-ingestion-edge-nonprod",
    "moz-fx-ingestion-edge-prod",
    "moz-fx-ingestion-sink-nonprod",
    "moz-fx-ingestion-sink-prod",
    "moz-fx-data-shar-nonprod-efed",
)


@dataclass(frozen=True)
class MetricDefinition:
    """A Cloud Monitoring metric to aggregate by hour.

    name is written to the metric column. group_by lists the resource or metric
    labels to keep (project_id is always kept). label_filters maps label names
    to RE2 regular expressions that must fully match.
    """

    name: str
    metric_type: str
    group_by: Tuple[str, ...]
    label_filters: Dict[str, str] = field(default_factory=dict)


METRICS = (
    MetricDefinition(
        name="pubsub_topic_byte_cost",
        metric_type="pubsub.googleapis.com/topic/byte_cost",
        group_by=("topic_id", "operation_type"),
    ),
    MetricDefinition(
        name="pubsub_subscription_byte_cost",
        metric_type="pubsub.googleapis.com/subscription/byte_cost",
        group_by=("subscription_id", "operation_type"),
    ),
    MetricDefinition(
        name="dataflow_predecompress_submission_bytes",
        metric_type="dataflow.googleapis.com/job/user_counter",
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/predecompress_submission_bytes"},
    ),
    MetricDefinition(
        name="dataflow_valid_submission_bytes",
        metric_type="dataflow.googleapis.com/job/user_counter",
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/valid_submission_bytes"},
    ),
    MetricDefinition(
        name="dataflow_valid_submission",
        metric_type="dataflow.googleapis.com/job/user_counter",
        group_by=("job_name", "metric_name"),
        label_filters={"metric_name": ".+/valid_submission"},
    ),
)

SCHEMA = [
    bigquery.SchemaField("submission_date", "DATE"),
    bigquery.SchemaField("interval_start_time", "TIMESTAMP"),
    bigquery.SchemaField("interval_end_time", "TIMESTAMP"),
    bigquery.SchemaField("metric", "STRING"),
    bigquery.SchemaField("metric_type", "STRING"),
    bigquery.SchemaField("project_id", "STRING"),
    bigquery.SchemaField(
        "labels",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ],
    ),
    bigquery.SchemaField("value", "FLOAT64"),
]

STEP = datetime.timedelta(hours=1)
WARNING_ATTEMPTS = 3


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
    group_by = ", ".join(("project_id",) + metric.group_by)
    window = f"{int(STEP.total_seconds())}s"
    return f"sum by ({group_by}) (increase({{{', '.join(matchers)}}}[{window}]))"


def create_session(quota_project: Optional[str]) -> AuthorizedSession:
    """Create an authorized HTTP session that retries transient errors."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/monitoring.read"],
        quota_project_id=quota_project,
    )
    session = AuthorizedSession(credentials)
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def query_range(
    session: AuthorizedSession,
    project: str,
    query: str,
    start: datetime.datetime,
    end: datetime.datetime,
) -> List[Dict[str, Any]]:
    """Run a PromQL range query against the project's metrics scope."""
    url = (
        "https://monitoring.googleapis.com/v1/projects/"
        f"{project}/location/global/prometheus/api/v1/query_range"
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
        # They have been transient so far, so retry before giving up.
        warnings = body.get("warnings")
        if not warnings:
            # result is null rather than an empty list when nothing matches.
            return body["data"]["result"] or []
        print(f"Warning from {project} (attempt {attempt}): {warnings}")
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
            # Most Dataflow counters are idle in most hours. A missing row
            # means zero.
            if float(value) == 0:
                continue
            # Each point is the increase over the step ending at its timestamp.
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
    projects: Iterable[str],
    metrics: Iterable[MetricDefinition],
) -> List[Dict[str, Any]]:
    """Fetch hourly rows for every metric and project on the given date."""
    day_start = datetime.datetime.combine(date, datetime.time(), datetime.timezone.utc)
    # Points are evaluated at the end of each hour, from 01:00 to 24:00.
    start, end = day_start + STEP, day_start + datetime.timedelta(days=1)

    rows = []
    for metric in metrics:
        for project in projects:
            result = query_range(
                session, project, build_promql(metric, project), start, end
            )
            metric_rows = to_rows(metric, project, result)
            print(f"{metric.name} in {project}: {len(metric_rows)} rows")
            rows.extend(metric_rows)
    return rows


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
    default=DEFAULT_PROJECTS,
    show_default=True,
    help="Project to read metrics from. Can be repeated.",
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
    if metric_names:
        if not dry_run:
            # Writing a subset would drop the other metrics from the partition.
            raise click.UsageError("--metric can only be used with --dry-run")
        metrics = [m for m in METRICS if m.name in metric_names]
    else:
        metrics = list(METRICS)

    session = create_session(quota_project)
    rows = fetch_day(session, date, projects, metrics)

    if not rows:
        # Data past the retention window comes back empty. Refuse to replace
        # an existing partition with nothing.
        raise click.ClickException(f"No metric data returned for {date}")

    if dry_run:
        print(f"Dry run: fetched {len(rows)} rows, not writing")
        return

    partition = f"{destination_table}${date.strftime('%Y%m%d')}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="submission_date"),
        clustering_fields=["metric", "project_id"],
    )
    client = bigquery.Client(project=destination_table.split(".")[0])
    client.load_table_from_json(rows, partition, job_config=job_config).result()
    print(f"Wrote {len(rows)} rows to {partition}")


if __name__ == "__main__":
    main()
