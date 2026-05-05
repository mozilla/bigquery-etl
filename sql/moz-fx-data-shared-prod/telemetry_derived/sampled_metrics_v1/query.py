#!/usr/bin/env python3

"""Import sampled metrics from Experimenter via the Experimenter API.

Appends new rows when sampling state changes. The latest row per
(metric_type, metric_name, channel, app_name) represents the current state.
Metrics that are no longer sampled get a sample_rate=1.0 record inserted.
"""

import datetime
import json
import re
from argparse import ArgumentParser

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EXPERIMENTER_API_URL = "https://experimenter.services.mozilla.com/api/v8/experiments/"

GLEAN_FEATURE_ID = "gleanInternalSdk"

BQ_SCHEMA = (
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("experimenter_slug", "STRING"),
    bigquery.SchemaField("is_rollout", "BOOLEAN"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("min_version", "STRING"),
    bigquery.SchemaField("max_version", "STRING"),
    bigquery.SchemaField("end_date", "DATE"),
    bigquery.SchemaField("metric_type", "STRING"),
    bigquery.SchemaField("metric_name", "STRING"),
    bigquery.SchemaField("sample_rate", "FLOAT"),
)

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="telemetry_derived")
parser.add_argument("--destination_table", default="sampled_metrics_v1")
parser.add_argument("--dry_run", action="store_true")


def fetch(url, retries=5, backoff_factor=2):
    """Fetch a URL with exponential backoff retry."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,  # Favour error from response.raise_for_status().
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    response = session.get(
        url,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_channel(targeting):
    """Parse the release channel from a targeting string."""
    match = re.search(r'browserSettings\.update\.channel\s*==\s*"(\w+)"', targeting)
    if match:
        return match.group(1)
    match = re.search(r'browserSettings\.update\.channel\s+in\s+\["([^"]+)"', targeting)
    if match:
        return match.group(1)
    return None


def parse_min_version(targeting):
    """Parse the minimum Firefox version from a targeting string."""
    match = re.search(r"version\|versionCompare\('([^']+)'\)\s*>=\s*0", targeting)
    if match:
        return match.group(1)
    return None


def parse_max_version(targeting):
    """Parse the maximum Firefox version from a targeting string."""
    match = re.search(r"version\|versionCompare\('([^']+)'\)\s*<\s*0", targeting)
    if match:
        return match.group(1)
    return None


def is_active(experiment):
    """Check if an experiment/rollout is currently active."""
    end_date = experiment.get("endDate")
    if end_date is None:
        return True
    try:
        end = datetime.date.fromisoformat(end_date)
        return end > datetime.date.today()
    except (ValueError, TypeError):
        return False


def get_sampled_metrics_from_api():
    """Fetch sampled metrics from active experiments/rollouts."""
    experiments = fetch(EXPERIMENTER_API_URL)
    rows = []

    for exp in experiments:
        feature_ids = exp.get("featureIds", [])
        if GLEAN_FEATURE_ID not in feature_ids:
            continue

        if not is_active(exp):
            continue

        slug = exp.get("slug", "")
        is_rollout = exp.get("isRollout", False)
        app_name = exp.get("appName", "")
        targeting = exp.get("targeting", "")
        start_date = exp.get("startDate")
        end_date = exp.get("endDate")

        bucket_config = exp.get("bucketConfig", {})
        count = bucket_config.get("count", 0)
        total = bucket_config.get("total", 1)
        # The rollout disables metrics for count/total of clients,
        # so the fraction that still sends the metric is 1 - count/total.
        sample_rate = round(1 - (count / total), 4) if total > 0 else 1.0

        channel = parse_channel(targeting)
        min_version = parse_min_version(targeting)
        max_version = parse_max_version(targeting)

        # Collect metrics set to false (disabled for this population).
        sampled_metrics = set()
        for branch in exp.get("branches", []):
            for feature in branch.get("features", []):
                value = feature.get("value", {})
                if not isinstance(value, dict):
                    continue
                config = value.get("gleanMetricConfiguration", {})
                metrics_enabled = config.get("metrics_enabled", {})
                for metric, enabled in metrics_enabled.items():
                    if enabled is False:
                        sampled_metrics.add(metric)

        for metric in sorted(sampled_metrics):
            parts = metric.split(".", 1)
            if len(parts) == 2:
                metric_type, metric_name = parts
            else:
                metric_type = None
                metric_name = metric

            rows.append(
                {
                    "start_date": start_date,
                    "experimenter_slug": slug,
                    "is_rollout": is_rollout,
                    "app_name": app_name,
                    "channel": channel,
                    "min_version": min_version,
                    "max_version": max_version,
                    "end_date": end_date,
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "sample_rate": sample_rate,
                }
            )

    return rows


def get_current_state(client, destination_table):
    """Query the latest row per metric from BigQuery.

    Returns a dict keyed by (metric_type, metric_name, channel, app_name)
    with the sample_rate as value.
    """
    query = f"""
        SELECT metric_type, metric_name, channel, app_name, sample_rate
        FROM `{destination_table}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY metric_type, metric_name, channel, app_name
            ORDER BY start_date DESC
        ) = 1
    """
    state = {}
    for row in client.query(query).result():
        key = (
            row["metric_type"],
            row["metric_name"],
            row["channel"],
            row["app_name"],
        )
        state[key] = float(row["sample_rate"])
    return state


def compute_diff(api_rows, current_state):
    """Compute rows to insert based on changes from current state.

    Returns only rows where the sample_rate changed, plus sample_rate=1.0
    rows for metrics that are no longer sampled.
    """
    today = datetime.date.today().isoformat()

    # Build a lookup of what the API says the current state should be
    api_state = {}
    api_row_lookup = {}
    for row in api_rows:
        key = (
            row["metric_type"],
            row["metric_name"],
            row["channel"],
            row["app_name"],
        )
        # If multiple experiments affect the same metric, keep the most
        # recent one (by start date)
        if key not in api_state or (row["start_date"] or "") > (
            api_row_lookup[key]["start_date"] or ""
        ):
            api_state[key] = row["sample_rate"]
            api_row_lookup[key] = row

    rows_to_insert = []

    # New or changed metrics
    for key, sample_rate in api_state.items():
        current_rate = current_state.get(key)
        if current_rate is None or round(current_rate, 4) != round(sample_rate, 4):
            rows_to_insert.append(api_row_lookup[key])

    # Metrics that are no longer sampled (were < 1.0, now absent from API)
    for key, current_rate in current_state.items():
        if key not in api_state and round(current_rate, 4) != 1.0:
            metric_type, metric_name, channel, app_name = key
            rows_to_insert.append(
                {
                    "start_date": today,
                    "experimenter_slug": None,
                    "is_rollout": None,
                    "app_name": app_name,
                    "channel": channel,
                    "min_version": None,
                    "max_version": None,
                    "end_date": None,
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "sample_rate": 1.0,
                }
            )

    return rows_to_insert


def main():
    """Run."""
    args = parser.parse_args()
    api_rows = get_sampled_metrics_from_api()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    client = bigquery.Client(args.project)
    current_state = get_current_state(client, destination_table)
    rows_to_insert = compute_diff(api_rows, current_state)

    if args.dry_run:
        print(json.dumps(rows_to_insert, indent=2))
        print(f"\nAPI rows: {len(api_rows)}")
        print(f"Rows to insert: {len(rows_to_insert)}")
        return

    if not rows_to_insert:
        print("No changes detected, nothing to insert")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND,
    )
    job_config.schema = BQ_SCHEMA

    client.load_table_from_json(
        rows_to_insert, destination_table, job_config=job_config
    ).result()
    print(f"Inserted {len(rows_to_insert)} rows")


if __name__ == "__main__":
    main()
