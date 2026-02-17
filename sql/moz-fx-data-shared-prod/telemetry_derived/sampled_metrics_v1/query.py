#!/usr/bin/env python3

"""Import sampled metrics from Experimenter via the Experimenter API.

Appends new rows when sampling state changes. The latest row per
(metric_type, metric_name, channel, app_name) represents the current state.
Metrics that are no longer sampled get a sample_rate=1.0 record inserted.
"""

import datetime
import json
import re
import sys
import time
from argparse import ArgumentParser

import requests
from google.cloud import bigquery

EXPERIMENTER_API_URL = "https://experimenter.services.mozilla.com/api/v6/experiments/"

GLEAN_FEATURE_ID = "gleanInternalSdk"

BQ_SCHEMA = (
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("experiment_slug", "STRING"),
    bigquery.SchemaField("is_rollout", "BOOLEAN"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("start_version", "INTEGER"),
    bigquery.SchemaField("end_date", "TIMESTAMP"),
    bigquery.SchemaField("metric_type", "STRING"),
    bigquery.SchemaField("metric_name", "STRING"),
    bigquery.SchemaField("sample_rate", "FLOAT"),
)

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="telemetry_derived")
parser.add_argument("--destination_table", default="sampled_metrics_v1")
parser.add_argument("--dry_run", action="store_true")


def fetch(url, retries=5, initial_delay=2):
    """Fetch a URL with exponential backoff retry."""
    delay = initial_delay
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={"user-agent": "https://github.com/mozilla/bigquery-etl"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            if attempt < retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
    raise last_exception


def parse_channel(targeting):
    """Parse the release channel from a targeting string."""
    match = re.search(r'browserSettings\.update\.channel\s*==\s*"(\w+)"', targeting)
    if match:
        return match.group(1)
    match = re.search(r'browserSettings\.update\.channel\s+in\s+\["([^"]+)"', targeting)
    if match:
        return match.group(1)
    return None


def parse_start_version(targeting):
    """Parse the minimum Firefox version from a targeting string."""
    match = re.search(r"versionCompare\('(\d+)\.", targeting)
    if match:
        return int(match.group(1))
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
        sample_rate = count / total if total > 0 else 0.0

        channel = parse_channel(targeting)
        start_version = parse_start_version(targeting)

        # Collect all metrics set to true across all branches
        enabled_metrics = set()
        for branch in exp.get("branches", []):
            for feature in branch.get("features", []):
                value = feature.get("value", {})
                if not isinstance(value, dict):
                    continue
                config = value.get("gleanMetricConfiguration", {})
                metrics_enabled = config.get("metrics_enabled", {})
                for metric, enabled in metrics_enabled.items():
                    if enabled is True:
                        enabled_metrics.add(metric)

        for metric in sorted(enabled_metrics):
            parts = metric.split(".", 1)
            if len(parts) == 2:
                metric_type, metric_name = parts
            else:
                metric_type = None
                metric_name = metric

            rows.append(
                {
                    "timestamp": start_date,
                    "experiment_slug": slug,
                    "is_rollout": is_rollout,
                    "app_name": app_name,
                    "channel": channel,
                    "start_version": start_version,
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
            ORDER BY timestamp DESC
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
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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
        if key not in api_state or (row["timestamp"] or "") > (
            api_row_lookup[key]["timestamp"] or ""
        ):
            api_state[key] = row["sample_rate"]
            api_row_lookup[key] = row

    rows_to_insert = []

    # New or changed metrics
    for key, sample_rate in api_state.items():
        current_rate = current_state.get(key)
        if current_rate is None or abs(current_rate - sample_rate) > 1e-9:
            rows_to_insert.append(api_row_lookup[key])

    # Metrics that are no longer sampled (were < 1.0, now absent from API)
    for key, current_rate in current_state.items():
        if key not in api_state and abs(current_rate - 1.0) > 1e-9:
            metric_type, metric_name, channel, app_name = key
            rows_to_insert.append(
                {
                    "timestamp": now,
                    "experiment_slug": None,
                    "is_rollout": None,
                    "app_name": app_name,
                    "channel": channel,
                    "start_version": None,
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

    if args.dry_run:
        print(json.dumps(api_rows, indent=2))
        print(f"\nTotal API rows: {len(api_rows)}")
        print("(Diff against BigQuery not available in dry_run mode)")
        sys.exit(0)

    client = bigquery.Client(args.project)
    current_state = get_current_state(client, destination_table)
    rows_to_insert = compute_diff(api_rows, current_state)

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
