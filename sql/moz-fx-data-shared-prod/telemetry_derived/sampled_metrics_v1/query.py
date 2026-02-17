#!/usr/bin/env python3

"""Import sampled metrics from Experimenter via the Experimenter API."""

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


def get_sampled_metrics():
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


def main():
    """Run."""
    args = parser.parse_args()
    rows = get_sampled_metrics()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    if args.dry_run:
        print(json.dumps(rows, indent=2))
        print(f"\nTotal rows: {len(rows)}")
        sys.exit(0)

    bq_schema = (
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

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = bq_schema

    client = bigquery.Client(args.project)
    client.load_table_from_json(rows, destination_table, job_config=job_config).result()
    print(f"Loaded {len(rows)} sampled metric rows")


if __name__ == "__main__":
    main()
