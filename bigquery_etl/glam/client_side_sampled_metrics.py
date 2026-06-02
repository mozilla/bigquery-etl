"""Client-side sampled metrics.

Fetches sampled metrics information from BigQuery.
"""

import math
from textwrap import dedent
from typing import Iterable, List, Optional

from google.cloud import bigquery

PROJECT_ID = "moz-fx-data-shared-prod"
DATASET = "telemetry_derived"
TABLE_NAME = "sampled_metrics_v1"
EXPECTED_SAMPLE_RATE = 0.1  # For now, we only support 10% sampling.

_PRODUCT_TO_APP_NAME = {
    "firefox_desktop": "firefox_desktop",
    # Experimenter doesn't distinguish Fenix variants; all map to appName=fenix.
    # Variants follow the Play Store app-id history
    # (see https://github.com/mozilla/telemetry-airflow/blob/2caa0865f017afa0743c94df02666c9af3d66862/dags/glam_fenix.py#L47-L56).
    "org_mozilla_fenix": "fenix",
    "org_mozilla_fenix_nightly": "fenix",
    "org_mozilla_firefox": "fenix",
    "org_mozilla_firefox_beta": "fenix",
    "org_mozilla_fennec_aurora": "fenix",
}


def _run_bq_query(query: str):
    """Run a BigQuery query and return rows."""
    client = bigquery.Client(project=PROJECT_ID)
    query_job = client.query(query)
    return query_job.result()


def _build_where_clause(metric_types: List[str], app_name: Optional[str]) -> str:
    """Return a SQL WHERE clause for metric_types + app_name, or '' if neither."""
    conditions = []
    if metric_types:
        quoted = ",".join(f"'{mt}'" for mt in sorted(set(metric_types)))
        conditions.append(f"metric_type IN ({quoted})")
    if app_name:
        conditions.append(f"app_name = '{app_name}'")
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def get(
    metric_types: Optional[Iterable[str]] = None,
    product: Optional[str] = None,
) -> dict[str, List[str]]:
    """Return sampled metrics grouped by metric_type from BigQuery.

    Takes the latest submission for each metric.
    """
    if metric_types is not None:
        metric_types = list(metric_types)
        if not metric_types:
            return {}
    else:
        metric_types = []
    if product is not None:
        if product not in _PRODUCT_TO_APP_NAME:
            raise ValueError(
                f"Unknown product {product!r}; "
                f"add a mapping in _PRODUCT_TO_APP_NAME if it's expected."
            )
        app_name = _PRODUCT_TO_APP_NAME[product]
    else:
        app_name = None
    where_clause = _build_where_clause(metric_types, app_name)
    # Rows with experimenter_slug = NULL are tombstones written by the
    # sampled_metrics ETL when a metric is no longer covered by any active
    # experiment/rollout. They aren't real sampled metrics and should be
    # filtered out before the sample-rate guard runs.
    query = dedent(f"""
        WITH latest_metrics AS (
          SELECT metric_type, metric_name, sample_rate, experimenter_slug, start_date
          FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}`
          {where_clause}
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY metric_type, metric_name, channel, app_name, os
            ORDER BY start_date DESC
          ) = 1
        )
        SELECT metric_type, metric_name, sample_rate
        FROM latest_metrics
        WHERE experimenter_slug IS NOT NULL
        """)

    try:
        rows = _run_bq_query(query)
    except Exception as e:
        raise RuntimeError("Failed to fetch sampled metrics") from e

    metrics: dict[str, List[str]] = {}
    for row in rows:
        sample_rate = float(row["sample_rate"])
        if not math.isclose(sample_rate, EXPECTED_SAMPLE_RATE, rel_tol=0, abs_tol=1e-9):
            raise RuntimeError(
                f"Sample rate for metric '{row['metric_name']}' "
                f"(type '{row['metric_type']}') is {row['sample_rate']} "
                f"not {EXPECTED_SAMPLE_RATE}"
            )
        metrics.setdefault(row["metric_type"], []).append(row["metric_name"])

    return metrics
