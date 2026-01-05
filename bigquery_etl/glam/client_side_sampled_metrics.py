"""Client-side sampled metrics.

Fetches sampled metrics from BigQuery instead of a hardcoded list.
"""

import json
import math
import subprocess
from typing import Iterable, List, Optional

PROJECT_ID = "moz-fx-glam-prod"
DATASET = "glam_etl"
TABLE_NAME = "sampled_metrics_v1"
EXPECTED_SAMPLE_RATE = 10.0  # For now, we only support 10% sampling.


def _run_bq_query(query: str) -> str:
    """Run a BigQuery query and return stdout."""
    res = subprocess.run(
        ["bq", "query", "--nouse_legacy_sql", "--format=json", query],
        check=True,
        stdout=subprocess.PIPE,
    )
    return res.stdout.decode().strip()


def _format_metric_types_filter(metric_types: Iterable[str]) -> str:
    """Return a SQL filter for the provided metric types."""
    unique_types = {mt for mt in metric_types}
    if not unique_types:
        return ""
    quoted = ",".join(f"'{mt}'" for mt in sorted(unique_types))
    return f"WHERE metric_type IN ({quoted})"


def get(metric_types: Optional[Iterable[str]] = None) -> dict[str, List[str]]:
    """Return sampled metrics grouped by metric_type from BigQuery.

    Takes the latest submission for each metric.
    Pass metric_types to restrict the query to specific types.
    """
    if metric_types is not None:
        metric_types = list(metric_types)
        if not metric_types:
            return {}
        where_clause = _format_metric_types_filter(metric_types)
    else:
        where_clause = ""
    query = (
        "WITH latest_metrics AS ("
        "  SELECT metric_type, metric_name, sample_rate, timestamp "
        f"  FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}` "
        f"  {where_clause} "
        "  QUALIFY ROW_NUMBER() OVER ("
        "    PARTITION BY metric_type, metric_name "
        "    ORDER BY timestamp DESC"
        "  ) = 1"
        ") "
        "SELECT metric_type, metric_name, sample_rate "
        "FROM latest_metrics "
    )

    try:
        output = _run_bq_query(query)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Failed to fetch sampled metrics") from e

    rows = json.loads(output) if output else []
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
