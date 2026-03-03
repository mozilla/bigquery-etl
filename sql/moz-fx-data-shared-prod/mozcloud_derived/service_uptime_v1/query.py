"""
Uptime measurement script for MozCloud services.

For each service:
  1. Queries request volume to auto-determine the volume tier and rate window
  2. Queries service uptime using the appropriate rate window
  3. Loads per-service results to BigQuery
"""

import logging
from argparse import ArgumentParser
from datetime import datetime, timezone

import google.auth
import google.auth.transport.requests
import requests
from google.cloud import bigquery

V2_PROJECT_ID = "moz-fx-metric-scope-v2-prod"

# Ordered high → low. First tier whose min_rps is met wins.
# Tier names reflect the order-of-magnitude lower bound of their req/sec range.
# Tiers control rate window only; error threshold is computed dynamically.
VOLUME_TIERS = [
    {"name": "10k", "min_rps": 10000, "rate_window": "1m"},
    {"name": "1k", "min_rps": 1000, "rate_window": "5m"},
    {"name": "100", "min_rps": 100, "rate_window": "10m"},
    {"name": "10", "min_rps": 10, "rate_window": "15m"},
    {"name": "1", "min_rps": 1, "rate_window": "20m"},
    {"name": "<1", "min_rps": 0, "rate_window": "25m"},
]

# Threshold formula: T = max(BASE_THRESHOLD, CONFIDENCE_MULTIPLIER / sqrt(N))
# where N = expected requests in the rate window.
# CONFIDENCE_MULTIPLIER sets how many noise standard deviations above zero
# the threshold sits — higher = fewer false positives, less sensitive.
BASE_THRESHOLD = 0.01  # strictest threshold applied to high-volume services
CONFIDENCE_MULTIPLIER = 2  # ~2 standard deviations above the noise floor

LOOKBACK_DAYS = [90, 30, 7, 1]

APP_CODES = {
    "grafana": {},
    "monitor": {},
    "vpn": {},
    "autograph": {},
    "relay": {},
    "fxa": {},
    "remote-settings": {},
    "merino": {},
    "sync": {},
    "autopush": {},
    "experimenter": {},
}

TRAFFIC_QUERY = """
avg_over_time(
  sum(rate(loadbalancing_googleapis_com:https_backend_request_count{
    backend_target_name=~".*-${APP_TOKEN}.*",
    monitored_resource="https_lb_rule",
    backend_target_type="BACKEND_SERVICE",
    backend_type="NETWORK_ENDPOINT_GROUP",
    cache_result="DISABLED"
  }[10m]))[${LOOKBACK}d:10m]
)
"""

UPTIME_QUERY = """
100 *
avg_over_time(
  (
    (
      (
        sum(rate(loadbalancing_googleapis_com:https_backend_request_count{
          backend_target_name=~".*-${APP_TOKEN}.*",
          monitored_resource="https_lb_rule",
          backend_target_type="BACKEND_SERVICE",
          backend_type="NETWORK_ENDPOINT_GROUP",
          cache_result="DISABLED",
          response_code_class="500"
        }[${RATE_WINDOW}]))) or vector(0)
      )
      /
      (
        sum(rate(loadbalancing_googleapis_com:https_backend_request_count{
          backend_target_name=~".*-${APP_TOKEN}.*",
          monitored_resource="https_lb_rule",
          backend_target_type="BACKEND_SERVICE",
          backend_type="NETWORK_ENDPOINT_GROUP",
          cache_result="DISABLED"
        }[${RATE_WINDOW}]))) or vector(1)
      )
    ) < bool ${THRESHOLD}
  )[${LOOKBACK}d:${RATE_WINDOW}]
)
"""

SCHEMA = [
    bigquery.SchemaField("run_date", "TIMESTAMP"),
    bigquery.SchemaField("measured_at", "TIMESTAMP"),
    bigquery.SchemaField("app_code", "STRING"),
    bigquery.SchemaField("lookback_days", "INTEGER"),
    bigquery.SchemaField("requests_per_sec", "FLOAT"),
    bigquery.SchemaField("volume_tier", "STRING"),
    bigquery.SchemaField("rate_window", "STRING"),
    bigquery.SchemaField("error_threshold_pct", "FLOAT"),
    bigquery.SchemaField("uptime_pct", "FLOAT"),
]


def get_access_token() -> str:
    """Get a Google Cloud access token for the Monitoring API."""
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/monitoring.read"]
    )
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.token


def determine_tier(rps: float) -> dict:
    """Return the volume tier for a given requests-per-second value."""
    for tier in VOLUME_TIERS:
        if rps >= tier["min_rps"]:
            return tier
    return VOLUME_TIERS[-1]


def compute_threshold(rps: float, rate_window: str) -> float:
    """Compute error threshold using T = max(BASE_THRESHOLD, c / sqrt(N)).

    N is the expected number of requests in the rate window. The dynamic
    component ensures low-volume services aren't penalised by noise — the
    threshold rises until it sits c standard deviations above the noise floor.
    """
    window_minutes = int(rate_window.rstrip("m"))
    n = rps * window_minutes * 60
    if n <= 0:
        return 1.0
    dynamic = CONFIDENCE_MULTIPLIER / (n**0.5)
    return max(BASE_THRESHOLD, dynamic)


def query_promql(
    project_id: str,
    promql_query: str,
    query_time: datetime,
    access_token: str,
) -> float | None:
    """Query the GCP Monitoring Prometheus API and return a scalar float."""
    url = f"https://monitoring.googleapis.com/v1/projects/{project_id}/location/global/prometheus/api/v1/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    time_str = query_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        response = requests.post(
            url, headers=headers, data={"query": promql_query, "time": time_str}
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "success" and data.get("data"):
            result = data["data"].get("result")
            result_type = data["data"].get("resultType")
            if result and result_type == "scalar":
                return float(result[1])
            elif result and result_type == "vector":
                return float(result[0]["value"][1])

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error: {e}")
        if e.response is not None:
            logging.error(f"Response body: {e.response.text}")
    except (KeyError, IndexError, ValueError) as e:
        logging.error(f"Parse error: {e}")

    return None


def measure_services(
    apps: dict, query_time: datetime, access_token: str, lookback_days: list[int]
) -> list[dict]:
    """Measure uptime for all apps across all lookback windows."""
    services = []
    run_date = query_time.strftime("%Y-%m-%d")
    measured_at = query_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    for app_key, app_meta in apps.items():
        app_token = f"{app_key}-prod"
        project_id = app_meta.get("metric_scope", V2_PROJECT_ID)

        for lookback in lookback_days:
            logging.info(f"[{app_key}] [{lookback}d] Querying traffic volume...")
            rps = query_promql(
                project_id,
                TRAFFIC_QUERY.replace("${APP_TOKEN}", app_token).replace(
                    "${LOOKBACK}", str(lookback)
                ),
                query_time,
                access_token,
            )

            if rps is None:
                logging.warning(f"[{app_key}] [{lookback}d] No RPS data, skipping.")
                continue

            tier = determine_tier(rps)
            error_threshold = compute_threshold(rps, tier["rate_window"])
            logging.info(
                f"[{app_key}] [{lookback}d] {rps:.4f} req/sec → {tier['name']} tier (threshold: {error_threshold:.2%})"
            )

            logging.info(f"[{app_key}] [{lookback}d] Querying uptime...")
            uptime = query_promql(
                project_id,
                UPTIME_QUERY.replace("${APP_TOKEN}", app_token)
                .replace("${LOOKBACK}", str(lookback))
                .replace("${RATE_WINDOW}", tier["rate_window"])
                .replace("${THRESHOLD}", str(error_threshold)),
                query_time,
                access_token,
            )
            uptime_str = f"{uptime:.2f}%" if uptime is not None else "no data"
            logging.info(f"[{app_key}] [{lookback}d] Uptime: {uptime_str}")

            services.append(
                {
                    "run_date": run_date,
                    "measured_at": measured_at,
                    "app_code": app_key,
                    "lookback_days": lookback,
                    "requests_per_sec": round(rps, 4),
                    "volume_tier": tier["name"],
                    "rate_window": tier["rate_window"],
                    "error_threshold_pct": round(error_threshold * 100, 4),
                    "uptime_pct": round(uptime, 4) if uptime is not None else None,
                }
            )

    return services


def load_to_bigquery(
    project: str, dataset: str, table: str, records: list[dict]
) -> None:
    """Load service uptime records to BigQuery."""
    client = bigquery.Client(project)
    destination = f"{project}.{dataset}.{table}"

    time_partitioning = bigquery.TimePartitioning(
        type=bigquery.TimePartitioningType.DAY,
        field="run_date",  # The column to partition by
        expiration_ms=None,
        require_partition_filter=False,  # Optional: enforce partition filtering
    )

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=time_partitioning,
    )

    job = client.load_table_from_json(records, destination, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(records)} records to {destination}")


def main():
    """Measure service uptime and SLO compliance for MozCloud apps."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="mozcloud_derived")
    parser.add_argument("--table", default="service_uptime_v1")
    parser.add_argument("--app", help="Filter to a single app (e.g. 'fxa').")
    parser.add_argument(
        "--date",
        help="Date to query uptime for (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--lookback-days",
        nargs="+",
        type=int,
        default=LOOKBACK_DAYS,
        metavar="N",
        help="Lookback window(s) in days (default: 90 30 7 1).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.app:
        if args.app not in APP_CODES:
            logging.error(
                f"Unknown app '{args.app}'. Available: {', '.join(APP_CODES)}"
            )
            raise SystemExit(1)
        apps = {args.app: APP_CODES[args.app]}
    else:
        apps = APP_CODES

    if args.date:
        query_time = datetime.strptime(args.date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    else:
        query_time = datetime.now(timezone.utc)
    access_token = get_access_token()

    services = measure_services(apps, query_time, access_token, args.lookback_days)
    load_to_bigquery(args.project, args.dataset, args.table, services)


if __name__ == "__main__":
    main()
