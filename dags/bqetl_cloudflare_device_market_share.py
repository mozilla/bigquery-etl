# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_cloudflare_device_market_share

Built from bigquery-etl repo, [`dags/bqetl_cloudflare_device_market_share.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_cloudflare_device_market_share.py)

#### Description

Pulls device usage market share data from Cloudflare API

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 6, 16, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_cloudflare_device_market_share",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    cloudflare_derived__device_usage__v1 = GKEPodOperator(
        task_id="cloudflare_derived__device_usage__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/cloudflare_derived/device_usage_v1/query.py",
        ]
        + [
            "--date",
            "{{ds}}",
            "--cloudflare_api_token",
            "{{ var.value.cloudflare_auth_token}}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
    )