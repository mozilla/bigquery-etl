# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.secret import Secret
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_fred_daily_exchange_rate

Built from bigquery-etl repo, [`dags/bqetl_fred_daily_exchange_rate.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fred_daily_exchange_rate.py)

#### Description

This DAG pulls currency exchange rate info from the FRED API.

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

external_derived__fred_daily_exchange_rates__v1_bqetl_fred_daily_exchange_rate__api_key = Secret(
    deploy_type="env",
    deploy_target="FRED_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_fred_daily_exchange_rate__api_key",
)


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 8, 27, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1500),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_fred_daily_exchange_rate",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    external_derived__fred_daily_exchange_rates__v1 = GKEPodOperator(
        task_id="external_derived__fred_daily_exchange_rates__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/fred_daily_exchange_rates_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        secrets=[
            external_derived__fred_daily_exchange_rates__v1_bqetl_fred_daily_exchange_rate__api_key,
        ],
    )
