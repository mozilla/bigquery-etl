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
### bqetl_us_bls_unemployment_data

Built from bigquery-etl repo, [`dags/bqetl_us_bls_unemployment_data.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_us_bls_unemployment_data.py)

#### Description

This DAG pulls US unemployment data from the BLS API

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

external_derived__us_unemployment__v1_bqetl_us_bls_unemployment_data__bls_us_api_key = (
    Secret(
        deploy_type="env",
        deploy_target="BLS_US_UNEMPLOYMENT_API_KEY",
        secret="airflow-gke-secrets",
        key="bqetl_us_bls_unemployment_data__bls_us_api_key",
    )
)


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 4, 1, 0, 0),
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
    "bqetl_us_bls_unemployment_data",
    default_args=default_args,
    schedule_interval="17 0 15 * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    external_derived__us_unemployment__v1 = GKEPodOperator(
        task_id="external_derived__us_unemployment__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/us_unemployment_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        secrets=[
            external_derived__us_unemployment__v1_bqetl_us_bls_unemployment_data__bls_us_api_key,
        ],
    )
