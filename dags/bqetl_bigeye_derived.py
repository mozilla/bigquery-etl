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
### bqetl_bigeye_derived

Built from bigquery-etl repo, [`dags/bqetl_bigeye_derived.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_bigeye_derived.py)

#### Description

Pulls metadata from BigEye API

#### Owner

phlee@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

bigeye_derived__collection_service__v1_bqetl_bigeye_api_key = Secret(
    deploy_type="env",
    deploy_target="BIGEYE_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_bigeye_api_key",
)
bigeye_derived__dashboard_service__v1_bqetl_bigeye_api_key = Secret(
    deploy_type="env",
    deploy_target="BIGEYE_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_bigeye_api_key",
)


default_args = {
    "owner": "phlee@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 14, 0, 0),
    "end_date": None,
    "email": ["phlee@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_bigeye_derived",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    bigeye_derived__collection_service__v1 = GKEPodOperator(
        task_id="bigeye_derived__collection_service__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/bigeye_derived/collection_service_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com"],
        secrets=[
            bigeye_derived__collection_service__v1_bqetl_bigeye_api_key,
        ],
    )

    bigeye_derived__dashboard_service__v1 = GKEPodOperator(
        task_id="bigeye_derived__dashboard_service__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/bigeye_derived/dashboard_service_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com"],
        secrets=[
            bigeye_derived__dashboard_service__v1_bqetl_bigeye_api_key,
        ],
    )
