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
### bqetl_semrush

Built from bigquery-etl repo, [`dags/bqetl_semrush.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_semrush.py)

#### Description

Daily ingestion of Semrush analytics data.
#### Owner

sherrera@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

semrush_derived__domain_overview__v1_bqetl_semrush_api_key = Secret(
    deploy_type="env",
    deploy_target="SEMRUSH_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_semrush_api_key",
)


default_args = {
    "owner": "sherrera@mozilla.com",
    "start_date": datetime.datetime(2026, 2, 24, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "sherrera@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_semrush",
    default_args=default_args,
    schedule_interval="0 14 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    semrush_derived__domain_overview__v1 = GKEPodOperator(
        task_id="semrush_derived__domain_overview__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/semrush_derived/domain_overview_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="sherrera@mozilla.com",
        email=["sherrera@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            semrush_derived__domain_overview__v1_bqetl_semrush_api_key,
        ],
    )
