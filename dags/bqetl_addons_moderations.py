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
### bqetl_addons_moderations

Built from bigquery-etl repo, [`dags/bqetl_addons_moderations.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_addons_moderations.py)

#### Description

This DAG schedules queries for moderations of addons data ex. from the external API Cinder.
#### Owner

mhirose@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

addon_moderations_derived__cinder_decisions_raw__v1_bqetl_addons__cinder_bearer_token = Secret(
    deploy_type="env",
    deploy_target="CINDER_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_addons__cinder_bearer_token",
)


default_args = {
    "owner": "mhirose@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 2, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mhirose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_addons_moderations",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    addon_moderations_derived__cinder_decisions_raw__v1 = GKEPodOperator(
        task_id="addon_moderations_derived__cinder_decisions_raw__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/addon_moderations_derived/cinder_decisions_raw_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            addon_moderations_derived__cinder_decisions_raw__v1_bqetl_addons__cinder_bearer_token,
        ],
    )
