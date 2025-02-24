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
### bqetl_jira_service_desk

Built from bigquery-etl repo, [`dags/bqetl_jira_service_desk.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_jira_service_desk.py)

#### Description

This DAG schedules some tasks that fetch data from the Jira API for service desk tickets
#### Owner

jmoscon@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

jira_service_desk_derived__user__v1_bqetl_jira_service_desk__jira_username = Secret(
    deploy_type="env",
    deploy_target="JIRA_USERNAME",
    secret="airflow-gke-secrets",
    key="bqetl_jira_service_desk__jira_username",
)
jira_service_desk_derived__user__v1_bqetl_jira_service_desk__jira_token = Secret(
    deploy_type="env",
    deploy_target="JIRA_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_jira_service_desk__jira_token",
)


default_args = {
    "owner": "jmoscon@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 16, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "jmoscon@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_jira_service_desk",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    jira_service_desk_derived__user__v1 = GKEPodOperator(
        task_id="jira_service_desk_derived__user__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/jira_service_desk_derived/user_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="jmoscon@mozilla.com",
        email=["jmoscon@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            jira_service_desk_derived__user__v1_bqetl_jira_service_desk__jira_username,
            jira_service_desk_derived__user__v1_bqetl_jira_service_desk__jira_token,
        ],
    )
