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
### bqetl_jira_tickets

Built from bigquery-etl repo, [`dags/bqetl_jira_tickets.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_jira_tickets.py)

#### Description

This DAG schedules Jira ticket ingestion tasks
#### Owner

mcastelluccio@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

jira_tickets_derived__ffxp_epic_issues__v1_bqetl_jira_service_desk__jira_username = (
    Secret(
        deploy_type="env",
        deploy_target="JIRA_USERNAME",
        secret="airflow-gke-secrets",
        key="bqetl_jira_service_desk__jira_username",
    )
)
jira_tickets_derived__ffxp_epic_issues__v1_bqetl_jira_service_desk__jira_token = Secret(
    deploy_type="env",
    deploy_target="JIRA_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_jira_service_desk__jira_token",
)
jira_tickets_derived__iim_incident_issues__v1_bqetl_jira_service_desk__jira_username = (
    Secret(
        deploy_type="env",
        deploy_target="JIRA_USERNAME",
        secret="airflow-gke-secrets",
        key="bqetl_jira_service_desk__jira_username",
    )
)
jira_tickets_derived__iim_incident_issues__v1_bqetl_jira_service_desk__jira_token = (
    Secret(
        deploy_type="env",
        deploy_target="JIRA_TOKEN",
        secret="airflow-gke-secrets",
        key="bqetl_jira_service_desk__jira_token",
    )
)


default_args = {
    "owner": "mcastelluccio@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mcastelluccio@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_jira_tickets",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    jira_tickets_derived__ffxp_epic_issues__v1 = GKEPodOperator(
        task_id="jira_tickets_derived__ffxp_epic_issues__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/jira_tickets_derived/ffxp_epic_issues_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            jira_tickets_derived__ffxp_epic_issues__v1_bqetl_jira_service_desk__jira_username,
            jira_tickets_derived__ffxp_epic_issues__v1_bqetl_jira_service_desk__jira_token,
        ],
    )

    jira_tickets_derived__iim_incident_issues__v1 = GKEPodOperator(
        task_id="jira_tickets_derived__iim_incident_issues__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/jira_tickets_derived/iim_incident_issues_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            jira_tickets_derived__iim_incident_issues__v1_bqetl_jira_service_desk__jira_username,
            jira_tickets_derived__iim_incident_issues__v1_bqetl_jira_service_desk__jira_token,
        ],
    )
