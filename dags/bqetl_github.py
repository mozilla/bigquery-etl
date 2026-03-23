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
### bqetl_github

Built from bigquery-etl repo, [`dags/bqetl_github.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_github.py)

#### Description

Ingests GitHub PR data for delivery visibility and incident analysis.
#### Owner

gkaberere@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

github_external__bqetl_prs__v1_bqetl_github__github_token = Secret(
    deploy_type="env",
    deploy_target="GITHUB_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_github__github_token",
)
github_external__private_bqetl_prs__v1_bqetl_github__github_token = Secret(
    deploy_type="env",
    deploy_target="GITHUB_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_github__github_token",
)


default_args = {
    "owner": "gkaberere@mozilla.com",
    "start_date": datetime.datetime(2025, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_github",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    github_external__bqetl_prs__v1 = GKEPodOperator(
        task_id="github_external__bqetl_prs__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/github_external/bqetl_prs_v1/query.py",
        ]
        + [
            "--date",
            "{{ds}}",
            "--repo",
            "mozilla/bigquery-etl",
            "--destination",
            "moz-fx-data-shared-prod.github_external.bqetl_prs_v1",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="gkaberere@mozilla.com",
        email=["gkaberere@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            github_external__bqetl_prs__v1_bqetl_github__github_token,
        ],
    )

    github_external__private_bqetl_prs__v1 = GKEPodOperator(
        task_id="github_external__private_bqetl_prs__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/github_external/private_bqetl_prs_v1/query.py",
        ]
        + [
            "--date",
            "{{ds}}",
            "--repo",
            "mozilla/private-bigquery-etl",
            "--destination",
            "moz-fx-data-shared-prod.github_external.private_bqetl_prs_v1",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="gkaberere@mozilla.com",
        email=["gkaberere@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            github_external__private_bqetl_prs__v1_bqetl_github__github_token,
        ],
    )
