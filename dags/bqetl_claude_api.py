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
### bqetl_claude_api

Built from bigquery-etl repo, [`dags/bqetl_claude_api.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_claude_api.py)

#### Description

Pulls daily API usage data from the Anthropic Admin API.
Tracks token consumption by model and API key.

#### Owner

mcastelluccio@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

claude_api_derived__api_keys__v1_bqetl_claude_api__admin_api_key = Secret(
    deploy_type="env",
    deploy_target="ANTHROPIC_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_claude_api__admin_api_key",
)
claude_api_derived__usage__v1_bqetl_claude_api__admin_api_key = Secret(
    deploy_type="env",
    deploy_target="ANTHROPIC_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_claude_api__admin_api_key",
)


default_args = {
    "owner": "mcastelluccio@mozilla.com",
    "start_date": datetime.datetime(2026, 1, 21, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_claude_api",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    claude_api_derived__api_keys__v1 = GKEPodOperator(
        task_id="claude_api_derived__api_keys__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/claude_api_derived/api_keys_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            claude_api_derived__api_keys__v1_bqetl_claude_api__admin_api_key,
        ],
    )

    claude_api_derived__usage__v1 = GKEPodOperator(
        task_id="claude_api_derived__usage__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/claude_api_derived/usage_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            claude_api_derived__usage__v1_bqetl_claude_api__admin_api_key,
        ],
    )
