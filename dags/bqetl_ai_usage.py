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
### bqetl_ai_usage

Built from bigquery-etl repo, [`dags/bqetl_ai_usage.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ai_usage.py)

#### Description

Monitors LLM API usage and costs across different providers.

#### Owner

phlee@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""

ai_usage_derived__claude_api_keys__v1_bqetl_ai_usage__anthropic_admin_api_key = Secret(
    deploy_type="env",
    deploy_target="ANTHROPIC_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_ai_usage__anthropic_admin_api_key",
)
ai_usage_derived__claude_costs__v1_bqetl_ai_usage__anthropic_admin_api_key = Secret(
    deploy_type="env",
    deploy_target="ANTHROPIC_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_ai_usage__anthropic_admin_api_key",
)
ai_usage_derived__claude_usage__v1_bqetl_ai_usage__anthropic_admin_api_key = Secret(
    deploy_type="env",
    deploy_target="ANTHROPIC_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_ai_usage__anthropic_admin_api_key",
)
ai_usage_derived__openai_completions__v1_bqetl_ai_usage__openai_admin_api_key = Secret(
    deploy_type="env",
    deploy_target="OPENAI_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_ai_usage__openai_admin_api_key",
)
ai_usage_derived__openai_costs__v1_bqetl_ai_usage__openai_admin_api_key = Secret(
    deploy_type="env",
    deploy_target="OPENAI_ADMIN_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_ai_usage__openai_admin_api_key",
)


default_args = {
    "owner": "phlee@mozilla.com",
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
    "bqetl_ai_usage",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    ai_usage_derived__ai_costs_combined__v1 = bigquery_etl_query(
        task_id="ai_usage_derived__ai_costs_combined__v1",
        destination_table="ai_costs_combined_v1",
        dataset_id="ai_usage_derived",
        project_id="moz-fx-data-shared-prod",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ai_usage_derived__claude_api_keys__v1 = GKEPodOperator(
        task_id="ai_usage_derived__claude_api_keys__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/ai_usage_derived/claude_api_keys_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            ai_usage_derived__claude_api_keys__v1_bqetl_ai_usage__anthropic_admin_api_key,
        ],
    )

    ai_usage_derived__claude_costs__v1 = GKEPodOperator(
        task_id="ai_usage_derived__claude_costs__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/ai_usage_derived/claude_costs_v1/query.py",
        ]
        + ["--date={{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            ai_usage_derived__claude_costs__v1_bqetl_ai_usage__anthropic_admin_api_key,
        ],
    )

    ai_usage_derived__claude_usage__v1 = GKEPodOperator(
        task_id="ai_usage_derived__claude_usage__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/ai_usage_derived/claude_usage_v1/query.py",
        ]
        + ["--date={{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="mcastelluccio@mozilla.com",
        email=["mcastelluccio@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            ai_usage_derived__claude_usage__v1_bqetl_ai_usage__anthropic_admin_api_key,
        ],
    )

    ai_usage_derived__openai_completions__v1 = GKEPodOperator(
        task_id="ai_usage_derived__openai_completions__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/ai_usage_derived/openai_completions_v1/query.py",
        ]
        + ["--date={{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            ai_usage_derived__openai_completions__v1_bqetl_ai_usage__openai_admin_api_key,
        ],
    )

    ai_usage_derived__openai_costs__v1 = GKEPodOperator(
        task_id="ai_usage_derived__openai_costs__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/ai_usage_derived/openai_costs_v1/query.py",
        ]
        + ["--date={{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            ai_usage_derived__openai_costs__v1_bqetl_ai_usage__openai_admin_api_key,
        ],
    )

    ai_usage_derived__ai_costs_combined__v1.set_upstream(
        ai_usage_derived__claude_costs__v1
    )

    ai_usage_derived__ai_costs_combined__v1.set_upstream(
        ai_usage_derived__openai_costs__v1
    )
