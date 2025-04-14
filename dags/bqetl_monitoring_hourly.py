# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_monitoring_hourly

Built from bigquery-etl repo, [`dags/bqetl_monitoring_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_monitoring_hourly.py)

#### Description

Data platform monitoring queries running hourly
#### Owner

bewu@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2025, 4, 11, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_monitoring_hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    monitoring_derived__jobs_by_organization__v1 = GKEPodOperator(
        task_id="monitoring_derived__jobs_by_organization__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/jobs_by_organization_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    with TaskGroup(
        "monitoring_derived__jobs_by_organization__v1_external",
    ) as monitoring_derived__jobs_by_organization__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_monitoring__wait_for_monitoring_derived__jobs_by_organization__v1",
            external_dag_id="bqetl_monitoring",
            external_task_id="wait_for_monitoring_derived__jobs_by_organization__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_monitoring__wait_for_monitoring_derived__jobs_by_organization__v1",
            external_dag_id="bqetl_shredder_monitoring",
            external_task_id="wait_for_monitoring_derived__jobs_by_organization__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=43200)).isoformat() }}",
        )

        monitoring_derived__jobs_by_organization__v1_external.set_upstream(
            monitoring_derived__jobs_by_organization__v1
        )
