# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_status_check

Built from bigquery-etl repo, [`dags/bqetl_status_check.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_status_check.py)

#### Description

This DAG checks if bigquery-etl is working properly. Dummy ETL tasks are executed to detect
breakages as soon as possible.

*Triage notes*

None of these tasks should fail. If they do it is very likely that other/all ETL tasks will
subsequently fail as well. Any failures should be communicated to the Data Infra Working Group
as soon as possible.

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2023, 4, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_status_check",
    default_args=default_args,
    schedule_interval=datetime.timedelta(seconds=3600),
    doc_md=docs,
    tags=tags,
) as dag:
    monitoring_derived__bigquery_etl_python_run_check__v1 = gke_command(
        task_id="monitoring_derived__bigquery_etl_python_run_check__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_etl_python_run_check_v1/query.py",
        ]
        + ["--task_instance={{ task_instance_key_str }}", "--run_id={{ run_id }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    monitoring_derived__bigquery_etl_sql_run_check__v1 = bigquery_etl_query(
        task_id="monitoring_derived__bigquery_etl_sql_run_check__v1",
        destination_table="bigquery_etl_sql_run_check_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=[
            "task_instance:STRING:{{task_instance_key_str}}",
            "run_id:STRING:{{run_id}}",
        ],
    )
