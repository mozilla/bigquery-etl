# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_default

Built from bigquery-etl repo, [`dags/bqetl_default.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_default.py)

#### Description

This is a default DAG to schedule tasks with lower business impact or that don't require a new or existing DAG. Queries are automatically scheduled in this DAG during creation when no dag name is specified using option --dag. See [related documentation in the cookbooks](https://mozilla.github.io/bigquery-etl/cookbooks/creating_a_derived_dataset/)
#### Owner

telemetry-alerts@mozilla.com
"""


default_args = {
    "owner": "telemetry-alerts@mozilla.com",
    "start_date": datetime.datetime(2023, 9, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_default",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    analysis__bqetl_default_task__v1 = bigquery_etl_query(
        #### WARNING: This task has been scheduled in the default DAG. It can be moved to a more suitable DAG using `bqetl query schedule`.
        task_id="analysis__bqetl_default_task__v1",
        destination_table="bqetl_default_task_v1",
        dataset_id="analysis",
        project_id="moz-fx-data-shared-prod",
        owner="telemetry-alerts@mozilla.com",
        email=["telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__event_monitoring_live__v1 = bigquery_etl_query(
        #### WARNING: This task has been scheduled in the default DAG. It can be moved to a more suitable DAG using `bqetl query schedule`.
        task_id="monitoring_derived__event_monitoring_live__v1",
        destination_table="event_monitoring_live_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="example@mozilla.com",
        email=["example@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
