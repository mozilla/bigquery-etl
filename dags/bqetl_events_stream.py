# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_events_stream

Built from bigquery-etl repo, [`dags/bqetl_events_stream.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_events_stream.py)

#### Description

Event stream tables
#### Owner

wstuckey@mozilla.com
"""


default_args = {
    "owner": "wstuckey@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 19, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "wstuckey@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_events_stream",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:
    org_mozilla_firefox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wstuckey@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wstuckey@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    org_mozilla_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
