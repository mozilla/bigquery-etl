# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_sponsored_tiles_clients_daily

Built from bigquery-etl repo, [`dags/bqetl_sponsored_tiles_clients_daily.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_sponsored_tiles_clients_daily.py)

#### Description

daily run of sponsored tiles related fields
#### Owner

skahmann@mozilla.com
"""


default_args = {
    "owner": "skahmann@mozilla.com",
    "start_date": datetime.datetime(2022, 9, 13, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "skahmann@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_sponsored_tiles_clients_daily",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    sponsored_tiles_clients_daily_v1 = bigquery_etl_query(
        task_id="sponsored_tiles_clients_daily_v1",
        destination_table="sponsored_tiles_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "cmorales@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sponsored_tiles_clients_daily_v1.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sponsored_tiles_clients_daily_v1.set_upstream(wait_for_copy_deduplicate_main_ping)
    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sponsored_tiles_clients_daily_v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
    wait_for_telemetry_derived__unified_metrics__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__unified_metrics__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sponsored_tiles_clients_daily_v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )
