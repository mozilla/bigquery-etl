# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_internet_outages

Built from bigquery-etl repo, [`dags/bqetl_internet_outages.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_internet_outages.py)

#### Description

DAG for building the internet outages datasets.
See [bug 1640204](https://bugzilla.mozilla.org/show_bug.cgi?id=1640204).

#### Owner

aplacitelli@mozilla.com
"""


default_args = {
    "owner": "aplacitelli@mozilla.com",
    "start_date": datetime.datetime(2020, 1, 1, 0, 0),
    "end_date": None,
    "email": ["aplacitelli@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_internet_outages",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    internet_outages__global_outages__v1 = bigquery_etl_query(
        task_id="internet_outages__global_outages__v1",
        destination_table="global_outages_v1",
        dataset_id="internet_outages",
        project_id="moz-fx-data-shared-prod",
        owner="aplacitelli@mozilla.com",
        email=["aplacitelli@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    internet_outages__global_outages__v1.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    internet_outages__global_outages__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )
    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=18000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    internet_outages__global_outages__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
