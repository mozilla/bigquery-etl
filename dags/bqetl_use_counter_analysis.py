# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_use_counter_analysis

Built from bigquery-etl repo, [`dags/bqetl_use_counter_analysis.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_use_counter_analysis.py)

#### Description

DAG to prepare use counter data for Firefox Desktop & Fenix for visualization
#### Owner

kwindau@mozilla.com
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2023, 12, 13, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_use_counter_analysis",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_derived__fenix_use_counters__v1 = bigquery_etl_query(
        task_id="fenix_derived__fenix_use_counters__v1",
        destination_table="fenix_use_counters_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__firefox_desktop_use_counters__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__firefox_desktop_use_counters__v1",
        destination_table="firefox_desktop_use_counters_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=25200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__fenix_use_counters__v1.set_upstream(wait_for_copy_deduplicate_all)

    firefox_desktop_derived__firefox_desktop_use_counters__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
