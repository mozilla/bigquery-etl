# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_desktop_installs_v1

Built from bigquery-etl repo, [`dags/bqetl_desktop_installs_v1.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_desktop_installs_v1.py)

#### Description

DAG to build mozdata-fx-data-shared-prod.firefox_desktop_derived.desktop_installs_v1 table
#### Owner

kwindau@mozilla.com
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2023, 12, 28, 0, 0),
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
    "bqetl_desktop_installs_v1",
    default_args=default_args,
    schedule_interval="55 23 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_desktop_derived__desktop_installs__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__desktop_installs__v1",
        destination_table="desktop_installs_v1",
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
        execution_delta=datetime.timedelta(seconds=82500),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_derived__desktop_installs__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_ga_derived__downloads_with_attribution__v1 = ExternalTaskSensor(
        task_id="wait_for_ga_derived__downloads_with_attribution__v1",
        external_dag_id="bqetl_google_analytics_derived",
        external_task_id="ga_derived__downloads_with_attribution__v1",
        execution_delta=datetime.timedelta(seconds=3300),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_derived__desktop_installs__v1.set_upstream(
        wait_for_ga_derived__downloads_with_attribution__v1
    )
