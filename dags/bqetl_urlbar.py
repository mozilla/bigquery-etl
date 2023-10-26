# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_urlbar

Built from bigquery-etl repo, [`dags/bqetl_urlbar.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_urlbar.py)

#### Description

Daily aggregation of metrics related to urlbar usage.

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2021, 8, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
        "akomar@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_urlbar",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_desktop_urlbar_events__v2 = bigquery_etl_query(
        task_id="firefox_desktop_urlbar_events__v2",
        destination_table="urlbar_events_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "akommasani@mozilla.com",
            "anicholson@mozilla.com",
            "dzeber@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__urlbar_clients_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__urlbar_clients_daily__v1",
        destination_table="urlbar_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "tbrooks@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_urlbar_events__v2.set_upstream(wait_for_copy_deduplicate_all)

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__urlbar_clients_daily__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
