# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_review_checker

Built from bigquery-etl repo, [`dags/bqetl_review_checker.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_review_checker.py)

#### Description

DAG to build review checker data
#### Owner

akommasani@mozilla.com
"""


default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "akommasani@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_review_checker",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_desktop_review_checker_clients__v1 = bigquery_etl_query(
        task_id="firefox_desktop_review_checker_clients__v1",
        destination_table="review_checker_clients_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "betling@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_review_checker_events__v1 = bigquery_etl_query(
        task_id="firefox_desktop_review_checker_events__v1",
        destination_table="review_checker_events_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "betling@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_review_checker_microsurvey__v1 = bigquery_etl_query(
        task_id="firefox_desktop_review_checker_microsurvey__v1",
        destination_table="review_checker_microsurvey_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "betling@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
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

    firefox_desktop_review_checker_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=datetime.timedelta(days=-1, seconds=75600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_review_checker_clients__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )
    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_review_checker_clients__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    firefox_desktop_review_checker_events__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    wait_for_clients_first_seen_v2 = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen_v2",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="clients_first_seen_v2",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_review_checker_microsurvey__v1.set_upstream(
        wait_for_clients_first_seen_v2
    )
    firefox_desktop_review_checker_microsurvey__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
