# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_experiments_daily

Built from bigquery-etl repo, [`dags/bqetl_experiments_daily.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_experiments_daily.py)

#### Description

The DAG schedules queries that query experimentation related
metrics (enrollments, search, ...) from stable tables to finalize
numbers of experiment monitoring datasets for a specific date.

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_experiments_daily",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    experiment_enrollment_daily_active_population = bigquery_etl_query(
        task_id="experiment_enrollment_daily_active_population",
        destination_table="experiment_enrollment_daily_active_population_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring__query_cost__v1 = bigquery_etl_query(
        task_id="monitoring__query_cost__v1",
        destination_table="query_cost_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-experiments",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__experiment_enrollment_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__experiment_enrollment_aggregates__v1",
        destination_table="experiment_enrollment_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__experiment_search_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__experiment_search_aggregates__v1",
        destination_table="experiment_search_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__experiments_daily_active_clients__v1 = bigquery_etl_query(
        task_id="telemetry_derived__experiments_daily_active_clients__v1",
        destination_table="experiments_daily_active_clients_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    experiment_enrollment_daily_active_population.set_upstream(
        telemetry_derived__experiments_daily_active_clients__v1
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_bq_main_events
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

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_event_events
    )

    telemetry_derived__experiment_search_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__experiment_search_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__experiments_daily_active_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
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

    telemetry_derived__experiments_daily_active_clients__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
