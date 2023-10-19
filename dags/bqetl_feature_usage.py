# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_feature_usage

Built from bigquery-etl repo, [`dags/bqetl_feature_usage.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_feature_usage.py)

#### Description

schedule run for mobile feature usage tables
#### Owner

rzhao@mozilla.com
"""


default_args = {
    "owner": "rzhao@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 17, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "rzhao@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_feature_usage",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    feature_usage_events__v1 = bigquery_etl_query(
        task_id="feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ios_feature_usage_events__v1 = bigquery_etl_query(
        task_id="ios_feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ios_feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="ios_feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__feature_usage__v2 = bigquery_etl_query(
        task_id="telemetry_derived__feature_usage__v2",
        destination_table="feature_usage_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.com",
        email=[
            "anicholson@mozilla.com",
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "rzhao@mozilla.com",
            "shong@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_bq_main_events)
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

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_event_events)
    wait_for_telemetry_derived__addons__v2 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__addons__v2",
        external_dag_id="bqetl_addons",
        external_task_id="telemetry_derived__addons__v2",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__addons__v2
    )
    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
    wait_for_telemetry_derived__main_remainder_1pct__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__main_remainder_1pct__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_remainder_1pct__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )
