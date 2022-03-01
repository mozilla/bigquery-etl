# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_feature_usage

Built from bigquery-etl repo, [`dags/bqetl_feature_usage.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_feature_usage.py)

#### Description

Daily aggregation of browser features usages from `main` pings,
`event` pings and addon data.

Depends on `bqetl_addons` and `bqetl_main_summary`, so is scheduled after.

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
        "loines@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_feature_usage",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    telemetry_derived__feature_usage__v2 = bigquery_etl_query(
        task_id="telemetry_derived__feature_usage__v2",
        destination_table="feature_usage_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.com",
        email=[
            "anicholson@mozilla.com",
            "ascholtz@mozilla.com",
            "cdowhygelund@mozilla.com",
            "loines@mozilla.com",
            "shong@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_bq_main_events = ExternalTaskCompletedSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_bq_main_events)
    wait_for_copy_deduplicate_all = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_event_events = ExternalTaskCompletedSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(wait_for_event_events)
    wait_for_telemetry_derived__addons__v2 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__addons__v2",
        external_dag_id="bqetl_addons",
        external_task_id="telemetry_derived__addons__v2",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__addons__v2
    )
    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
    wait_for_telemetry_derived__main_1pct__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__main_1pct__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_1pct__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__feature_usage__v2.set_upstream(
        wait_for_telemetry_derived__main_1pct__v1
    )
