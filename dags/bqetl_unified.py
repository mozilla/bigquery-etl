# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_unified

Built from bigquery-etl repo, [`dags/bqetl_unified.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_unified.py)

#### Description

Schedule queries that unify metrics across all products.

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 10, 12, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
        "loines@mozilla.com",
        "lvargas@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_unified",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    telemetry_derived__rolling_cohorts__v1 = bigquery_etl_query(
        task_id="telemetry_derived__rolling_cohorts__v1",
        destination_table="rolling_cohorts_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=[
            "anicholson@mozilla.com",
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__unified_metrics__v1 = bigquery_etl_query(
        task_id="telemetry_derived__unified_metrics__v1",
        destination_table="unified_metrics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__rolling_cohorts__v1.set_upstream(
        telemetry_derived__unified_metrics__v1
    )

    wait_for_search_derived__mobile_search_clients_daily__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_mobile_search",
            external_task_id="search_derived__mobile_search_clients_daily__v1",
            execution_delta=datetime.timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )
    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
    wait_for_telemetry_derived__core_clients_last_seen__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
            external_dag_id="bqetl_core",
            external_task_id="telemetry_derived__core_clients_last_seen__v1",
            execution_delta=datetime.timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )
