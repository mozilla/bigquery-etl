# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_analytics_tables

Built from bigquery-etl repo, [`dags/bqetl_analytics_tables.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_analytics_tables.py)

#### Description

Scheduled queries for analytics tables. engineering.
#### Owner

lvargas@mozilla.com
"""


default_args = {
    "owner": "lvargas@mozilla.com",
    "start_date": datetime.datetime(2022, 12, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "lvargas@mozilla.com",
        "gkaberere@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_analytics_tables",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_android_clients = bigquery_etl_query(
        task_id="firefox_android_clients",
        destination_table="firefox_android_clients_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    with TaskGroup(
        "firefox_android_clients_external"
    ) as firefox_android_clients_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_firefox_android_clients",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_firefox_android_clients",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_android_clients_external.set_upstream(firefox_android_clients)

    firefox_ios_clients = bigquery_etl_query(
        task_id="firefox_ios_clients",
        destination_table="firefox_ios_clients_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kignasiak@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kignasiak@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    with TaskGroup("firefox_ios_clients_external") as firefox_ios_clients_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_firefox_ios_clients",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_firefox_ios_clients",
        )

        firefox_ios_clients_external.set_upstream(firefox_ios_clients)

    wait_for_baseline_clients_daily = ExternalTaskSensor(
        task_id="wait_for_baseline_clients_daily",
        external_dag_id="copy_deduplicate",
        external_task_id="baseline_clients_daily",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_android_clients.set_upstream(wait_for_baseline_clients_daily)

    firefox_ios_clients.set_upstream(wait_for_baseline_clients_daily)
    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_clients.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_firefox_ios_derived__new_profile_activation__v2 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__new_profile_activation__v2",
        external_dag_id="bqetl_mobile_activation",
        external_task_id="firefox_ios_derived__new_profile_activation__v2",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_clients.set_upstream(
        wait_for_firefox_ios_derived__new_profile_activation__v2
    )
