# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

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
    checks__fail_fenix_derived__firefox_android_clients__v2 = bigquery_dq_check(
        task_id="checks__fail_fenix_derived__firefox_android_clients__v2",
        source_table="firefox_android_clients_v2",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_fenix_derived__firefox_android_clients__v2 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__firefox_android_clients__v2",
        source_table="firefox_android_clients_v2",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    clients_first_seen_v2 = bigquery_etl_query(
        task_id="clients_first_seen_v2",
        destination_table="clients_first_seen_v2",
        dataset_id="telemetry_derived",
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

    fenix_derived__firefox_android_clients__v2 = bigquery_etl_query(
        task_id="fenix_derived__firefox_android_clients__v2",
        destination_table="firefox_android_clients_v2",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

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

    wait_for_baseline_clients_daily = ExternalTaskSensor(
        task_id="wait_for_baseline_clients_daily",
        external_dag_id="copy_deduplicate",
        external_task_id="baseline_clients_daily",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_fenix_derived__firefox_android_clients__v2.set_upstream(
        wait_for_baseline_clients_daily
    )

    checks__fail_fenix_derived__firefox_android_clients__v2.set_upstream(
        fenix_derived__firefox_android_clients__v2
    )

    checks__warn_fenix_derived__firefox_android_clients__v2.set_upstream(
        wait_for_baseline_clients_daily
    )

    checks__warn_fenix_derived__firefox_android_clients__v2.set_upstream(
        fenix_derived__firefox_android_clients__v2
    )

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

    clients_first_seen_v2.set_upstream(wait_for_copy_deduplicate_all)
    wait_for_copy_deduplicate_first_shutdown_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_first_shutdown_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_first_shutdown_ping",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    clients_first_seen_v2.set_upstream(wait_for_copy_deduplicate_first_shutdown_ping)
    wait_for_telemetry_derived__clients_daily__v6 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily__v6",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    clients_first_seen_v2.set_upstream(wait_for_telemetry_derived__clients_daily__v6)

    fenix_derived__firefox_android_clients__v2.set_upstream(
        wait_for_baseline_clients_daily
    )
    fenix_derived__firefox_android_clients__v2.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_fenix_derived__new_profile_activation__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__new_profile_activation__v1",
        external_dag_id="bqetl_mobile_activation",
        external_task_id="fenix_derived__new_profile_activation__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__firefox_android_clients__v2.set_upstream(
        wait_for_fenix_derived__new_profile_activation__v1
    )

    firefox_android_clients.set_upstream(wait_for_baseline_clients_daily)
