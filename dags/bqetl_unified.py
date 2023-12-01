# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

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
        date_partition_parameter="cohort_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__rolling_cohorts__v1_external",
    ) as telemetry_derived__rolling_cohorts__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_telemetry_derived__rolling_cohorts__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_telemetry_derived__rolling_cohorts__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81900)).isoformat() }}",
        )

        telemetry_derived__rolling_cohorts__v1_external.set_upstream(
            telemetry_derived__rolling_cohorts__v1
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

    with TaskGroup(
        "telemetry_derived__unified_metrics__v1_external",
    ) as telemetry_derived__unified_metrics__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_telemetry_derived__unified_metrics__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__wait_for_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_telemetry_derived__unified_metrics__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_sponsored_tiles_clients_daily__wait_for_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_sponsored_tiles_clients_daily",
            external_task_id="wait_for_telemetry_derived__unified_metrics__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_newtab__wait_for_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_newtab",
            external_task_id="wait_for_telemetry_derived__unified_metrics__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="kpi_forecasting__wait_for_unified_metrics",
            external_dag_id="kpi_forecasting",
            external_task_id="wait_for_unified_metrics",
            execution_date="{{ (execution_date + macros.timedelta(seconds=3600)).isoformat() }}",
        )

        telemetry_derived__unified_metrics__v1_external.set_upstream(
            telemetry_derived__unified_metrics__v1
        )

    telemetry_derived__rolling_cohorts__v1.set_upstream(
        telemetry_derived__unified_metrics__v1
    )

    wait_for_fenix_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.fenix_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_fenix_derived__clients_last_seen_joined__v1
    )
    wait_for_firefox_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.firefox_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_firefox_ios_derived__clients_last_seen_joined__v1
    )
    wait_for_focus_android_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_android_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.focus_android_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_focus_android_derived__clients_last_seen_joined__v1
    )
    wait_for_focus_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.focus_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_focus_ios_derived__clients_last_seen_joined__v1
    )
    wait_for_klar_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.klar_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_klar_ios_derived__clients_last_seen_joined__v1
    )
    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )
    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )
