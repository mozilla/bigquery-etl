# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_analytics_aggregations

Built from bigquery-etl repo, [`dags/bqetl_analytics_aggregations.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_analytics_aggregations.py)

#### Description

Scheduler to populate the aggregations required for analytics engineering and reports optimization. It provides data to build growth, search and usage metrics, as well as acquisition and retention KPIs, in a model that facilitates reporting in Looker.
#### Owner

lvargas@mozilla.com
"""


default_args = {
    "owner": "lvargas@mozilla.com",
    "start_date": datetime.datetime(2022, 5, 12, 0, 0),
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
    "bqetl_analytics_aggregations",
    default_args=default_args,
    schedule_interval="15 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    active_users_aggregates_attribution_v1 = bigquery_etl_query(
        task_id="active_users_aggregates_attribution_v1",
        destination_table="active_users_aggregates_attribution_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    active_users_aggregates_device_v1 = bigquery_etl_query(
        task_id="active_users_aggregates_device_v1",
        destination_table="active_users_aggregates_device_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "active_users_aggregates_device_v1_external",
    ) as active_users_aggregates_device_v1_external:
        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_active_users_aggregates_device_v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_active_users_aggregates_device_v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        active_users_aggregates_device_v1_external.set_upstream(
            active_users_aggregates_device_v1
        )

    active_users_aggregates_v1 = bigquery_etl_query(
        task_id="active_users_aggregates_v1",
        destination_table="active_users_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_active_users_aggregates = bigquery_etl_query(
        task_id="fenix_active_users_aggregates",
        destination_table="active_users_aggregates_v2",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_active_users_aggregates = bigquery_etl_query(
        task_id="firefox_desktop_active_users_aggregates",
        destination_table="active_users_aggregates_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_active_users_aggregates = bigquery_etl_query(
        task_id="firefox_ios_active_users_aggregates",
        destination_table="active_users_aggregates_v2",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    focus_android_active_users_aggregates = bigquery_etl_query(
        task_id="focus_android_active_users_aggregates",
        destination_table="active_users_aggregates_v2",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    focus_ios_active_users_aggregates = bigquery_etl_query(
        task_id="focus_ios_active_users_aggregates",
        destination_table="active_users_aggregates_v2",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    klar_ios_active_users_aggregates = bigquery_etl_query(
        task_id="klar_ios_active_users_aggregates",
        destination_table="active_users_aggregates_v2",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__cohort_daily_statistics__v1 = bigquery_etl_query(
        task_id="telemetry_derived__cohort_daily_statistics__v1",
        destination_table="cohort_daily_statistics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=[
            "anicholson@mozilla.com",
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="activity_date",
        depends_on_past=False,
    )

    telemetry_derived__desktop_cohort_daily_retention__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_cohort_daily_retention__v1",
        destination_table="desktop_cohort_daily_retention_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_checks__fail_fenix_derived__firefox_android_clients__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_delta=datetime.timedelta(seconds=8100),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    active_users_aggregates_attribution_v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )
    wait_for_telemetry_derived__unified_metrics__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__unified_metrics__v1",
        execution_delta=datetime.timedelta(seconds=4500),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    active_users_aggregates_attribution_v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )

    active_users_aggregates_device_v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )

    active_users_aggregates_v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )

    fenix_active_users_aggregates.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )
    wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_active_users_aggregates.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_fenix_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_active_users_aggregates.set_upstream(
        wait_for_fenix_derived__clients_last_seen_joined__v1
    )
    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_active_users_aggregates.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_active_users_aggregates.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )

    firefox_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )
    firefox_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_firefox_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_active_users_aggregates.set_upstream(
        wait_for_firefox_ios_derived__clients_last_seen_joined__v1
    )
    firefox_ios_active_users_aggregates.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    wait_for_focus_android_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_android_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    focus_android_active_users_aggregates.set_upstream(
        wait_for_focus_android_derived__clients_last_seen_joined__v1
    )
    focus_android_active_users_aggregates.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )
    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    focus_android_active_users_aggregates.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    focus_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )
    focus_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_focus_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    focus_ios_active_users_aggregates.set_upstream(
        wait_for_focus_ios_derived__clients_last_seen_joined__v1
    )
    focus_ios_active_users_aggregates.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    klar_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )
    klar_ios_active_users_aggregates.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_klar_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    klar_ios_active_users_aggregates.set_upstream(
        wait_for_klar_ios_derived__clients_last_seen_joined__v1
    )
    klar_ios_active_users_aggregates.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    wait_for_telemetry_derived__rolling_cohorts__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__rolling_cohorts__v1",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__rolling_cohorts__v1",
        execution_delta=datetime.timedelta(seconds=4500),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__cohort_daily_statistics__v1.set_upstream(
        wait_for_telemetry_derived__rolling_cohorts__v1
    )
    telemetry_derived__cohort_daily_statistics__v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )

    wait_for_clients_first_seen_v2 = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen_v2",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="clients_first_seen_v2",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__desktop_cohort_daily_retention__v1.set_upstream(
        wait_for_clients_first_seen_v2
    )
    telemetry_derived__desktop_cohort_daily_retention__v1.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
