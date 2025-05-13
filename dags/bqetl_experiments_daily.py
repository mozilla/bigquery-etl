# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_experiments_daily

Built from bigquery-etl repo, [`dags/bqetl_experiments_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_experiments_daily.py)

#### Description

The DAG schedules queries that query experimentation related
metrics (enrollments, search, ...) from stable tables to finalize
numbers of experiment monitoring datasets for a specific date.

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
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
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_experiments_daily",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_fenix_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_nightly_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fennec_aurora_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_firefox_beta_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_beta_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_beta_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_firefox_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.firefox_desktop_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_fennec_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_fennec_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_fennec_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_firefox_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefox_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefox_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

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

    fenix_derived__nimbus_recorded_targeting_context__v1 = bigquery_etl_query(
        task_id="fenix_derived__nimbus_recorded_targeting_context__v1",
        destination_table="nimbus_recorded_targeting_context_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="chumphreys@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "chumphreys@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__tos_rollout_enrollments__v1 = bigquery_etl_query(
        task_id="fenix_derived__tos_rollout_enrollments__v1",
        destination_table="tos_rollout_enrollments_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dberry@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "dberry@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__tos_rollout_enrollments__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__tos_rollout_enrollments__v1",
        destination_table="tos_rollout_enrollments_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dberry@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "dberry@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__tos_rollout_enrollments__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__tos_rollout_enrollments__v1",
        destination_table="tos_rollout_enrollments_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dberry@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "dberry@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
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

    org_mozilla_fenix_derived__nimbus_recorded_targeting_context__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_fenix_derived__nimbus_recorded_targeting_context__v1",
            destination_table="nimbus_recorded_targeting_context_v1",
            dataset_id="org_mozilla_fenix_derived",
            project_id="moz-fx-data-shared-prod",
            owner="chumphreys@mozilla.com",
            email=[
                "ascholtz@mozilla.com",
                "chumphreys@mozilla.com",
                "telemetry-alerts@mozilla.com",
            ],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    org_mozilla_ios_firefox_derived__nimbus_recorded_targeting_context__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__nimbus_recorded_targeting_context__v1",
        destination_table="nimbus_recorded_targeting_context_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="chumphreys@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "chumphreys@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__experiment_crash_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__experiment_crash_aggregates__v1",
        destination_table="experiment_crash_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
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

    fenix_derived__nimbus_recorded_targeting_context__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    fenix_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_fenix_derived__events_stream__v1
    )

    fenix_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1
    )

    fenix_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1
    )

    fenix_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_firefox_beta_derived__events_stream__v1
    )

    fenix_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_firefox_derived__events_stream__v1
    )

    firefox_desktop_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_firefox_desktop_derived__events_stream__v1
    )

    firefox_ios_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__events_stream__v1
    )

    firefox_ios_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__events_stream__v1
    )

    firefox_ios_derived__tos_rollout_enrollments__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1
    )

    org_mozilla_fenix_derived__nimbus_recorded_targeting_context__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__nimbus_recorded_targeting_context__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__experiment_crash_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_bq_main_events
    )

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__experiment_enrollment_aggregates__v1.set_upstream(
        wait_for_event_events
    )

    telemetry_derived__experiment_search_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__experiment_search_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__experiments_daily_active_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__experiments_daily_active_clients__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
