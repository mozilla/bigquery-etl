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
### bqetl_generated_funnels

Built from bigquery-etl repo, [`dags/bqetl_generated_funnels.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_generated_funnels.py)

#### Description

DAG scheduling funnels defined in sql_generators/funnels
#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 14, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_generated_funnels",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_accounts_backend_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_backend_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="accounts_backend.accounts_backend_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_accounts_frontend_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_frontend_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="accounts_frontend.accounts_frontend_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_ios_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_ios_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="firefox_ios.bigeye__firefox_ios_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=61200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_fenix_derived__firefox_android_clients__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_delta=datetime.timedelta(seconds=10800),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_org_mozilla_fenix_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_accounts_derived__fxa_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_ios_derived__clients_activation__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__clients_activation__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="firefox_ios_derived__clients_activation__v1",
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
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
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
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_frontend_derived__account_pref_delete_funnel__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__account_pref_delete_funnel__v1",
        destination_table="account_pref_delete_funnel_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__accounts_pref_engagement__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__accounts_pref_engagement__v1",
        destination_table="accounts_pref_engagement_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__email_first_reg_login_funnels_by_service__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__email_first_reg_login_funnels_by_service__v1",
        destination_table="email_first_reg_login_funnels_by_service_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__login_engagement_funnel__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__login_engagement_funnel__v1",
        destination_table="login_engagement_funnel_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__login_funnels_by_entrypoint__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__login_funnels_by_entrypoint__v1",
        destination_table="login_funnels_by_entrypoint_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__login_funnels_by_service__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__login_funnels_by_service__v1",
        destination_table="login_funnels_by_service_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__monitor_mozilla_accounts_funnels__v1 = (
        bigquery_etl_query(
            task_id="accounts_frontend_derived__monitor_mozilla_accounts_funnels__v1",
            destination_table="monitor_mozilla_accounts_funnels_v1",
            dataset_id="accounts_frontend_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ksiegler@mozilla.org",
            email=[
                "ascholtz@mozilla.com",
                "ksiegler@mozilla.org",
                "telemetry-alerts@mozilla.com",
            ],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    accounts_frontend_derived__pwd_reset_funnels_by_service__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__pwd_reset_funnels_by_service__v1",
        destination_table="pwd_reset_funnels_by_service_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__reg_engagement_funnel__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__reg_engagement_funnel__v1",
        destination_table="reg_engagement_funnel_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__registration_funnels_by_service__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__registration_funnels_by_service__v1",
        destination_table="registration_funnels_by_service_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__android_onboarding__v1 = bigquery_etl_query(
        task_id="fenix_derived__android_onboarding__v1",
        destination_table="android_onboarding_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__registration_funnels_legacy_events__v1 = (
        bigquery_etl_query(
            task_id="firefox_accounts_derived__registration_funnels_legacy_events__v1",
            destination_table="registration_funnels_legacy_events_v1",
            dataset_id="firefox_accounts_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ksiegler@mozilla.org",
            email=[
                "ascholtz@mozilla.com",
                "ksiegler@mozilla.org",
                "telemetry-alerts@mozilla.com",
            ],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    firefox_ios_derived__ios_onboarding_main__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__ios_onboarding_main__v1",
        destination_table="ios_onboarding_main_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "rzhao@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__ios_onboarding_multiple_choice__v1",
        destination_table="ios_onboarding_multiple_choice_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "rzhao@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitor_frontend_derived__monitor_dashboard_user_journey_funnels__v1 = bigquery_etl_query(
        task_id="monitor_frontend_derived__monitor_dashboard_user_journey_funnels__v1",
        destination_table="monitor_dashboard_user_journey_funnels_v1",
        dataset_id="monitor_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_frontend_derived__account_pref_delete_funnel__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__account_pref_delete_funnel__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__accounts_pref_engagement__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__email_first_reg_login_funnels_by_service__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__email_first_reg_login_funnels_by_service__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__login_engagement_funnel__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__login_funnels_by_entrypoint__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__login_funnels_by_entrypoint__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__login_funnels_by_service__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__login_funnels_by_service__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__monitor_mozilla_accounts_funnels__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__monitor_mozilla_accounts_funnels__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__pwd_reset_funnels_by_service__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__reg_engagement_funnel__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    accounts_frontend_derived__registration_funnels_by_service__v1.set_upstream(
        wait_for_accounts_backend_derived__events_stream__v1
    )

    accounts_frontend_derived__registration_funnels_by_service__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__firefox_ios_derived__attribution_clients__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_org_mozilla_fenix_derived__events_stream__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_org_mozilla_firefox_beta_derived__events_stream__v1
    )

    fenix_derived__android_onboarding__v1.set_upstream(
        wait_for_org_mozilla_firefox_derived__events_stream__v1
    )

    firefox_accounts_derived__registration_funnels_legacy_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__registration_funnels_legacy_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__registration_funnels_legacy_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__firefox_ios_derived__attribution_clients__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_firefox_ios_derived__clients_activation__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__events_stream__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__events_stream__v1
    )

    firefox_ios_derived__ios_onboarding_main__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__firefox_ios_derived__attribution_clients__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__ios_onboarding_multiple_choice__v1.set_upstream(
        wait_for_firefox_ios_derived__clients_activation__v1
    )

    monitor_frontend_derived__monitor_dashboard_user_journey_funnels__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
