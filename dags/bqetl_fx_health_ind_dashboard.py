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
### bqetl_fx_health_ind_dashboard

Built from bigquery-etl repo, [`dags/bqetl_fx_health_ind_dashboard.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fx_health_ind_dashboard.py)

#### Description

This DAG builds aggregate tables used in the Firefox Health dashboard

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 13, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_fx_health_ind_dashboard",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_telemetry_derived__main_remainder_1pct__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__main_remainder_1pct__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_remainder_1pct__v1",
        execution_delta=datetime.timedelta(seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__events_1pct__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__events_1pct__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__events_1pct__v1",
        execution_delta=datetime.timedelta(seconds=50400),
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
        execution_delta=datetime.timedelta(seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_fenix_derived__active_users_aggregates__v3 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_delta=datetime.timedelta(seconds=42300),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=42300),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=42300),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=42300),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=42300),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=42300),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_active_users_aggregates_v4 = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_active_users_aggregates_v4",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="firefox_desktop_active_users_aggregates_v4",
        execution_delta=datetime.timedelta(seconds=42300),
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
        execution_delta=datetime.timedelta(seconds=54000),
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
        execution_delta=datetime.timedelta(seconds=54000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        execution_delta=datetime.timedelta(seconds=50400),
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
        execution_delta=datetime.timedelta(seconds=54000),
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
        execution_delta=datetime.timedelta(seconds=54000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__fx_health_ind_antivirus__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_antivirus__v1",
        destination_table="fx_health_ind_antivirus_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_bookmarks_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_bookmarks_by_country__v1",
        destination_table="fx_health_ind_bookmarks_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_bookmarks_by_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_bookmarks_by_os__v1",
        destination_table="fx_health_ind_bookmarks_by_os_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_bookmarks_by_os_version__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_bookmarks_by_os_version__v1",
        destination_table="fx_health_ind_bookmarks_by_os_version_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_cert_errors__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_cert_errors__v1",
        destination_table="fx_health_ind_cert_errors_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_clients_daily_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_clients_daily_by_country__v1",
        destination_table="fx_health_ind_clients_daily_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_clients_daily_by_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_clients_daily_by_os__v1",
        destination_table="fx_health_ind_clients_daily_by_os_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_clients_daily_by_os_version__v1 = (
        bigquery_etl_query(
            task_id="telemetry_derived__fx_health_ind_clients_daily_by_os_version__v1",
            destination_table="fx_health_ind_clients_daily_by_os_version_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    telemetry_derived__fx_health_ind_desktop_dau_by_device_type__v1 = (
        bigquery_etl_query(
            task_id="telemetry_derived__fx_health_ind_desktop_dau_by_device_type__v1",
            destination_table="fx_health_ind_desktop_dau_by_device_type_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_mau_per_os__v1",
        destination_table='fx_health_ind_mau_per_os_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_mau_per_tier1_country__v1",
        destination_table='fx_health_ind_mau_per_tier1_country_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    telemetry_derived__fx_health_ind_new_profiles_by_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_new_profiles_by_os__v1",
        destination_table="fx_health_ind_new_profiles_by_os_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_np_by_install_type__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_np_by_install_type__v1",
        destination_table='fx_health_ind_np_by_install_type_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["fsd:DATE:{{macros.ds_add(ds, -7)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    telemetry_derived__fx_health_ind_page_reloads__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_page_reloads__v1",
        destination_table="fx_health_ind_page_reloads_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_ratios_smooth__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_ratios_smooth__v1",
        destination_table="fx_health_ind_ratios_smooth_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_searches_by_provider__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_searches_by_provider__v1",
        destination_table="fx_health_ind_searches_by_provider_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_vid_plybck_by_country__v1",
        destination_table="fx_health_ind_vid_plybck_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_vid_plybck_by_os__v1",
        destination_table="fx_health_ind_vid_plybck_by_os_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_os_version__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_vid_plybck_by_os_version__v1",
        destination_table="fx_health_ind_vid_plybck_by_os_version_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_webcompat__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_webcompat__v1",
        destination_table="fx_health_ind_webcompat_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_win_instll_by_instll_typ__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_win_instll_by_instll_typ__v1",
        destination_table="fx_health_ind_win_instll_by_instll_typ_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_win_uninstll__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_win_uninstll__v1",
        destination_table="fx_health_ind_win_uninstll_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1",
        destination_table='fx_health_ind_windows_versions_mau_per_os_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    telemetry_derived__install_vs_uninstall_ratio__v1 = bigquery_etl_query(
        task_id="telemetry_derived__install_vs_uninstall_ratio__v1",
        destination_table="install_vs_uninstall_ratio_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__install_vs_uninstall_ratio_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__install_vs_uninstall_ratio_by_country__v1",
        destination_table="install_vs_uninstall_ratio_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_account_signed_in_status__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_account_signed_in_status__v1",
        destination_table="uninstalls_by_account_signed_in_status_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_addon__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_addon__v1",
        destination_table="uninstalls_by_addon_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_attr_src__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_attr_src__v1",
        destination_table="uninstalls_by_attr_src_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_browser_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_browser_aggregates__v1",
        destination_table="uninstalls_by_browser_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_channel_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_channel_aggregates__v1",
        destination_table="uninstalls_by_channel_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_country_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_country_aggregates__v1",
        destination_table="uninstalls_by_country_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_day__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_day__v1",
        destination_table="uninstalls_by_day_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_default__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_default__v1",
        destination_table="uninstalls_by_default_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_default_search_engine__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_default_search_engine__v1",
        destination_table="uninstalls_by_default_search_engine_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_dflt_srch__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_dflt_srch__v1",
        destination_table="uninstalls_by_dflt_srch_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_distribution_id__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_distribution_id__v1",
        destination_table="uninstalls_by_distribution_id_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_dlsource__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_dlsource__v1",
        destination_table="uninstalls_by_dlsource_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_isp__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_isp__v1",
        destination_table="uninstalls_by_isp_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_os_install_yr__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_os_install_yr__v1",
        destination_table="uninstalls_by_os_install_yr_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_by_os_ver_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_by_os_ver_aggregates__v1",
        destination_table="uninstalls_by_os_ver_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_account_signed_in_status__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_account_signed_in_status__v1",
        destination_table="uninstalls_on_day_of_install_by_account_signed_in_status_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_addon__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_addon__v1",
        destination_table="uninstalls_on_day_of_install_by_addon_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_attr_src__v1 = (
        bigquery_etl_query(
            task_id="telemetry_derived__uninstalls_on_day_of_install_by_attr_src__v1",
            destination_table="uninstalls_on_day_of_install_by_attr_src_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    telemetry_derived__uninstalls_on_day_of_install_by_browser__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_browser__v1",
        destination_table="uninstalls_on_day_of_install_by_browser_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_country__v1",
        destination_table="uninstalls_on_day_of_install_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_cpu_core_count__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_cpu_core_count__v1",
        destination_table="uninstalls_on_day_of_install_by_cpu_core_count_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_dflt_srch__v1 = (
        bigquery_etl_query(
            task_id="telemetry_derived__uninstalls_on_day_of_install_by_dflt_srch__v1",
            destination_table="uninstalls_on_day_of_install_by_dflt_srch_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    telemetry_derived__uninstalls_on_day_of_install_by_dlsource__v1 = (
        bigquery_etl_query(
            task_id="telemetry_derived__uninstalls_on_day_of_install_by_dlsource__v1",
            destination_table="uninstalls_on_day_of_install_by_dlsource_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    telemetry_derived__uninstalls_on_day_of_install_by_os_install_yr__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_os_install_yr__v1",
        destination_table="uninstalls_on_day_of_install_by_os_install_yr_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_on_day_of_install_by_os_ver__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_on_day_of_install_by_os_ver__v1",
        destination_table="uninstalls_on_day_of_install_by_os_ver_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_per_other_installs__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_per_other_installs__v1",
        destination_table="uninstalls_per_other_installs_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_relative_to_profile_creation__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_relative_to_profile_creation__v1",
        destination_table="uninstalls_relative_to_profile_creation_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__uninstalls_to_dau_ratio_by_country__v1 = bigquery_etl_query(
        task_id="telemetry_derived__uninstalls_to_dau_ratio_by_country__v1",
        destination_table="uninstalls_to_dau_ratio_by_country_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__user_cancelled_install_share__v1 = bigquery_etl_query(
        task_id="telemetry_derived__user_cancelled_install_share__v1",
        destination_table="user_cancelled_install_share_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_health_ind_antivirus__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_bookmarks_by_country__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_bookmarks_by_os__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_bookmarks_by_os_version__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_cert_errors__v1.set_upstream(
        wait_for_telemetry_derived__events_1pct__v1
    )

    telemetry_derived__fx_health_ind_clients_daily_by_country__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__fx_health_ind_clients_daily_by_os__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__fx_health_ind_clients_daily_by_os_version__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__fx_health_ind_desktop_dau_by_device_type__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_os__v1.set_upstream(
        wait_for_firefox_desktop_active_users_aggregates_v4
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_mau_per_tier1_country__v1.set_upstream(
        wait_for_firefox_desktop_active_users_aggregates_v4
    )

    telemetry_derived__fx_health_ind_new_profiles_by_os__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__fx_health_ind_np_by_install_type__v1.set_upstream(
        wait_for_bq_main_events
    )

    telemetry_derived__fx_health_ind_np_by_install_type__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__fx_health_ind_np_by_install_type__v1.set_upstream(
        wait_for_event_events
    )

    telemetry_derived__fx_health_ind_page_reloads__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__fx_health_ind_ratios_smooth__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__fx_health_ind_searches_by_provider__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_country__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_os__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__fx_health_ind_vid_plybck_by_os_version__v1.set_upstream(
        wait_for_telemetry_derived__main_remainder_1pct__v1
    )

    telemetry_derived__fx_health_ind_webcompat__v1.set_upstream(wait_for_bq_main_events)

    telemetry_derived__fx_health_ind_webcompat__v1.set_upstream(wait_for_event_events)

    telemetry_derived__fx_health_ind_win_instll_by_instll_typ__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__fx_health_ind_win_uninstll__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__fx_health_ind_windows_versions_mau_per_os__v1.set_upstream(
        wait_for_firefox_desktop_active_users_aggregates_v4
    )

    telemetry_derived__install_vs_uninstall_ratio__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__install_vs_uninstall_ratio_by_country__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_account_signed_in_status__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_addon__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_attr_src__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_browser_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_channel_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_country_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_day__v1.set_upstream(wait_for_copy_deduplicate_all)

    telemetry_derived__uninstalls_by_default__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_default_search_engine__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_dflt_srch__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_distribution_id__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_dlsource__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_isp__v1.set_upstream(wait_for_copy_deduplicate_all)

    telemetry_derived__uninstalls_by_os_install_yr__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_by_os_ver_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_account_signed_in_status__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_addon__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_attr_src__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_browser__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_country__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_cpu_core_count__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_dflt_srch__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_dlsource__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_os_install_yr__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_on_day_of_install_by_os_ver__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_per_other_installs__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_relative_to_profile_creation__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_to_dau_ratio_by_country__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__uninstalls_to_dau_ratio_by_country__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__user_cancelled_install_share__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
