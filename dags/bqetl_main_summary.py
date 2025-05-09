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
### bqetl_main_summary

Built from bigquery-etl repo, [`dags/bqetl_main_summary.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_main_summary.py)

#### Description

General-purpose derived tables for analyzing usage of desktop Firefox.
This is one of our highest-impact DAGs and should be handled carefully.

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
    "bqetl_main_summary",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_clients_first_seen_v3 = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen_v3",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="clients_first_seen_v3",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_telemetry_derived__clients_last_seen__v2 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        source_table="clients_last_seen_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 15, 0, 0),
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_telemetry_derived__clients_last_seen__v2_external",
    ) as checks__fail_telemetry_derived__clients_last_seen__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_conv_evnt_categorization__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_desktop_conv_evnt_categorization",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_engagement_model__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_desktop_engagement_model",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_funnel__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_desktop_funnel",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_feature_usage__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_feature_usage",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_newtab__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_newtab",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        checks__fail_telemetry_derived__clients_last_seen__v2_external.set_upstream(
            checks__fail_telemetry_derived__clients_last_seen__v2
        )

    checks__warn_telemetry_derived__clients_last_seen__v2 = bigquery_dq_check(
        task_id="checks__warn_telemetry_derived__clients_last_seen__v2",
        source_table="clients_last_seen_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 15, 0, 0),
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    client_probe_processes__v1 = bigquery_etl_query(
        task_id="client_probe_processes__v1",
        destination_table="client_probe_processes_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wlachance@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    crashes_daily_v1 = bigquery_etl_query(
        task_id="crashes_daily_v1",
        destination_table="crashes_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=85,
    )

    firefox_desktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_exact_mau28_by_dimensions_v2 = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_dimensions_v2",
        destination_table="firefox_desktop_exact_mau28_by_dimensions_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__clients_daily__v6 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily__v6",
        destination_table="clients_daily_v6",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2019, 11, 5, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__clients_daily__v6_external",
    ) as telemetry_derived__clients_daily__v6_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_telemetry_derived__clients_daily__v6",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_telemetry_derived__clients_daily__v6",
        )

        ExternalTaskMarker(
            task_id="bqetl_google_analytics_derived_ga4__wait_for_telemetry_derived__clients_daily__v6",
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="wait_for_telemetry_derived__clients_daily__v6",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search__wait_for_telemetry_derived__clients_daily__v6",
            external_dag_id="bqetl_search",
            external_task_id="wait_for_telemetry_derived__clients_daily__v6",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="jetstream__wait_for_clients_daily",
            external_dag_id="jetstream",
            external_task_id="wait_for_clients_daily",
            execution_date="{{ (execution_date + macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="operational_monitoring__wait_for_clients_daily",
            external_dag_id="operational_monitoring",
            external_task_id="wait_for_clients_daily",
            execution_date="{{ (execution_date + macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="parquet_export__wait_for_clients_daily",
            external_dag_id="parquet_export",
            external_task_id="wait_for_clients_daily",
            execution_date="{{ (execution_date + macros.timedelta(seconds=3600)).isoformat() }}",
        )

        telemetry_derived__clients_daily__v6_external.set_upstream(
            telemetry_derived__clients_daily__v6
        )

    telemetry_derived__clients_daily_event__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily_event__v1",
        destination_table="clients_daily_event_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=85,
    )

    telemetry_derived__clients_daily_joined__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily_joined__v1",
        destination_table="clients_daily_joined_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=85,
    )

    with TaskGroup(
        "telemetry_derived__clients_daily_joined__v1_external",
    ) as telemetry_derived__clients_daily_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_review_checker__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_review_checker",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_internet_outages__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_internet_outages",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=68400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_search",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_default_browser_aggregates__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_default_browser_aggregates",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_devtools__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_devtools",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_cert_error_privacy_dashboard__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_fx_cert_error_privacy_dashboard",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=33600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_urlbar__wait_for_telemetry_derived__clients_daily_joined__v1",
            external_dag_id="bqetl_urlbar",
            external_task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        telemetry_derived__clients_daily_joined__v1_external.set_upstream(
            telemetry_derived__clients_daily_joined__v1
        )

    telemetry_derived__clients_first_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_first_seen__v1",
        destination_table="clients_first_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 5, 5, 0, 0),
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        priority_weight=80,
    )

    with TaskGroup(
        "telemetry_derived__clients_first_seen__v1_external",
    ) as telemetry_derived__clients_first_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_desktop_ad_click_history__wait_for_telemetry_derived__clients_first_seen__v1",
            external_dag_id="bqetl_firefox_desktop_ad_click_history",
            external_task_id="wait_for_telemetry_derived__clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_conv_evnt_categorization__wait_for_telemetry_derived__clients_first_seen__v1",
            external_dag_id="bqetl_desktop_conv_evnt_categorization",
            external_task_id="wait_for_telemetry_derived__clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        telemetry_derived__clients_first_seen__v1_external.set_upstream(
            telemetry_derived__clients_first_seen__v1
        )

    telemetry_derived__clients_last_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen__v1",
        destination_table="clients_last_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2019, 4, 15, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
    )

    with TaskGroup(
        "telemetry_derived__clients_last_seen__v1_external",
    ) as telemetry_derived__clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_addons__wait_for_telemetry_derived__clients_last_seen__v1",
            external_dag_id="bqetl_addons",
            external_task_id="wait_for_telemetry_derived__clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_telemetry_derived__clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_telemetry_derived__clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        telemetry_derived__clients_last_seen__v1_external.set_upstream(
            telemetry_derived__clients_last_seen__v1
        )

    telemetry_derived__clients_last_seen__v2 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen__v2",
        destination_table="clients_last_seen_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 15, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
    )

    telemetry_derived__clients_last_seen_event__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen_event__v1",
        destination_table="clients_last_seen_event_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
    )

    telemetry_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtzs@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "ascholtzs@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
    )

    telemetry_derived__events_1pct__v1 = bigquery_etl_query(
        task_id="telemetry_derived__events_1pct__v1",
        destination_table="events_1pct_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 8, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "telemetry_derived__events_1pct__v1_external",
    ) as telemetry_derived__events_1pct__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_telemetry_derived__events_1pct__v1",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_telemetry_derived__events_1pct__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        telemetry_derived__events_1pct__v1_external.set_upstream(
            telemetry_derived__events_1pct__v1
        )

    telemetry_derived__firefox_desktop_usage__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_desktop_usage__v1",
        destination_table="firefox_desktop_usage_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_derived__main_1pct__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_1pct__v1",
        destination_table="main_1pct_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 6, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    telemetry_derived__main_nightly__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_nightly__v1",
        destination_table="main_nightly_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 7, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    telemetry_derived__main_remainder_1pct__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_remainder_1pct__v1",
        destination_table="main_remainder_1pct_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 7, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "telemetry_derived__main_remainder_1pct__v1_external",
    ) as telemetry_derived__main_remainder_1pct__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_feature_usage__wait_for_telemetry_derived__main_remainder_1pct__v1",
            external_dag_id="bqetl_feature_usage",
            external_task_id="wait_for_telemetry_derived__main_remainder_1pct__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_telemetry_derived__main_remainder_1pct__v1",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_telemetry_derived__main_remainder_1pct__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        telemetry_derived__main_remainder_1pct__v1_external.set_upstream(
            telemetry_derived__main_remainder_1pct__v1
        )

    telemetry_derived__main_use_counter_1pct__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_use_counter_1pct__v1",
        destination_table="main_use_counter_1pct_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 7, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    telemetry_derived__suggest_clients_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__suggest_clients_daily__v1",
        destination_table="suggest_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__suggest_clients_daily__v1_external",
    ) as telemetry_derived__suggest_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__wait_for_telemetry_derived__suggest_clients_daily__v1",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_telemetry_derived__suggest_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        telemetry_derived__suggest_clients_daily__v1_external.set_upstream(
            telemetry_derived__suggest_clients_daily__v1
        )

    checks__fail_telemetry_derived__clients_last_seen__v2.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    checks__fail_telemetry_derived__clients_last_seen__v2.set_upstream(
        telemetry_derived__clients_last_seen__v2
    )

    checks__warn_telemetry_derived__clients_last_seen__v2.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    checks__warn_telemetry_derived__clients_last_seen__v2.set_upstream(
        telemetry_derived__clients_last_seen__v2
    )

    crashes_daily_v1.set_upstream(wait_for_copy_deduplicate_all)

    firefox_desktop_exact_mau28_by_client_count_dimensions.set_upstream(
        checks__fail_telemetry_derived__clients_last_seen__v2
    )

    firefox_desktop_exact_mau28_by_dimensions.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    firefox_desktop_exact_mau28_by_dimensions_v2.set_upstream(
        checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__clients_daily__v6.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__clients_daily_event__v1.set_upstream(wait_for_bq_main_events)

    telemetry_derived__clients_daily_event__v1.set_upstream(wait_for_event_events)

    telemetry_derived__clients_daily_joined__v1.set_upstream(
        checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__clients_daily_joined__v1.set_upstream(crashes_daily_v1)

    telemetry_derived__clients_daily_joined__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_daily_joined__v1.set_upstream(
        telemetry_derived__clients_daily_event__v1
    )

    telemetry_derived__clients_first_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_last_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_last_seen__v1.set_upstream(
        telemetry_derived__clients_first_seen__v1
    )

    telemetry_derived__clients_last_seen__v2.set_upstream(
        wait_for_clients_first_seen_v3
    )

    telemetry_derived__clients_last_seen__v2.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_last_seen_event__v1.set_upstream(
        telemetry_derived__clients_daily_event__v1
    )

    telemetry_derived__clients_last_seen_joined__v1.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    telemetry_derived__clients_last_seen_joined__v1.set_upstream(
        telemetry_derived__clients_last_seen_event__v1
    )

    telemetry_derived__events_1pct__v1.set_upstream(wait_for_bq_main_events)

    telemetry_derived__events_1pct__v1.set_upstream(wait_for_event_events)

    telemetry_derived__firefox_desktop_usage__v1.set_upstream(
        firefox_desktop_exact_mau28_by_dimensions_v2
    )

    telemetry_derived__main_1pct__v1.set_upstream(wait_for_copy_deduplicate_main_ping)

    telemetry_derived__main_nightly__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__main_remainder_1pct__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__main_use_counter_1pct__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__suggest_clients_daily__v1.set_upstream(wait_for_bq_main_events)

    telemetry_derived__suggest_clients_daily__v1.set_upstream(wait_for_event_events)

    telemetry_derived__suggest_clients_daily__v1.set_upstream(
        telemetry_derived__clients_daily_joined__v1
    )
