# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/copy_deduplicate_task_markers.py

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.utils.task_group import TaskGroup
import datetime

docs = """
### copy_deduplicate_task_markers

Generated DAG that propagates `copy_deduplicate` task clears to every
downstream bqetl DAG via `ExternalTaskMarker`s.

The `copy_deduplicate` DAG has a single marker per source task pointing at
the `{source_task_id}_marker` tasks in this DAG. When a source task is cleared, the clear
propagates through the markers here, propagating to every downstream
`wait_for_{source_task_id}` sensor in the corresponding bqetl DAGs.

This DAG is generated in bigquery-etl so that new bqetl DAGs appear automatically
without requiring a change to `copy_deduplicate.py`.

### Triage notes

This DAG contains only task markers, so its state should match copy_deduplicate. Tasks here
typically should not be directly cleared or marked as success/fail. Instead, the tasks in
copy_deduplicate should be cleared with "Recursive" selected.
"""

default_args = {
    "owner": "telemetry-alerts@mozilla.com",
    "start_date": datetime.datetime(2026, 4, 23),
    "email": ["telemetry-alerts@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=30),
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "copy_deduplicate_task_markers",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    bq_main_events_marker = EmptyOperator(
        task_id="bq_main_events_marker",
    )

    with TaskGroup("bq_main_events_task_markers") as bq_main_events_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_amo_stats__bq_main_events",
            external_dag_id="bqetl_amo_stats",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__bq_main_events",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=11700)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__bq_main_events",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_feature_usage__bq_main_events",
            external_dag_id="bqetl_feature_usage",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_cert_error_privacy_dashboard__bq_main_events",
            external_dag_id="bqetl_fx_cert_error_privacy_dashboard",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=56400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__bq_main_events",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=54000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_main_summary__bq_main_events",
            external_dag_id="bqetl_main_summary",
            external_task_id="wait_for_bq_main_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

    bq_main_events_marker >> bq_main_events_task_markers

    copy_deduplicate_all_marker = EmptyOperator(
        task_id="copy_deduplicate_all_marker",
    )

    with TaskGroup(
        "copy_deduplicate_all_task_markers"
    ) as copy_deduplicate_all_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_accounts_derived__copy_deduplicate_all",
            external_dag_id="bqetl_accounts_derived",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=5400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_activity_stream__copy_deduplicate_all",
            external_dag_id="bqetl_activity_stream",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_amo_stats__copy_deduplicate_all",
            external_dag_id="bqetl_amo_stats",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__copy_deduplicate_all",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=11700)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__copy_deduplicate_all",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_broken_reports_agg__copy_deduplicate_all",
            external_dag_id="bqetl_broken_reports_agg",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=18000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_client_attributes__copy_deduplicate_all",
            external_dag_id="bqetl_client_attributes",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=67200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_core__copy_deduplicate_all",
            external_dag_id="bqetl_core",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_crashes__copy_deduplicate_all",
            external_dag_id="bqetl_crashes",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__copy_deduplicate_all",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_default_browser_aggregates__copy_deduplicate_all",
            external_dag_id="bqetl_default_browser_aggregates",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_engagement_model__copy_deduplicate_all",
            external_dag_id="bqetl_desktop_engagement_model",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_funnel__copy_deduplicate_all",
            external_dag_id="bqetl_desktop_funnel",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_installs_v1__copy_deduplicate_all",
            external_dag_id="bqetl_desktop_installs_v1",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=82500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ech_adoption_rate__copy_deduplicate_all",
            external_dag_id="bqetl_ech_adoption_rate",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__copy_deduplicate_all",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__copy_deduplicate_all",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_feature_usage__copy_deduplicate_all",
            external_dag_id="bqetl_feature_usage",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_enterprise__copy_deduplicate_all",
            external_dag_id="bqetl_firefox_enterprise",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=18000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_installer_aggregates__copy_deduplicate_all",
            external_dag_id="bqetl_firefox_installer_aggregates",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__copy_deduplicate_all",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fog_decision_support__copy_deduplicate_all",
            external_dag_id="bqetl_fog_decision_support",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__copy_deduplicate_all",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=54000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__copy_deduplicate_all",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_glean_usage__copy_deduplicate_all",
            external_dag_id="bqetl_glean_usage",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_internal_tooling__copy_deduplicate_all",
            external_dag_id="bqetl_internal_tooling",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_internet_outages__copy_deduplicate_all",
            external_dag_id="bqetl_internet_outages",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=21600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ltv__copy_deduplicate_all",
            external_dag_id="bqetl_ltv",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_main_summary__copy_deduplicate_all",
            external_dag_id="bqetl_main_summary",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__copy_deduplicate_all",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=39600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_messaging_system__copy_deduplicate_all",
            external_dag_id="bqetl_messaging_system",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__copy_deduplicate_all",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__copy_deduplicate_all",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=39600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__copy_deduplicate_all",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=39600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_search__copy_deduplicate_all",
            external_dag_id="bqetl_mobile_search",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_monitoring__copy_deduplicate_all",
            external_dag_id="bqetl_monitoring",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_newtab__copy_deduplicate_all",
            external_dag_id="bqetl_newtab",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nimbus_feature_monitoring__copy_deduplicate_all",
            external_dag_id="bqetl_nimbus_feature_monitoring",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_fenix_derived__copy_deduplicate_all",
            external_dag_id="bqetl_org_mozilla_fenix_derived",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__copy_deduplicate_all",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_focus_derived__copy_deduplicate_all",
            external_dag_id="bqetl_org_mozilla_focus_derived",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_pageload_v1__copy_deduplicate_all",
            external_dag_id="bqetl_pageload_v1",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_pocket__copy_deduplicate_all",
            external_dag_id="bqetl_pocket",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=39600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_public_data_json__copy_deduplicate_all",
            external_dag_id="bqetl_public_data_json",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_review_checker__copy_deduplicate_all",
            external_dag_id="bqetl_review_checker",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_rust_component_metrics__copy_deduplicate_all",
            external_dag_id="bqetl_rust_component_metrics",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__copy_deduplicate_all",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=16200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_terms_daily__copy_deduplicate_all",
            external_dag_id="bqetl_search_terms_daily",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=25200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_serp__copy_deduplicate_all",
            external_dag_id="bqetl_serp",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=-3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_terms_of_use__copy_deduplicate_all",
            external_dag_id="bqetl_terms_of_use",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=18000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_urlbar__copy_deduplicate_all",
            external_dag_id="bqetl_urlbar",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_usage_reporting__copy_deduplicate_all",
            external_dag_id="bqetl_usage_reporting",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_use_counter_analysis__copy_deduplicate_all",
            external_dag_id="bqetl_use_counter_analysis",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=25200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_ads__copy_deduplicate_all",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_copy_deduplicate_all",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

    copy_deduplicate_all_marker >> copy_deduplicate_all_task_markers

    copy_deduplicate_first_shutdown_ping_marker = EmptyOperator(
        task_id="copy_deduplicate_first_shutdown_ping_marker",
    )

    with TaskGroup(
        "copy_deduplicate_first_shutdown_ping_task_markers"
    ) as copy_deduplicate_first_shutdown_ping_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__copy_deduplicate_first_shutdown_ping",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_copy_deduplicate_first_shutdown_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

    (
        copy_deduplicate_first_shutdown_ping_marker
        >> copy_deduplicate_first_shutdown_ping_task_markers
    )

    copy_deduplicate_main_ping_marker = EmptyOperator(
        task_id="copy_deduplicate_main_ping_marker",
    )

    with TaskGroup(
        "copy_deduplicate_main_ping_task_markers"
    ) as copy_deduplicate_main_ping_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_addons__copy_deduplicate_main_ping",
            external_dag_id="bqetl_addons",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_amo_stats__copy_deduplicate_main_ping",
            external_dag_id="bqetl_amo_stats",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_platform__copy_deduplicate_main_ping",
            external_dag_id="bqetl_desktop_platform",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_devtools__copy_deduplicate_main_ping",
            external_dag_id="bqetl_devtools",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__copy_deduplicate_main_ping",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fog_decision_support__copy_deduplicate_main_ping",
            external_dag_id="bqetl_fog_decision_support",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__copy_deduplicate_main_ping",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=54000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_internet_outages__copy_deduplicate_main_ping",
            external_dag_id="bqetl_internet_outages",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=21600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_main_summary__copy_deduplicate_main_ping",
            external_dag_id="bqetl_main_summary",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_monitoring__copy_deduplicate_main_ping",
            external_dag_id="bqetl_monitoring",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_public_data_json__copy_deduplicate_main_ping",
            external_dag_id="bqetl_public_data_json",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search__copy_deduplicate_main_ping",
            external_dag_id="bqetl_search",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ssl_ratios__copy_deduplicate_main_ping",
            external_dag_id="bqetl_ssl_ratios",
            external_task_id="wait_for_copy_deduplicate_main_ping",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

    copy_deduplicate_main_ping_marker >> copy_deduplicate_main_ping_task_markers

    event_events_marker = EmptyOperator(
        task_id="event_events_marker",
    )

    with TaskGroup("event_events_task_markers") as event_events_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_amo_stats__event_events",
            external_dag_id="bqetl_amo_stats",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__event_events",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=11700)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__event_events",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_feature_usage__event_events",
            external_dag_id="bqetl_feature_usage",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=14400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_cert_error_privacy_dashboard__event_events",
            external_dag_id="bqetl_fx_cert_error_privacy_dashboard",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=56400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__event_events",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=54000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_main_summary__event_events",
            external_dag_id="bqetl_main_summary",
            external_task_id="wait_for_event_events",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

    event_events_marker >> event_events_task_markers

    telemetry_derived__core_clients_first_seen__v1_marker = EmptyOperator(
        task_id="telemetry_derived__core_clients_first_seen__v1_marker",
    )

    with TaskGroup(
        "telemetry_derived__core_clients_first_seen__v1_task_markers"
    ) as telemetry_derived__core_clients_first_seen__v1_task_markers:

        ExternalTaskMarker(
            task_id="bqetl_core__telemetry_derived__core_clients_first_seen__v1",
            external_dag_id="bqetl_core",
            external_task_id="wait_for_telemetry_derived__core_clients_first_seen__v1",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_glean_usage__telemetry_derived__core_clients_first_seen__v1",
            external_dag_id="bqetl_glean_usage",
            external_task_id="wait_for_telemetry_derived__core_clients_first_seen__v1",
            execution_date="{{ (logical_date + datetime.timedelta(seconds=3600)).isoformat() }}",
        )

    (
        telemetry_derived__core_clients_first_seen__v1_marker
        >> telemetry_derived__core_clients_first_seen__v1_task_markers
    )
