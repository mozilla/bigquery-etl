# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_analytics_tables

Built from bigquery-etl repo, [`dags/bqetl_analytics_tables.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_analytics_tables.py)

#### Description

Scheduled queries for analytics tables. engineering.
#### Owner

lvargas@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
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

    wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
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

    wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_derived__baseline_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_derived__baseline_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_fenix_derived__firefox_android_clients__v1 = bigquery_dq_check(
        task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
        source_table="firefox_android_clients_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_fenix_derived__firefox_android_clients__v1_external",
    ) as checks__fail_fenix_derived__firefox_android_clients__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=72000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_data_observability_test_data_copy__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_data_observability_test_data_copy",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=64800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
        )

        checks__fail_fenix_derived__firefox_android_clients__v1_external.set_upstream(
            checks__fail_fenix_derived__firefox_android_clients__v1
        )

    checks__fail_telemetry_derived__clients_first_seen__v2 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__clients_first_seen__v2",
        source_table="clients_first_seen_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_telemetry_derived__clients_first_seen__v2_external",
    ) as checks__fail_telemetry_derived__clients_first_seen__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_review_checker__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_review_checker",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_conv_evnt_categorization__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_desktop_conv_evnt_categorization",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mozilla_org_derived__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_mozilla_org_derived",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
        )

        ExternalTaskMarker(
            task_id="bqetl_google_analytics_derived_ga4__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_search",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_main_summary__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_main_summary",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_engagement_model__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_desktop_engagement_model",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        checks__fail_telemetry_derived__clients_first_seen__v2_external.set_upstream(
            checks__fail_telemetry_derived__clients_first_seen__v2
        )

    checks__warn_fenix_derived__firefox_android_clients__v1 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__firefox_android_clients__v1",
        source_table="firefox_android_clients_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_fenix_derived__funnel_retention_clients_week_2__v1 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__funnel_retention_clients_week_2__v1",
        source_table="funnel_retention_clients_week_2_v1",
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
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_fenix_derived__funnel_retention_clients_week_4__v1 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__funnel_retention_clients_week_4__v1",
        source_table="funnel_retention_clients_week_4_v1",
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
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_fenix_derived__funnel_retention_week_4__v1 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__funnel_retention_week_4__v1",
        source_table="funnel_retention_week_4_v1",
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
        depends_on_past=False,
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

    fenix_derived__funnel_retention_clients_week_2__v1 = bigquery_etl_query(
        task_id="fenix_derived__funnel_retention_clients_week_2__v1",
        destination_table="funnel_retention_clients_week_2_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__funnel_retention_clients_week_4__v1 = bigquery_etl_query(
        task_id="fenix_derived__funnel_retention_clients_week_4__v1",
        destination_table="funnel_retention_clients_week_4_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "fenix_derived__funnel_retention_clients_week_4__v1_external",
    ) as fenix_derived__funnel_retention_clients_week_4__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_fenix_derived__funnel_retention_clients_week_4__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_fenix_derived__funnel_retention_clients_week_4__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        fenix_derived__funnel_retention_clients_week_4__v1_external.set_upstream(
            fenix_derived__funnel_retention_clients_week_4__v1
        )

    fenix_derived__funnel_retention_week_4__v1 = bigquery_etl_query(
        task_id="fenix_derived__funnel_retention_week_4__v1",
        destination_table="funnel_retention_week_4_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "kik@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "fenix_derived__funnel_retention_week_4__v1_external",
    ) as fenix_derived__funnel_retention_week_4__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_fenix_derived__funnel_retention_week_4__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_fenix_derived__funnel_retention_week_4__v1",
        )

        fenix_derived__funnel_retention_week_4__v1_external.set_upstream(
            fenix_derived__funnel_retention_week_4__v1
        )

    firefox_android_clients = bigquery_etl_query(
        task_id="firefox_android_clients",
        destination_table="firefox_android_clients_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
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

    telemetry_derived__clients_first_seen_28_days_later__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_first_seen_28_days_later__v1",
        destination_table='clients_first_seen_28_days_later_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    checks__fail_fenix_derived__firefox_android_clients__v1.set_upstream(
        firefox_android_clients
    )

    checks__fail_telemetry_derived__clients_first_seen__v2.set_upstream(
        clients_first_seen_v2
    )

    checks__warn_fenix_derived__firefox_android_clients__v1.set_upstream(
        firefox_android_clients
    )

    checks__warn_fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        fenix_derived__funnel_retention_clients_week_2__v1
    )

    checks__warn_fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        fenix_derived__funnel_retention_clients_week_4__v1
    )

    checks__warn_fenix_derived__funnel_retention_week_4__v1.set_upstream(
        fenix_derived__funnel_retention_week_4__v1
    )

    clients_first_seen_v2.set_upstream(wait_for_copy_deduplicate_all)

    clients_first_seen_v2.set_upstream(wait_for_copy_deduplicate_first_shutdown_ping)

    clients_first_seen_v2.set_upstream(wait_for_telemetry_derived__clients_daily__v6)

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_clients_week_4__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__funnel_retention_week_4__v1.set_upstream(
        fenix_derived__funnel_retention_clients_week_4__v1
    )

    firefox_android_clients.set_upstream(wait_for_copy_deduplicate_all)

    firefox_android_clients.set_upstream(
        wait_for_fenix_derived__new_profile_activation__v1
    )

    firefox_android_clients.set_upstream(
        wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1
    )

    firefox_android_clients.set_upstream(
        wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
    )

    firefox_android_clients.set_upstream(
        wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
    )

    firefox_android_clients.set_upstream(
        wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
    )

    firefox_android_clients.set_upstream(
        wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1
    )

    telemetry_derived__clients_first_seen_28_days_later__v1.set_upstream(
        checks__fail_telemetry_derived__clients_first_seen__v2
    )

    telemetry_derived__clients_first_seen_28_days_later__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )