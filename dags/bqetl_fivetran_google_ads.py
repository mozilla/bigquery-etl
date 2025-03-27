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
### bqetl_fivetran_google_ads

Built from bigquery-etl repo, [`dags/bqetl_fivetran_google_ads.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fivetran_google_ads.py)

#### Description

Queries for Google Ads data coming from Fivetran. Fivetran updates these tables every hour.
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2023, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_fivetran_google_ads",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_checks__fail_fenix_derived__firefox_android_clients__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_fenix_derived__funnel_retention_week_4__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__funnel_retention_week_4__v1",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="fenix_derived__funnel_retention_week_4__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__fenix_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__fenix_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="fenix.bigeye__fenix_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__fenix_derived__new_profile_activation_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__fenix_derived__new_profile_activation_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="fenix.bigeye__fenix_derived__new_profile_activation_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
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
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
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
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_android_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_android_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="focus_android.focus_android_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_ios_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_ios_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="focus_ios.focus_ios_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_klar_android_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_android_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="klar_android.klar_android_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_klar_ios_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_ios_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="klar_ios.klar_ios_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    bigeye__google_ads_derived__android_app_campaign_stats__v1 = bigquery_bigeye_check(
        task_id="bigeye__google_ads_derived__android_app_campaign_stats__v1",
        table_id="moz-fx-data-shared-prod.google_ads_derived.android_app_campaign_stats_v1",
        warehouse_id="1939",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    checks__fail_google_ads_derived__ad_groups__v1 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__ad_groups__v1",
        source_table="ad_groups_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    checks__fail_google_ads_derived__android_app_campaign_stats__v1 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__android_app_campaign_stats__v1",
        source_table='android_app_campaign_stats_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_google_ads_derived__android_app_campaign_stats__v2 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__android_app_campaign_stats__v2",
        source_table='android_app_campaign_stats_v2${{ macros.ds_format(macros.ds_add(ds, -13), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -13)}}"]
        + ["ltv_recorded_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_google_ads_derived__campaigns__v1 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__campaigns__v1",
        source_table="campaigns_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    checks__fail_google_ads_derived__campaigns__v2 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__campaigns__v2",
        source_table="campaigns_v2",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    checks__fail_google_ads_derived__daily_campaign_stats__v1 = bigquery_dq_check(
        task_id="checks__fail_google_ads_derived__daily_campaign_stats__v1",
        source_table="daily_campaign_stats_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    google_ads_derived__accounts__v1 = bigquery_etl_query(
        task_id="google_ads_derived__accounts__v1",
        destination_table="accounts_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__ad_groups__v1 = bigquery_etl_query(
        task_id="google_ads_derived__ad_groups__v1",
        destination_table="ad_groups_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__android_app_campaign_stats__v1 = bigquery_etl_query(
        task_id="google_ads_derived__android_app_campaign_stats__v1",
        destination_table='android_app_campaign_stats_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    google_ads_derived__android_app_campaign_stats__v2 = bigquery_etl_query(
        task_id="google_ads_derived__android_app_campaign_stats__v2",
        destination_table='android_app_campaign_stats_v2${{ macros.ds_format(macros.ds_add(ds, -13), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -13)}}"]
        + ["ltv_recorded_date:DATE:{{ds}}"],
    )

    google_ads_derived__campaign_conversions_by_date__v1 = bigquery_etl_query(
        task_id="google_ads_derived__campaign_conversions_by_date__v1",
        destination_table="campaign_conversions_by_date_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__campaign_names_map__v1 = bigquery_etl_query(
        task_id="google_ads_derived__campaign_names_map__v1",
        destination_table="campaign_names_map_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "google_ads_derived__campaign_names_map__v1_external",
    ) as google_ads_derived__campaign_names_map__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_campaign_cost_breakdowns__wait_for_google_ads_derived__campaign_names_map__v1",
            external_dag_id="bqetl_campaign_cost_breakdowns",
            external_task_id="wait_for_google_ads_derived__campaign_names_map__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        google_ads_derived__campaign_names_map__v1_external.set_upstream(
            google_ads_derived__campaign_names_map__v1
        )

    google_ads_derived__campaigns__v1 = bigquery_etl_query(
        task_id="google_ads_derived__campaigns__v1",
        destination_table="campaigns_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__campaigns__v2 = bigquery_etl_query(
        task_id="google_ads_derived__campaigns__v2",
        destination_table="campaigns_v2",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__daily_ad_group_stats__v1 = bigquery_etl_query(
        task_id="google_ads_derived__daily_ad_group_stats__v1",
        destination_table="daily_ad_group_stats_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    google_ads_derived__daily_campaign_stats__v1 = bigquery_etl_query(
        task_id="google_ads_derived__daily_campaign_stats__v1",
        destination_table="daily_campaign_stats_v1",
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    bigeye__google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        google_ads_derived__android_app_campaign_stats__v1
    )

    checks__fail_google_ads_derived__ad_groups__v1.set_upstream(
        google_ads_derived__ad_groups__v1
    )

    checks__fail_google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        google_ads_derived__android_app_campaign_stats__v1
    )

    checks__fail_google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        google_ads_derived__android_app_campaign_stats__v2
    )

    checks__fail_google_ads_derived__campaigns__v1.set_upstream(
        google_ads_derived__campaigns__v1
    )

    checks__fail_google_ads_derived__campaigns__v2.set_upstream(
        google_ads_derived__campaigns__v2
    )

    checks__fail_google_ads_derived__daily_campaign_stats__v1.set_upstream(
        google_ads_derived__daily_campaign_stats__v1
    )

    google_ads_derived__ad_groups__v1.set_upstream(
        checks__fail_google_ads_derived__campaigns__v1
    )

    google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        checks__fail_google_ads_derived__ad_groups__v1
    )

    google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        checks__fail_google_ads_derived__campaigns__v2
    )

    google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        wait_for_fenix_derived__funnel_retention_week_4__v1
    )

    google_ads_derived__android_app_campaign_stats__v1.set_upstream(
        google_ads_derived__daily_ad_group_stats__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__fenix_derived__attribution_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__fenix_derived__new_profile_activation_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__firefox_ios_derived__attribution_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        checks__fail_google_ads_derived__campaigns__v2
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_focus_android_derived__attribution_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_focus_ios_derived__attribution_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        google_ads_derived__daily_ad_group_stats__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_klar_android_derived__attribution_clients__v1
    )

    google_ads_derived__android_app_campaign_stats__v2.set_upstream(
        wait_for_klar_ios_derived__attribution_clients__v1
    )

    google_ads_derived__campaign_conversions_by_date__v1.set_upstream(
        google_ads_derived__campaign_names_map__v1
    )

    google_ads_derived__campaign_names_map__v1.set_upstream(
        google_ads_derived__accounts__v1
    )

    google_ads_derived__campaigns__v1.set_upstream(google_ads_derived__accounts__v1)

    google_ads_derived__campaigns__v2.set_upstream(google_ads_derived__accounts__v1)
