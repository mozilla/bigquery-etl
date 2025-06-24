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
### bqetl_analytics_aggregations

Built from bigquery-etl repo, [`dags/bqetl_analytics_aggregations.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_analytics_aggregations.py)

#### Description

Scheduler to populate the aggregations required for analytics engineering and reports optimization. It provides data to build growth, search and usage metrics, as well as acquisition and retention KPIs, in a model that facilitates reporting in Looker.
#### Owner

lvargas@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
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
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_analytics_aggregations",
    default_args=default_args,
    schedule_interval="15 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_checks__fail_telemetry_derived__unified_metrics__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="checks__fail_telemetry_derived__unified_metrics__v1",
        execution_delta=datetime.timedelta(seconds=4500),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__fenix_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
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
            execution_delta=datetime.timedelta(seconds=8100),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_ios_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_ios_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.bigeye__firefox_ios_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=900),
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
        execution_delta=datetime.timedelta(seconds=900),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_android_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_android_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.focus_android_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_ios_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_ios_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.focus_ios_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_klar_android_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_android_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.klar_android_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
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
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_klar_ios_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_ios_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.klar_ios_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_first_seen__v2 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_delta=datetime.timedelta(seconds=8100),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=8100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
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

    checks__fail_fenix_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_fenix_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_fenix_derived__active_users_aggregates__v3_external",
    ) as checks__fail_fenix_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_fenix_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_fenix_derived__active_users_aggregates__v3
        )

    checks__fail_firefox_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_firefox_ios_derived__active_users_aggregates__v3_external",
    ) as checks__fail_firefox_ios_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_firefox_ios_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_firefox_ios_derived__active_users_aggregates__v3
        )

    checks__fail_focus_android_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_focus_android_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_focus_android_derived__active_users_aggregates__v3_external",
    ) as checks__fail_focus_android_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_focus_android_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_focus_android_derived__active_users_aggregates__v3
        )

    checks__fail_focus_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_focus_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_focus_ios_derived__active_users_aggregates__v3_external",
    ) as checks__fail_focus_ios_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_focus_ios_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_focus_ios_derived__active_users_aggregates__v3
        )

    checks__fail_klar_android_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_klar_android_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_klar_android_derived__active_users_aggregates__v3_external",
    ) as checks__fail_klar_android_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_klar_android_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_klar_android_derived__active_users_aggregates__v3
        )

    checks__fail_klar_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__fail_klar_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_klar_ios_derived__active_users_aggregates__v3_external",
    ) as checks__fail_klar_ios_derived__active_users_aggregates__v3_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        checks__fail_klar_ios_derived__active_users_aggregates__v3_external.set_upstream(
            checks__fail_klar_ios_derived__active_users_aggregates__v3
        )

    checks__warn_fenix_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_fenix_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v4 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_derived__active_users_aggregates__v4",
        source_table='active_users_aggregates_v4${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_firefox_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_firefox_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_focus_android_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_focus_android_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_focus_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_focus_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_klar_android_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_klar_android_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    checks__warn_klar_ios_derived__active_users_aggregates__v3 = bigquery_dq_check(
        task_id="checks__warn_klar_ios_derived__active_users_aggregates__v3",
        source_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    fenix_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="fenix_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    fenix_derived__locale_aggregates__v1 = bigquery_etl_query(
        task_id="fenix_derived__locale_aggregates__v1",
        destination_table='locale_aggregates_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    firefox_desktop_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="firefox_desktop_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    firefox_desktop_active_users_aggregates_v4 = bigquery_etl_query(
        task_id="firefox_desktop_active_users_aggregates_v4",
        destination_table='active_users_aggregates_v4${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    with TaskGroup(
        "firefox_desktop_active_users_aggregates_v4_external",
    ) as firefox_desktop_active_users_aggregates_v4_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_firefox_desktop_active_users_aggregates_v4",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_firefox_desktop_active_users_aggregates_v4",
            execution_date="{{ (execution_date - macros.timedelta(seconds=900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fx_health_ind_dashboard__wait_for_firefox_desktop_active_users_aggregates_v4",
            external_dag_id="bqetl_fx_health_ind_dashboard",
            external_task_id="wait_for_firefox_desktop_active_users_aggregates_v4",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=44100)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_dynamic_dau__wait_for_firefox_desktop_active_users_aggregates_v4",
            external_dag_id="bqetl_dynamic_dau",
            external_task_id="wait_for_firefox_desktop_active_users_aggregates_v4",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=51300)).isoformat() }}",
        )

        firefox_desktop_active_users_aggregates_v4_external.set_upstream(
            firefox_desktop_active_users_aggregates_v4
        )

    firefox_desktop_derived__baseline_active_users_aggregates__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__baseline_active_users_aggregates__v2",
        destination_table="baseline_active_users_aggregates_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=[
            "ago@mozilla.com",
            "gkaberere@mozilla.com",
            "kwindau@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__locale_aggregates__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__locale_aggregates__v1",
        destination_table='locale_aggregates_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    firefox_ios_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="firefox_ios_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    focus_android_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="focus_android_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    focus_ios_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="focus_ios_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    klar_android_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="klar_android_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    klar_ios_active_users_aggregates_v3 = bigquery_etl_query(
        task_id="klar_ios_active_users_aggregates_v3",
        destination_table='active_users_aggregates_v3${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
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

    active_users_aggregates_device_v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__unified_metrics__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__fail_fenix_derived__active_users_aggregates__v3.set_upstream(
        fenix_active_users_aggregates_v3
    )

    checks__fail_firefox_ios_derived__active_users_aggregates__v3.set_upstream(
        firefox_ios_active_users_aggregates_v3
    )

    checks__fail_focus_android_derived__active_users_aggregates__v3.set_upstream(
        focus_android_active_users_aggregates_v3
    )

    checks__fail_focus_ios_derived__active_users_aggregates__v3.set_upstream(
        focus_ios_active_users_aggregates_v3
    )

    checks__fail_klar_android_derived__active_users_aggregates__v3.set_upstream(
        klar_android_active_users_aggregates_v3
    )

    checks__fail_klar_ios_derived__active_users_aggregates__v3.set_upstream(
        klar_ios_active_users_aggregates_v3
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__warn_fenix_derived__active_users_aggregates__v3.set_upstream(
        fenix_active_users_aggregates_v3
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v3.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v3.set_upstream(
        firefox_desktop_active_users_aggregates_v3
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v4.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    checks__warn_firefox_desktop_derived__active_users_aggregates__v4.set_upstream(
        firefox_desktop_active_users_aggregates_v4
    )

    checks__warn_firefox_ios_derived__active_users_aggregates__v3.set_upstream(
        firefox_ios_active_users_aggregates_v3
    )

    checks__warn_focus_android_derived__active_users_aggregates__v3.set_upstream(
        focus_android_active_users_aggregates_v3
    )

    checks__warn_focus_ios_derived__active_users_aggregates__v3.set_upstream(
        focus_ios_active_users_aggregates_v3
    )

    checks__warn_klar_android_derived__active_users_aggregates__v3.set_upstream(
        klar_android_active_users_aggregates_v3
    )

    checks__warn_klar_ios_derived__active_users_aggregates__v3.set_upstream(
        klar_ios_active_users_aggregates_v3
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__locale_aggregates__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    firefox_desktop_active_users_aggregates_v4.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    firefox_desktop_derived__baseline_active_users_aggregates__v2.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_derived__baseline_active_users_aggregates__v2.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_derived__baseline_active_users_aggregates__v2.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1
    )

    firefox_desktop_derived__locale_aggregates__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__firefox_ios_derived__metrics_clients_last_seen__v1
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_active_users_aggregates_v3.set_upstream(
        wait_for_firefox_ios_derived__clients_activation__v1
    )

    focus_android_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    focus_android_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    focus_android_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    focus_android_active_users_aggregates_v3.set_upstream(
        wait_for_focus_android_derived__metrics_clients_last_seen__v1
    )

    focus_android_active_users_aggregates_v3.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    focus_ios_active_users_aggregates_v3.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    focus_ios_active_users_aggregates_v3.set_upstream(
        wait_for_focus_ios_derived__metrics_clients_last_seen__v1
    )

    klar_android_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    klar_android_active_users_aggregates_v3.set_upstream(
        wait_for_klar_android_derived__metrics_clients_last_seen__v1
    )

    klar_ios_active_users_aggregates_v3.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    klar_ios_active_users_aggregates_v3.set_upstream(
        wait_for_klar_ios_derived__metrics_clients_last_seen__v1
    )

    telemetry_derived__desktop_cohort_daily_retention__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_first_seen__v2
    )

    telemetry_derived__desktop_cohort_daily_retention__v1.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
