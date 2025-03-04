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
### bqetl_cohort_retention

Built from bigquery-etl repo, [`dags/bqetl_cohort_retention.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_cohort_retention.py)

#### Description

Schedules daily level retention queries
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 2, 12, 0, 0),
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
    "bqetl_cohort_retention",
    default_args=default_args,
    schedule_interval="40 19 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=63600),
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
        execution_delta=datetime.timedelta(seconds=27600),
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
        execution_delta=datetime.timedelta(seconds=27600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__focus_android_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__focus_android_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="focus_android.bigeye__focus_android_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(seconds=27600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__focus_ios_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__focus_ios_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="focus_ios.bigeye__focus_ios_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(seconds=27600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__klar_android_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__klar_android_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="klar_android.bigeye__klar_android_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(seconds=27600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__klar_ios_derived__attribution_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__klar_ios_derived__attribution_clients__v1",
        external_dag_id="bqetl_mobile_kpi_metrics",
        external_task_id="klar_ios.bigeye__klar_ios_derived__attribution_clients__v1",
        execution_delta=datetime.timedelta(seconds=27600),
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
        execution_delta=datetime.timedelta(seconds=63600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_telemetry_derived__rolling_cohorts__v2 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__rolling_cohorts__v2",
        source_table="rolling_cohorts_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    telemetry_derived__cohort_daily_statistics__v2 = bigquery_etl_query(
        task_id="telemetry_derived__cohort_daily_statistics__v2",
        destination_table="cohort_daily_statistics_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__cohort_weekly_statistics__v1 = bigquery_etl_query(
        task_id="telemetry_derived__cohort_weekly_statistics__v1",
        destination_table="cohort_weekly_statistics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    telemetry_derived__rolling_cohorts__v2 = bigquery_etl_query(
        task_id="telemetry_derived__rolling_cohorts__v2",
        destination_table="rolling_cohorts_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_telemetry_derived__rolling_cohorts__v2.set_upstream(
        telemetry_derived__rolling_cohorts__v2
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__cohort_daily_statistics__v2.set_upstream(
        checks__fail_telemetry_derived__rolling_cohorts__v2
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__cohort_weekly_statistics__v1.set_upstream(
        checks__fail_telemetry_derived__rolling_cohorts__v2
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__fenix_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__firefox_ios_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__focus_android_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__focus_ios_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__klar_android_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__klar_ios_derived__attribution_clients__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_bigeye__org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__rolling_cohorts__v2.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )
