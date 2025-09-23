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
### bqetl_cohort_daily_churn

Built from bigquery-etl repo, [`dags/bqetl_cohort_daily_churn.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_cohort_daily_churn.py)

#### Description

This DAG populates a table that measures churn of new users
Currently set to run weekly since it's an intensive query

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 1, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_cohort_daily_churn",
    default_args=default_args,
    schedule_interval="50 14 * * 7",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
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
        execution_delta=datetime.timedelta(seconds=46200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_glean_telemetry_derived__rolling_cohorts__v1 = ExternalTaskSensor(
        task_id="wait_for_glean_telemetry_derived__rolling_cohorts__v1",
        external_dag_id="bqetl_cohort_retention",
        external_task_id="glean_telemetry_derived__rolling_cohorts__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=69000),
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
        execution_delta=datetime.timedelta(seconds=46200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__rolling_cohorts__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__rolling_cohorts__v2",
        external_dag_id="bqetl_cohort_retention",
        external_task_id="checks__fail_telemetry_derived__rolling_cohorts__v2",
        execution_delta=datetime.timedelta(days=-1, seconds=69000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glean_telemetry_derived__cohort_daily_churn__v1 = bigquery_etl_query(
        task_id="glean_telemetry_derived__cohort_daily_churn__v1",
        destination_table="cohort_daily_churn_v1",
        dataset_id="glean_telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=["kwindau@mozilla.com", "mhirose@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    telemetry_derived__cohort_daily_churn__v1 = bigquery_etl_query(
        task_id="telemetry_derived__cohort_daily_churn__v1",
        destination_table="cohort_daily_churn_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    glean_telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_glean_telemetry_derived__rolling_cohorts__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__cohort_daily_churn__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__rolling_cohorts__v2
    )
