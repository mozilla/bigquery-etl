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
### bqetl_mobile_kpi_metrics

Built from bigquery-etl repo, [`dags/bqetl_mobile_kpi_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_mobile_kpi_metrics.py)

#### Description

Generates support metrics for mobile KPI's
#### Owner

kik@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2024, 6, 3, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_kpi_metrics",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    task_group_fenix = TaskGroup("fenix")

    task_group_firefox_ios = TaskGroup("firefox_ios")

    task_group_focus_android = TaskGroup("focus_android")

    task_group_focus_ios = TaskGroup("focus_ios")

    task_group_klar_android = TaskGroup("klar_android")

    task_group_klar_ios = TaskGroup("klar_ios")

    wait_for_checks__fail_fenix_derived__firefox_android_clients__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_delta=datetime.timedelta(seconds=36000),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
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
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        execution_delta=datetime.timedelta(seconds=28800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_focus_beta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_focus_beta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.org_mozilla_focus_beta_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_focus_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_focus_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.org_mozilla_focus_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_focus_nightly_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_focus_nightly_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.org_mozilla_focus_nightly_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_focus_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_focus_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.org_mozilla_ios_focus_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_klar_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_klar_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.org_mozilla_klar_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_ios_klar_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_klar_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.org_mozilla_ios_klar_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__engagement__v1 = bigquery_etl_query(
        task_id="fenix_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    fenix_derived__retention__v1 = bigquery_etl_query(
        task_id="fenix_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    firefox_ios_derived__engagement__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    firefox_ios_derived__retention__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_ios,
    )

    focus_android_derived__engagement__v1 = bigquery_etl_query(
        task_id="focus_android_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    focus_android_derived__retention__v1 = bigquery_etl_query(
        task_id="focus_android_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_focus_android,
    )

    focus_ios_derived__engagement__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    focus_ios_derived__retention__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_focus_ios,
    )

    klar_android_derived__engagement__v1 = bigquery_etl_query(
        task_id="klar_android_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_android,
    )

    klar_android_derived__retention__v1 = bigquery_etl_query(
        task_id="klar_android_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_klar_android,
    )

    klar_ios_derived__engagement__v1 = bigquery_etl_query(
        task_id="klar_ios_derived__engagement__v1",
        destination_table="engagement_v1",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_ios,
    )

    klar_ios_derived__retention__v1 = bigquery_etl_query(
        task_id="klar_ios_derived__retention__v1",
        destination_table='retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mozilla/kpi_table_reviewers",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
        task_group=task_group_klar_ios,
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
    )

    fenix_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    focus_android_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_focus_beta_derived__baseline_clients_daily__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_focus_derived__baseline_clients_daily__v1
    )

    focus_android_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_focus_nightly_derived__baseline_clients_daily__v1
    )

    focus_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    focus_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    focus_ios_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_ios_focus_derived__baseline_clients_daily__v1
    )

    klar_android_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    klar_android_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    klar_android_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_klar_derived__baseline_clients_daily__v1
    )

    klar_ios_derived__engagement__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    klar_ios_derived__retention__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    klar_ios_derived__retention__v1.set_upstream(
        wait_for_org_mozilla_ios_klar_derived__baseline_clients_daily__v1
    )