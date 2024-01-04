# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_gud

Built from bigquery-etl repo, [`dags/bqetl_gud.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_gud.py)

#### Description

Optimized tables that power the [Mozilla Growth and Usage Dashboard](https://gud.telemetry.mozilla.org).
#### Owner

jklukas@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_gud",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    telemetry_derived__smoot_usage_desktop__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_desktop__v2",
        destination_table="smoot_usage_desktop_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_desktop_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_desktop_compressed__v2",
        destination_table="smoot_usage_desktop_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_fxa__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_fxa__v2",
        destination_table="smoot_usage_fxa_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_fxa_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_fxa_compressed__v2",
        destination_table="smoot_usage_fxa_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_new_profiles__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles__v2",
        destination_table="smoot_usage_new_profiles_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles_compressed__v2",
        destination_table="smoot_usage_new_profiles_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_nondesktop__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_nondesktop__v2",
        destination_table="smoot_usage_nondesktop_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__smoot_usage_nondesktop_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_nondesktop_compressed__v2",
        destination_table="smoot_usage_nondesktop_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_desktop__v2.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )

    telemetry_derived__smoot_usage_desktop_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_desktop__v2
    )

    wait_for_firefox_accounts_derived__fxa_users_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_users_last_seen__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_users_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=5400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_fxa__v2.set_upstream(
        wait_for_firefox_accounts_derived__fxa_users_last_seen__v1
    )

    telemetry_derived__smoot_usage_fxa_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_fxa__v2
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        telemetry_derived__smoot_usage_desktop__v2
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        telemetry_derived__smoot_usage_fxa__v2
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        telemetry_derived__smoot_usage_nondesktop__v2
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_new_profiles__v2
    )

    wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="lockwise_android.mozilla_lockbox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="lockwise_ios.org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_android.org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="reference_browser.org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_fire_tv.org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_reality.org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1
    )
    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__smoot_usage_nondesktop__v2.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__smoot_usage_nondesktop_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_nondesktop__v2
    )
