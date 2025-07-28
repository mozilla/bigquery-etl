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
### bqetl_nondesktop

Built from bigquery-etl repo, [`dags/bqetl_nondesktop.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_nondesktop.py)

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_nondesktop",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="lockwise_ios.checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="lockwise_android.mozilla_lockbox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="reference_browser.org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_nondesktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_nondesktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_nondesktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_day_2_7_activation__v1",
        destination_table="firefox_nondesktop_day_2_7_activation_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkaberere@mozilla.com",
        email=["gkaberere@mozilla.com", "jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_exact_mau28__v1",
        destination_table="firefox_nondesktop_exact_mau28_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__mobile_usage__v1 = bigquery_etl_query(
        task_id="telemetry_derived__mobile_usage__v1",
        destination_table="mobile_usage_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    firefox_nondesktop_exact_mau28_by_client_count_dimensions.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__mobile_usage__v1.set_upstream(
        telemetry_derived__firefox_nondesktop_exact_mau28__v1
    )
