# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_mobile_activation

Built from bigquery-etl repo, [`dags/bqetl_mobile_activation.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mobile_activation.py)

#### Description

Queries related to the mobile activation metric used by Marketing
#### Owner

vsabino@mozilla.com
"""


default_args = {
    "owner": "vsabino@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "vsabino@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_activation",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_derived__new_profile_activation__v1 = bigquery_etl_query(
        task_id="fenix_derived__new_profile_activation__v1",
        destination_table="new_profile_activation_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="vsabino@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "vsabino@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "fenix_derived__new_profile_activation__v1_external",
    ) as fenix_derived__new_profile_activation__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_fenix_derived__new_profile_activation__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_fenix_derived__new_profile_activation__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        fenix_derived__new_profile_activation__v1_external.set_upstream(
            fenix_derived__new_profile_activation__v1
        )

    firefox_ios_derived__new_profile_activation__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__new_profile_activation__v1",
        destination_table="new_profile_activation_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="vsabino@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "vsabino@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__new_profile_activation__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__new_profile_activation__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )
    wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )
    wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )
    wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__new_profile_activation__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )
