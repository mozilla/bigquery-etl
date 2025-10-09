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
### bqetl_braze_win10_sync

Built from bigquery-etl repo, [`dags/bqetl_braze_win10_sync.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze_win10_sync.py)

#### Description

Daily run to pull inactive Win10 users and sync them to Braze.

#### Owner

lmcfall@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "lmcfall@mozilla.com",
    "start_date": datetime.datetime(2025, 10, 6, 0, 0),
    "end_date": None,
    "email": ["lmcfall@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze_win10_sync",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_accounts_backend_derived__users_services_last_seen__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_accounts_backend_derived__users_services_last_seen__v1",
            external_dag_id="bqetl_accounts_derived",
            external_task_id="accounts_backend_derived__users_services_last_seen__v1",
            execution_delta=datetime.timedelta(seconds=12600),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_accounts_backend_external__emails__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_backend_external__emails__v1",
        external_dag_id="bqetl_accounts_backend_external",
        external_task_id="accounts_backend_external__emails__v1",
        execution_delta=datetime.timedelta(seconds=16200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_braze_derived__users__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_braze_derived__users__v1",
        external_dag_id="bqetl_braze",
        external_task_id="checks__fail_braze_derived__users__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=18000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    braze_derived__fxa_win10_users_daily__v1 = bigquery_etl_query(
        task_id="braze_derived__fxa_win10_users_daily__v1",
        destination_table="fxa_win10_users_daily_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lmcfall@mozilla.com",
        email=["lmcfall@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__fxa_win10_users_historical__v1 = bigquery_etl_query(
        task_id="braze_derived__fxa_win10_users_historical__v1",
        destination_table="fxa_win10_users_historical_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lmcfall@mozilla.com",
        email=["lmcfall@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    braze_external__win10_users_sync__v1 = bigquery_etl_query(
        task_id="braze_external__win10_users_sync__v1",
        destination_table="win10_users_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="lmcfall@mozilla.com",
        email=["lmcfall@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__fxa_win10_users_daily__v1.set_upstream(
        wait_for_accounts_backend_derived__users_services_last_seen__v1
    )

    braze_derived__fxa_win10_users_daily__v1.set_upstream(
        wait_for_accounts_backend_external__emails__v1
    )

    braze_derived__fxa_win10_users_daily__v1.set_upstream(
        wait_for_checks__fail_braze_derived__users__v1
    )

    braze_derived__fxa_win10_users_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    braze_derived__fxa_win10_users_historical__v1.set_upstream(
        braze_derived__fxa_win10_users_daily__v1
    )

    braze_external__win10_users_sync__v1.set_upstream(
        braze_derived__fxa_win10_users_historical__v1
    )
