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
### bqetl_accounts_derived

Built from bigquery-etl repo, [`dags/bqetl_accounts_derived.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_accounts_derived.py)

#### Description

Derived tables for analyzing data from Mozilla Accounts (`accounts_backend` and
`accounts_frontend` Glean applications).

This DAG is under active development.

#### Owner

akomar@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "akomar@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 1, 0, 0),
    "end_date": None,
    "email": ["akomar@mozilla.com", "ksiegler@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_accounts_derived",
    default_args=default_args,
    schedule_interval="30 2 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=5400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_backend_derived__monitoring_db_counts__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__monitoring_db_counts__v1",
        destination_table="monitoring_db_counts_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=["akomar@mozilla.com", "ksiegler@mozilla.com", "wclouser@mozilla.com"],
        date_partition_parameter="as_of_date",
        depends_on_past=False,
    )

    accounts_backend_derived__monitoring_db_recovery_phones_counts__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__monitoring_db_recovery_phones_counts__v1",
        destination_table="monitoring_db_recovery_phones_counts_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=["akomar@mozilla.com", "ksiegler@mozilla.com", "wclouser@mozilla.com"],
        date_partition_parameter="as_of_date",
        depends_on_past=False,
    )

    accounts_backend_derived__users_services_daily__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__users_services_daily__v1",
        destination_table="users_services_daily_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.com",
        email=["akomar@mozilla.com", "ksiegler@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_backend_derived__users_services_last_seen__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__users_services_last_seen__v1",
        destination_table="users_services_last_seen_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.com",
        email=["akomar@mozilla.com", "ksiegler@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__warn_accounts_backend_derived__users_services_daily__v1 = bigquery_dq_check(
        task_id="checks__warn_accounts_backend_derived__users_services_daily__v1",
        source_table="users_services_daily_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ksiegler@mozilla.com",
        email=["akomar@mozilla.com", "ksiegler@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    accounts_backend_derived__users_services_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_backend_derived__users_services_last_seen__v1.set_upstream(
        accounts_backend_derived__users_services_daily__v1
    )

    checks__warn_accounts_backend_derived__users_services_daily__v1.set_upstream(
        accounts_backend_derived__users_services_daily__v1
    )

    checks__warn_accounts_backend_derived__users_services_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
